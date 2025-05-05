import os
import json
import gzip
import uuid
import logging
from datetime import datetime
from celery_app import celery_app
from celery.schedules import crontab
import boto3
from cachetools import TTLCache

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Config
BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
AWS_PROFILE_NAME = os.environ.get('AWS_PROFILE_NAME', 'default')
BUFFER_THRESHOLD = int(os.environ.get('LOG_BUFFER_THRESHOLD', 5))
BUFFER_TTL = int(os.environ.get('LOG_BUFFER_TTL_SECONDS', 600))  # 10 min default

# Safe buffer (Redis replacement; TTLCache auto-expires old chat_ids)
LOG_BUFFER = TTLCache(maxsize=10000, ttl=BUFFER_TTL)

def get_s3_client():
    session = boto3.Session(profile_name=AWS_PROFILE_NAME)
    return session.client('s3')

def _generate_filename(chat_id):
    iso_time = datetime.utcnow().isoformat()
    uid = uuid.uuid4().hex[:8]
    return f'chatlogs/{chat_id}/batch_{iso_time}_{uid}.json.gz'

@celery_app.task(bind=True, max_retries=3, default_retry_delay=60, acks_late=True)
def store_batch_chat_logs_task(self, chat_id):
    log_entries = LOG_BUFFER.pop(chat_id, None)
    if not log_entries:
        logger.info(f"No logs to store for chat_id: {chat_id}")
        return

    filename = _generate_filename(chat_id)
    json_payload = json.dumps(log_entries).encode('utf-8')
    compressed_payload = gzip.compress(json_payload)

    try:
        s3_client = get_s3_client()
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=filename,
            Body=compressed_payload
        )
        logger.info(f"Stored logs for {chat_id} to s3://{BUCKET_NAME}/{filename}")
    except Exception as e:
        logger.error(f"Error storing logs for {chat_id}: {e}, retrying...")
        raise self.retry(exc=e)

def buffer_chat_log(chat_id, user, message):
    timestamp = datetime.utcnow().isoformat()
    log_entry = {'timestamp': timestamp, 'user': user, 'message': message}
    
    if chat_id not in LOG_BUFFER:
        LOG_BUFFER[chat_id] = []

    LOG_BUFFER[chat_id].append(log_entry)

    if len(LOG_BUFFER[chat_id]) >= BUFFER_THRESHOLD:
        store_batch_chat_logs_task.delay(chat_id)

@celery_app.task(bind=True, max_retries=3, default_retry_delay=60, acks_late=True)
def flush_all_chat_logs(self):
    chat_ids_to_flush = list(LOG_BUFFER.keys())
    for chat_id in chat_ids_to_flush:
        if LOG_BUFFER.get(chat_id):
            store_batch_chat_logs_task.delay(chat_id)
    logger.info(f"Flushed logs for chat IDs: {chat_ids_to_flush}")

@celery_app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(
        60.0,
        flush_all_chat_logs.s(),
        name='flush_all_chat_logs_every_minute'
    )
