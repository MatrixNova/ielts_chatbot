import boto3
import json
import gzip
import uuid
import config
import redis
import io
from datetime import datetime, timezone
from celery.schedules import crontab
from botocore.exceptions import ClientError
from functools import lru_cache
import logging

logger = logging.getLogger(config.APP_NAME)

# Celery App Import 
try:
    from celery_app import celery_app
    logger.info("celery_app has been successfully imported from Celery")

except ImportError as e:
    logger.exception("Error importing celery_app. Ensure celery.py exists. Error: %s", e)
    raise

bucket_name = config.AWS_BUCKET_NAME
log_buffer_threshold = config.LOG_BUFFER_THRESHOLD
log_buffer_ttl_seconds = config.LOG_BUFFER_TTL_SECONDS

# Redis Initialization
redis_client = redis.StrictRedis(
    host = config.REDIS_HOST, 
    port = config.REDIS_PORT, 
    db = config.REDIS_DB, 
    decode_responses=False
)
redis_log = config.REDIS_LOG_LIST_KEY_PREFIX
logger.info(f"Redis client initialized for {config.REDIS_HOST}:{config.REDIS_PORT}, DB: {config.REDIS_DB}")

# Core Functions
@lru_cache(maxsize=1)
def get_s3_client():
    return boto3.client('s3')

def utcnow():
    return datetime.now(datetime.timezone.utc)

def generate_filename(chat_id):
    safe_chat_id = str(chat_id).replace("/", "_").replace(":", "_")
    iso_time = utcnow().isoformat().replace(":", "-")
    uid = uuid.uuid4().hex[:8]

    return (f"chatlogs/{safe_chat_id}/batch_{iso_time}_{uid}.json.gz")

def compress_json_payload(log_entries):
    out = io.BytesIO()

    with gzip.GzipFile(fileobj=out, mode="w") as f:
        f.write(json.dumps(log_entries).encode('utf-8'))

    return out.getvalue()

# Celerty Tasks
@celery_app.task(bind=True, max_retries=3, default_retry_delay=60, acks_late=True)
def store_batch_chat_logs_task(self, chat_id):
    redis_key = f"{redis_log}{chat_id}"
    filename = None

    try:
        log_entries_bytes = redis_client.lrange(redis_key, 0, -1)
        log_entries = []

        for i, entry_bytes in enumerate(log_entries_bytes):
            try:
                log_entries.append(json.loads(entry_bytes.decode('utf-8')))
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON at index {i} for chat_id {chat_id}. Skipping.")

        if not log_entries:
            logger.info(f"No valid log entries to store for chat_id: {chat_id}.")
            redis_client.delete(redis_key)
            return

        filename = generate_filename(chat_id)
        compressed_payload = compress_json_payload(log_entries)
        s3 = get_s3_client()

        s3.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=compressed_payload,
            ContentType='application/json',
            ContentEncoding='gzip'
        )

        logger.info("Stored logs", extra={
            "chat_id": chat_id,
            "s3_path": f"s3://{bucket_name}/{filename}",
            "log_count": len(log_entries)
        })
        redis_client.delete(redis_key)

    except ClientError as e:
        logger.error(f"S3 error storing logs for {chat_id}: {e}")
        raise self.retry(exc=e)
    
    except Exception as e:
        logger.error(f"Unexpected error storing logs for {chat_id}: {e}")
        raise self.retry(exc=e)

def buffer_chat_log(chat_id, user, message):
    timestamp = utcnow().isoformat()
    log_entry = {'timestamp': timestamp, 'user': user, 'message': message}
    key = (f"{redis_log}{chat_id}")

    try:
        entry_bytes = json.dumps(log_entry).encode('utf-8')
        current_length = redis_client.rpush(key, entry_bytes)
        redis_client.expire(key, log_buffer_ttl_seconds)

        if current_length >= log_buffer_threshold:
            logger.info(f"Threshold reached ({current_length}/{log_buffer_threshold}) for chat_id {chat_id}. Triggering task.")
            store_batch_chat_logs_task.delay(chat_id)

    except redis.exceptions.RedisError as e:
        logger.error(f"Redis error buffering log for {chat_id}: {e}")

    except Exception as e:
        logger.error(f"Unexpected error buffering log for {chat_id}: {e}")

@celery_app.task(bind=True, max_retries=3, default_retry_delay=120, acks_late=True)
def flush_all_chat_logs(self):
    flushed_chat_ids_count = 0
    logger.info("Starting periodic flush of all chat logs")

    try:
        pipeline = redis_client.pipeline()
        keys = list(redis_client.scan_iter(match=f"{redis_log}*"))

        for key in keys:
            pipeline.llen(key)
        lengths = pipeline.execute()

        for key, length in zip(keys, lengths):
            key_str = key.decode('utf-8')
            chat_id = key_str[len(redis_log):]

            if length > 0:
                logger.info(f"Periodic flush: Triggering task for chat_id {chat_id}")
                store_batch_chat_logs_task.delay(chat_id)
                flushed_chat_ids_count += 1

            else:
                redis_client.delete(key)

        logger.info(f"Periodic flush complete. Triggered tasks for {flushed_chat_ids_count} chat ID(s).")

    except redis.exceptions.RedisError as e:
        logger.error(f"Redis error during periodic flush: {e}")

    except Exception as e:
        logger.error(f"Unexpected error during periodic flush: {e}")

@celery_app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(
        crontab(minute='*/5'),
        flush_all_chat_logs.s(),
        name='flush_all_chat_logs_every_5_minutes'
    )

    logger.info("Scheduled periodic task: flush_all_chat_logs_every_5_minutes")