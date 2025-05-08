import boto3
import json
import gzip
import uuid
import config # Your project's config
import redis
import io
# *** Import 'time' module for timestamps ***
import time
# *** REMOVED all 'datetime' imports ***
from celery.schedules import crontab # Keep this if needed for celery beat schedules elsewhere
from botocore.exceptions import ClientError
from functools import lru_cache
import logging

logger = logging.getLogger(config.APP_NAME)

# Celery App Import
try:
    from celery_app import celery_app
    logger.info("celery_app has been successfully imported from Celery")
except ImportError as e:
    logger.exception("Error importing celery_app. Ensure celery.py exists in project root.", exc_info=True)
    raise

# AWS and Redis config reading
bucket_name = config.AWS_BUCKET_NAME
try:
    log_buffer_threshold = int(config.LOG_BUFFER_THRESHOLD)
    log_buffer_ttl_seconds = int(config.LOG_BUFFER_TTL_SECONDS)
except (TypeError, ValueError):
    logger.error("Invalid LOG_BUFFER_THRESHOLD or LOG_BUFFER_TTL_SECONDS in config. Using defaults.")
    log_buffer_threshold = 100
    log_buffer_ttl_seconds = 3600

# Redis Initialization
redis_client = None
redis_log_prefix = "chatlogs_list:" # Default prefix
try:
    redis_log_prefix = config.REDIS_LOG_LIST_KEY_PREFIX
    redis_client = redis.StrictRedis(
        host=config.REDIS_HOST,
        port=config.REDIS_PORT,
        db=config.REDIS_DB,
        decode_responses=False
    )
    redis_client.ping()
    logger.info(f"Redis client initialized and connected for {config.REDIS_HOST}:{config.REDIS_PORT}, DB: {config.REDIS_DB}")
except redis.exceptions.ConnectionError as e:
     logger.error(f"Redis connection failed: {e}. Chatlog buffering will likely fail.", exc_info=True)
except Exception as e:
    logger.error(f"Error initializing Redis client or reading prefix: {e}", exc_info=True)


# Core Functions
@lru_cache(maxsize=1)
def get_s3_client():
    try:
        return boto3.client('s3')
    except Exception as e:
        logger.error(f"Failed to create S3 client: {e}", exc_info=True)
        return None

# --- TIMESTAMP FUNCTION using only 'time' module ---
def get_utc_iso_timestamp_from_time():
     """Generates an ISO8601-like UTC timestamp string using only the time module."""
     try:
        current_unix_time = time.time()
        utc_struct_time = time.gmtime(current_unix_time) # UTC breakdown
        # Format YYYY-MM-DDTHH:MM:SS
        base_timestamp = time.strftime("%Y-%m-%dT%H:%M:%S", utc_struct_time)
        # Calculate microseconds from the fractional part of unix time
        microseconds = f"{(current_unix_time % 1):.6f}"[1:] # Gets ".xxxxxx"
        # Combine and add 'Z' for UTC indication
        iso_format_string = base_timestamp + microseconds + "Z"
        return iso_format_string
     except Exception as e:
        logger.error(f"Error generating ISO timestamp using time module: {e}", exc_info=True)
        # Fallback to simple epoch time as string
        return str(time.time()) + "_fallback_epoch"

def generate_filename(chat_id):
    """Generates a unique S3 filename using only the time module."""
    try:
        safe_chat_id = str(chat_id).replace(":", "_").replace("/", "_").replace("\\", "_").replace("..", "_")

        # Get current time info using 'time'
        current_unix_time = time.time()
        utc_struct_time = time.gmtime(current_unix_time) # Get UTC breakdown

        # Create date folder string (YYYY-MM-DD) using strftime
        date_folder = time.strftime("%Y-%m-%d", utc_struct_time)

        # Create file-safe timestamp string (YYYY-MM-DD_HH-MM-SS) using strftime
        timestamp_for_file = time.strftime("%Y-%m-%d_%H-%M-%S", utc_struct_time)

        uid = uuid.uuid4().hex[:8]

        s3_key = f"chatlogs/{date_folder}/{safe_chat_id}/batch_{timestamp_for_file}_{uid}.json.gz"
        return s3_key

    except Exception as e:
        logger.error(f"Error generating filename for chat_id {chat_id}: {e}", exc_info=True)
        try:
             fallback_ts = str(int(time.time()))
        except:
             fallback_ts = "unknown_time"
        return f"chatlogs/error/error_batch_{fallback_ts}_{uuid.uuid4().hex[:8]}.json.gz"


def compress_json_payload(log_entries):
    """Compresses a list of log entries (dicts) into gzipped JSON bytes."""
    # (No changes needed here, same as before)
    try:
        json_string = json.dumps(log_entries)
        json_bytes = json_string.encode('utf-8')
        out = io.BytesIO()
        with gzip.GzipFile(fileobj=out, mode="wb") as f:
            f.write(json_bytes)
        return out.getvalue()
    except Exception as e:
         logger.error(f"Error during JSON compression: {e}", exc_info=True)
         return None

# Celery Tasks
@celery_app.task(bind=True, max_retries=3, default_retry_delay=60, acks_late=True)
def store_batch_chat_logs_task(self, chat_id):
    """Celery task to retrieve logs from Redis and store them compressed in S3."""
    # (Code inside this task remains largely the same, using logger, checking clients)
    if not redis_client:
        logger.error(f"Redis client not available. Cannot store logs for chat_id: {chat_id}")
        return {"status": "failed_no_redis", "chat_id": chat_id}
    redis_key = f"{redis_log_prefix}{chat_id}"
    filename = None
    processed_count = 0
    try:
        log_entries_bytes = redis_client.lrange(redis_key, 0, -1)
        if not log_entries_bytes:
             logger.info(f"No log entries found in Redis for chat_id: {chat_id}. Key: {redis_key}")
             return {"status": "no_logs_found", "chat_id": chat_id}
        log_entries = []
        for i, entry_bytes in enumerate(log_entries_bytes):
            try:
                log_entries.append(json.loads(entry_bytes.decode('utf-8')))
            except Exception as decode_e:
                 logger.warning(f"Error decoding/parsing log entry at index {i} for chat_id {chat_id}, key {redis_key}: {decode_e}. Skipping entry.")
        if not log_entries:
            logger.warning(f"No valid log entries decoded for chat_id: {chat_id}. Deleting Redis key {redis_key}.")
            try: redis_client.delete(redis_key)
            except Exception as del_e: logger.error(f"Redis error deleting key {redis_key} after decode failure: {del_e}")
            return {"status": "decoding_failed", "chat_id": chat_id}

        compressed_payload = compress_json_payload(log_entries)
        if compressed_payload is None:
             logger.error(f"Compression failed for chat_id {chat_id}. Logs might be lost.")
             raise ValueError("Compression failed")

        filename = generate_filename(chat_id)
        s3 = get_s3_client()
        if not s3:
            logger.error(f"Failed to get S3 client. Cannot store logs for chat_id: {chat_id}")
            raise ConnectionError("S3 client unavailable.")

        s3.put_object(Bucket=bucket_name, Key=filename, Body=compressed_payload, ContentType='application/json', ContentEncoding='gzip')
        processed_count = len(log_entries)
        logger.info(f"Stored logs for chat_id {chat_id} to s3://{bucket_name}/{filename} ({processed_count} entries).")

        try: redis_client.delete(redis_key)
        except Exception as del_e: logger.error(f"Redis error deleting key {redis_key} after S3 upload: {del_e}.")

        return {"status": "success", "chat_id": chat_id, "s3_path": f"s3://{bucket_name}/{filename}", "count": processed_count}

    except Exception as e:
         logger.error(f"Error in store_batch_chat_logs_task for {chat_id}: {e}", exc_info=True)
         try: raise self.retry(exc=e)
         except self.MaxRetriesExceededError:
             logger.critical(f"Max retries exceeded for storing logs for chat_id {chat_id}.")
             return {"status": "failed_max_retries", "chat_id": chat_id}


def buffer_chat_log(chat_id, user, message):
    """Appends a chat log entry to the Redis buffer list using time module."""
    if not redis_client:
        logger.warning(f"Redis client not available. Cannot buffer log for chat_id: {chat_id}")
        return

    # *** Use the new timestamp function ***
    timestamp_str = get_utc_iso_timestamp_from_time()
    # *** ---------------------------- ***

    log_entry = {'timestamp': timestamp_str, 'user': user, 'message': message}
    key = f"{redis_log_prefix}{chat_id}"

    try:
        entry_bytes = json.dumps(log_entry).encode('utf-8')
        pipeline = redis_client.pipeline()
        pipeline.rpush(key, entry_bytes)
        pipeline.expire(key, log_buffer_ttl_seconds)
        results = pipeline.execute()
        current_length = results[0]

        logger.debug(f"Buffered log for chat_id {chat_id}. New buffer length: {current_length}")

        if current_length >= log_buffer_threshold:
            logger.info(f"Log buffer threshold reached ({current_length}/{log_buffer_threshold}) for chat_id {chat_id}. Triggering S3 storage task.")
            store_batch_chat_logs_task.delay(chat_id)

    except redis.exceptions.RedisError as e:
        logger.error(f"Redis error buffering log for {chat_id} (key: {key}): {e}", exc_info=True)
    except Exception as e: # Catch JSON errors etc.
        logger.error(f"Unexpected error buffering log for {chat_id} (key: {key}): {e}", exc_info=True)


# Periodic Task (still needs investigation for the Celery internal error if enabled)
@celery_app.task(bind=True, max_retries=3, default_retry_delay=120, acks_late=True)
def flush_all_chat_logs(self):
    """Periodic task to check all chat log buffers in Redis and trigger storage if needed."""
    if not redis_client:
        logger.error("Redis client not available for periodic flush.")
        return

    flushed_chat_ids_count = 0
    logger.info("Starting periodic flush of all chat logs (using time module)")
    match_pattern = f"{redis_log_prefix}*"
    try:
        keys_found = 0
        for key_bytes in redis_client.scan_iter(match=match_pattern):
            keys_found += 1
            key_str = key_bytes.decode('utf-8')
            chat_id = key_str[len(redis_log_prefix):]
            try:
                length = redis_client.llen(key_bytes)
                if length > 0:
                    logger.info(f"Periodic flush: Found non-empty buffer for chat_id {chat_id} ({length} entries). Triggering task.")
                    store_batch_chat_logs_task.delay(chat_id)
                    flushed_chat_ids_count += 1
            except Exception as e: # Catch errors checking length or delaying task for specific key
                 logger.error(f"Error processing key {key_str} during periodic flush: {e}", exc_info=True)
        logger.info(f"Periodic flush checked {keys_found} potential keys. Triggered tasks for {flushed_chat_ids_count} chat ID(s).")
    except Exception as e:
        logger.error(f"Unexpected error during periodic flush scan: {e}", exc_info=True)


# Schedule periodic tasks using Celery Beat
@celery_app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    logger.warning("Periodic task 'flush_all_chat_logs' is currently DISABLED for debugging.")
    # Keep this commented out until Celery internal ValueError is resolved
    # logger.info("Setting up periodic tasks...")
    # sender.add_periodic_task(
    #     crontab(minute='*/5'),
    #     flush_all_chat_logs.s(),
    #     name='flush_all_chat_logs_every_5_minutes'
    # )
    # logger.info("Scheduled periodic task: flush_all_chat_logs_every_5_minutes")