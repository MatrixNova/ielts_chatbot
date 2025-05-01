import psycopg2
import config
from pinecone import Pinecone, ServerlessSpec
from celery.exceptions import MaxRetriesExceededError, Retry

import logging
logger = logging.getLogger(config.APP_NAME)

# Celery App Import 
try:
    from celery_app import celery_app
    logger.info("celery_app has been successfully imported from Celery")

except ImportError as e:
    logger.exception("An exception has occurred when importing celery_app instance."
                     "Ensure celery.py exist in the project root. Error: %s", e)
    raise

# Database Pooling Context Manager Import
try:
    from .db_pool_setup import db_connection
    logger.info("db_connection context manager has been successfully imported from db_pool_setup")

except ImportError as e:
    logger.exception("An exception has occured when importing db_connection context manager."
                     "Ensure db_pool_setup.py exist in the project root. Error: %s", e)
    
    # Ensure that the files will parse, but the tasks will fail
    class db_connection():
        def __enter__(self):
            raise ConnectionError("db_pool_setup module failed to import")
        
        def __exit__(self, t, v, tb): pass

# Status Constants
from enum import Enum

class ProcessingStatus(Enum):
    PENDING = 'pending_embedding'
    EMBEDDED = 'embedded'
    FAILED = 'embedding_failed'

def validate_status(status):
    if isinstance(status, ProcessingStatus):
        return status.value
    
    raise ValueError(f"Invalid status: {status}. Must be in accordance to ProcessingStatus")

# Pinecone Client 
pinecone_index_cache = None     # For workers tasks
api_key = config.PINECONE_API_KEY
index_name = config.PINECONE_INDEX_NAME

# Core Functions
def get_pinecone_index():       ## Get Pinecone index object from cache or connect if not cached
    global pinecone_index_cache
    if pinecone_index_cache:
        logger.debug("Returning cached Pinecone index object")
        return pinecone_index_cache
    
    if not api_key:
        logger.error("An error has occurred. Pinecone API key not found")
        return None
    
    if not index_name:
        logger.error("An error has occurred. Pinecone index name not found")
        return None

    try:
        pc = Pinecone(api_key = api_key)        
        index = pc.Index(index_name)
        logger.debug(f"Attempting to connect to index {index_name}")

        # Fetching stat to ensure connection is live
        index.describe_index_stats()
        logging.info(f"Successfully connected to Pinecone index {index_name}")
        pinecone_index_cache = index

        return index
    
    except Exception as e:
        logger.exception(f"Failed to connect to existing Pinecone index '{index_name}': {e}")
        pinecone_index_cache = None

        return None
    
# Check and create Pinecone index (if needed)
def setup_pinecone_index():
    try:
        pc = Pinecone(api_key)
        logger.info(f"Setting up Pinecone index {index_name}")

        # Check if the index exist
        if index_name not in pc.list_indexes().names:
            logger.warning(f"Warning. Index '{index_name}' not found. Creating the index")

            try:
                pc.create_index(
                    name = index_name,
                    dimension = config.PINECONE_INDEX_DIMENSION,
                    metric = config.PINECONE_INDEX_METRIC,
                    spec = ServerlessSpec(cloud = config.PINECONE_INDEX_CLOUD, region = config.PINECONE_INDEX_REGION),
                    embed = {
                        "model": config.PINECONE_INDEX_MODEL,
                        "field_map": {"text": "text"}   
                    }
                )
                    
                logger.info(f"Index '{index_name}' created successfully.")
            
            except Exception as e:
                logger.exception(f"Failed to create Pinecone index '{index_name}': {e}")
                return False
        
        else:
            logger.info(f"Pinecone index '{index_name}' already exists. Setup complete.")
            return True
    
    except Exception as e:
        logger.exception(f"An error has occurred during Pinecone index setup for '{index_name}': {e}")

# Database Operations
def fetch_passages_by_ids(passage_ids):
    if not passage_ids:
        logger.warning("Process intitiated but passage_ids is an empty list")

    logger.debug(f"Fetching data for {len(passage_ids)} passage IDs")
    passages = []

    try:
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT passage_id, title, text 
                    FROM passages
                    WHERE passage_id = ANY(%s::int[])
                    """, (passage_ids,)
                    )
                passages_raw = cur.fetchall()

                # Convert to list of dictionaries
                passages = [{'passage_id': p[0], 'title': p[1], 'text': p[2]} for p in passages_raw]
                if len(passages) != len(passage_ids):
                    logger.warning("Warning. Could not find data for all requested passage IDs."
                                   f"Requested: {len(passage_ids)}. Found: {len(passages)}")
                    
    except (psycopg2.Error, ConnectionError, Exception) as e:
        logger.exception(f"An error has occurred when trying to fetch passages by IDs: {e}")

        return None

# Update passage status within a task context in PostgreSQL
def update_passages_status_in_DB(passage_ids, new_status):
    if not passage_ids:
        return None
    
    status_value = validate_status(new_status)
    logger.debug(f"Updating task status to {status_value} for {len(passage_ids)} IDs.")

    try:
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE passages
                    SET status = %s
                    WHERE passage_id = ANY(%s::int[])
                    """, (status_value, passage_ids)
                    )
                
                conn.commit()

        logger.debug(f"Task successfully updated status for {len(passage_ids)} IDs.")
        return True
    
    except (psycopg2, ConnectionError, Exception) as e:
        logger.error("Task failed to update Database status to '%s' for IDs (preview: %s): %s",
                     status_value, passage_ids[:5], e, exc_info=True)
        
        return False
    
# Vectorization preparation
def prepare_vectors_for_Pinecone(passages):
    logger.debug(f"Preparing {len(passages)} passages for Pinecone upserting")

    vectors_to_upsert = []
    skipped_ids = []

    for passage in passages:
        passage_id = passage.get('passage_id')
        title = passage.get('title')
        text = passage.get('text')

        if not passage_id or not text or not text.strip():
            logger.warning(f"Warning. Skipping passage preparation with ID {passage_id} or empty text")

            if passage_id:
                skipped_ids.append(passage_id)
                continue

        vector_id = f"passage-{passage_id}"

        metadata = {
            "passage_id": str(passage_id),
            "title": str(title),
            "text": str(text) 
        }

        vectors_to_upsert.append({
            "id": vector_id,
            "metadata": metadata
        })

    if skipped_ids:
        logger.warning(f"Skipped preparation for {len(skipped_ids)} passages with IDs: {skipped_ids}")

    logger.debug(f"Prepared {len(vectors_to_upsert)} vectors for Pinecone")

    return vectors_to_upsert, skipped_ids

# Celery Tasks
@celery_app.task(bind = True, max_retries = 3, default_retry_delay = 60, acks_late = True)
def prepare_vectors_task(self, passage_ids_batch):
    task_id = self.request.id
    logger.info(f"[PREPARE TASK START]. Task ID: {task_id}")
    
    passages = fetch_passages_by_ids(passage_ids_batch)

    if passages is None:
        logger.error(f"[PREPARE TASK FAIL]. Task ID: {task_id}. Failed to fetch passage data from DB.")
        return {
            "status": "fetch_failed", 
            "vectors": [], 
            "skipped_ids": passage_ids_batch
            }

    
    if not passages:
        logger.warning(f"[PREPARE TASK SKIP] Task ID: {task_id}. No passage data found for requested IDs.")
        return {
            "status": "no_passages", 
            "vectors": [], 
            "failed_ids": passage_ids_batch
            }
    
    vectors, skipped_ids = prepare_vectors_for_Pinecone(passages)

    # Mark passages that failed preparation as FAILED in PostgreSQL
    if skipped_ids:
         logger.warning(f"[PREPARE TASK WARN]. Task ID: {task_id}. Preparation skipped for {len(skipped_ids)} IDs. Marking FAILED")
         update_passages_status_in_DB(skipped_ids, ProcessingStatus.FAILED)
    
    logger.info(f"[PREPARE TASK OK]. Task ID: {task_id}. Prepared {len(vectors)} vectors. Skipped {len(skipped_ids)} preparation")
    return {
            "status": "prepared",
            "vectors": vectors,
            "skipped_ids": skipped_ids
    }

@celery_app.task(bind = True, max_retries = 3, acks_late = True)
def upsert_vectors_task(self, vectors_payload):
    task_id = self.request.id

    # Check the status and vectors from the previous task
    if vectors_payload.get("status") != "prepared" or not vectors_payload.get("vectors"):
        logger.warning(f"[UPSERT TASK SKIP] Task ID: {task_id}. No valid vectors received from previous task"
                       f"(Status: {vectors_payload.get('status')}).")
    
        return {"status": "skipped_no_vectors"}

    
    logger.info(f"[UPSERT TASK START]. Task ID: {task_id}")

    vectors = vectors_payload["vectors"]
    pinecone_index = get_pinecone_index()

    if not pinecone_index:
        logger.error("[TASK FAILED]. Pinecone connection error")
        
        raise self.retry(countdown = 30)

    try:
        total_upserted_count = 0
        upsert_batch_size = config.PINECONE_UPSERT_BATCH_SIZE

        for i in range(0, len(vectors), upsert_batch_size):
            batch = vectors[i : i + upsert_batch_size]
            if not batch: continue

            logger.debug(f"[UPSERT TASK SUB-BATCH]. Task ID: {task_id}. Upserting {len(batch)} vectors.")

            # Upsert the sub-batch
            pinecone_index.upsert(
                 vectors = batch,
                 namespace = config.PINECONE_NAMESPACE
            )
            
            upserted_ids = [int(v["metadata"]["passage_id"]) for v in batch]
            update_success = update_passages_status_in_DB(upserted_ids, ProcessingStatus.EMBEDDED)

        if not update_success:
            logger.critical(f"[UPSERT TASK CRITICAL]. Task ID: {task_id}. Pinecone upsert is OK, but Database update FAILED for IDs:"
                            f"{upserted_ids}. Manual intervention required")
        
        else:
            total_upserted_count += len(upserted_ids)

        logger.info(f"[UPSERT TASK SUCCESS]. Task ID: {task_id}. Successfully upserted and updated status for {total_upserted_count} vectors")

        return{
            "status": "success",
            "count": len(upserted_ids)
        }
    
    except Exception as e:
        logger.error(f"[TASK RETRY]. Upsert failed: {e}", exc_info=True)
        raise self.retry(exc=e)
    
# Fetch the IDs of passages pending embedding in batches
def fetch_all_pending_ids():
    batch_limit = config.TASK_FETCH_BATCH_SIZE

    logger.info("Fetching pending passage IDs")
    pending_ids = []
    offset = 0

    pending_status_value = validate_status(ProcessingStatus.PENDING)

    while True:
        logger.info(f"Fetching IDs with offset: {offset}, limit: {batch_limit}")

        try:
            with db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT passage_id FROM passages
                        WHERE status = %s
                        ORDER BY passage_id
                        LIMIT %s OFFSET %s
                    """, (pending_status_value, batch_limit, offset))

                    batch = [row[0] for row in cur.fetchall()]
                    if not batch:
                        break

                    pending_ids.extend(batch)
                    offset += len(batch)

        except (psycopg2.Error, ConnectionError, Exception) as e:
            logger.exception(f"An error has occurred when fetching batch of pending passage IDs: {e}")

            return None

from math import ceil
from celery import chain

# Fetch all pending passage IDs and dispatches Celery tasks in batches
def launch_embedding_tasks():
    logger.info("Launch Embedding Task Dispatch")
    all_pending_ids = fetch_all_pending_ids()

    if all_pending_ids is None:
        logger.error("Failed to fetch pending embedding passage IDs. Aborting dispatch.")
        return {
            "status": "db_fetch_failed",
            "task_launched": 0
        }
    
    if not all_pending_ids:
        logger.info("No passage found with status: %s", validate_status(ProcessingStatus.PENDING))
        return{
            "status": "no_pending_passages",
            "task_launched": 0
        }

    num_tasks_to_launch = ceil(len(all_pending_ids) / config.TASK_PROCESS_BATCH_SIZE)
    logger.info(f"Found {len(all_pending_ids)} pending passages. Proceed to launch {num_tasks_to_launch} "
                f"tasks (batch size: {config.TASK_FETCH_BATCH_SIZE})")

    launched_count = 0
    for i in range(0, len(all_pending_ids), config.TASK_PROCESS_BATCH_SIZE):
        batch_ids = all_pending_ids[i : i + config.TASK_PROCESS_BATCH_SIZE]
        if not batch_ids:
            continue

        # Automatically pass the result of vector preparation into upsert task
        try:
            task_chain = chain(prepare_vectors_task.s(batch_ids), upsert_vectors_task.s())
            task_chain.apply_async(queue = "embedding_queue")

            launched_count += 1
            logger.info(f"Dispatched task chain {launched_count} / {num_tasks_to_launch}"
                         f"for {len(batch_ids)} IDs (Preview: {batch_ids[:5]}).")
        
        except Exception as e:
            logger.exception(f"Failed to dispatch Celery task chain for batch starting with ID {batch_ids[0]}: {e}")

    logger.info(f"Finished dispatching {launched_count} embedding tasks")
    return {
        "status": "task_dispatched",
        "tasks_launched": launched_count
    }