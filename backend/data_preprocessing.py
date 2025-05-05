import os
import pymupdf
import psycopg2
import config
from psycopg2 import pool as psycopg_pool
from psycopg2 import extras as psycopg_extra
from celery.exceptions import MaxRetriesExceededError, Retry

import logging
logger = logging.getLogger(config.APP_NAME)

# Celery App Import 
try:
    from celery_app import celery_app
    logger.info("celery_app has been successfully imported from Celery")

except ImportError as e:
    logger.exception("An exception has occurred when importing celery_app instance."
                     f"Ensure celery.py exist in the project root. Error: {e}")
    raise

# Database Pooling Context Manager Import
try:
    from .db_pool_setup import db_connection
    logger.info("db_connection context manager has been successfully imported from db_pool_setup")

except ImportError as e:
    logger.exception("An exception has occured when importing db_connection context manager."
                     f"Ensure db_pool_setup.py exist in the project root. Error: {e}")
    
    # Ensure that the files will parse, but the tasks will fail
    class db_connection():
        def __enter__(self):
            raise ConnectionError("db_pool_setup module failed to import")
        
        def __exit__(self, t, v, tb): pass

# Status Constants
from enum import Enum

class ProcessingStatus(Enum):
    PROCESSED_CHUNK = 'processed_chunk'
    EMPTY_CONTENT = 'empty_content'
    EXTRACTION_FAILED = 'extraction_failed'

def validate_status(status):
    if isinstance(status, ProcessingStatus):
        return status.value
    
    raise ValueError(f"Invalid status: {status}. Must be in accordance to ProcessingStatus")

# Core Functions
def extract_text_from_PDF(file_path):
    filename = os.path.basename(file_path)
    base_filename, _ = os.path.splitext(filename)
    logger.debug(f"Extracting text from '{filename}'.")

    try:
        with pymupdf.open(file_path) as pdf_document:
            full_text = " ".join(page.get_text() for page in pdf_document).strip()

        lines = [line.strip() for line in full_text.splitlines() if line.strip()]

        if not lines:
            logger.warning(f"Warning. No text content extract from '{filename}'.")

            return base_filename, "", validate_status(ProcessingStatus.EMPTY_CONTENT)

        title = lines[0]
        text = " ".join(lines[1:]) if len(lines) > 1 else ""
    
        if not text.strip():
            logger.warning(f"Warning. Only title found, no main body text extracted from the file {filename}")

        return title, text, None
    
    except Exception as e:
        logger.error(f"Extraction failed for the file '{filename}': {e}")
        return None, None, validate_status(ProcessingStatus.EXTRACTION_FAILED)

def load_and_chunk_db_operations(conn, cur, original_passage_id, title, full_text):    
    logger.debug(f"Performing loading and chunking passages ID '{original_passage_id}':")

    # Chunking parameters to limit each chunk under Pinecone's threasehold
    try:
        chunk_size = 1000
        overlap = 100
        chunks_to_insert = []
        start = 0
        text_len = len(full_text)
        
        while start < text_len:
            end = start + chunk_size
            chunk_text = full_text[start:min(end, text_len)]     # Making sure the end index does not exceed text length
            
            if chunk_text and chunk_text.strip():
                chunk_num = len(chunks_to_insert) + 1
                chunk_title = f"{title} (Chunk {chunk_num})"
                chunks_to_insert.append((chunk_title, chunk_text, validate_status(ProcessingStatus.PROCESSED_CHUNK)))

            next_start = start + chunk_size - overlap

            # Avoid loop if overlap is bigger or equal to chunk size, or the text is too short
            if next_start <= start:
                break

            start = next_start

        if not chunks_to_insert:
            logger.warning("Warning. No valid chunks generated for passage ID '%s' ('%s'). Delete original entry.", original_passage_id, title[:50])
            cur.execute("""
                    DELETE FROM passages WHERE passage_id = %s
                    """, (original_passage_id,))
            
            conn.commit()
            
            return True
 
        # Delete original entries before inserting chunks
        cur.execute("""
                    DELETE FROM passages WHERE passage_id = %s
                    """, (original_passage_id,))
        
        conn.commit()

        if cur.rowcount == 0:
            logger.warning(f"Warning. Original passage ID '{original_passage_id}' not found for deletion")

        # Batch insert new chunks
        insert_query = "INSERT INTO passages (title, text, status) VALUES %s"
        psycopg_extra.execute_values(cur, insert_query, chunks_to_insert, template = None, page_size = 100)

        logger.info(f"Replace passage ID '{original_passage_id}' with {len(chunks_to_insert)} chunks in the Database")
        return True
    
    except Exception as e:
        logger.error(f"An error has occurred during Database chunking operations for passage ID '{original_passage_id}' with {e} chunks")
        raise

def mark_file_as_processed(conn, cur, filename):
    logger.debug(f"Marking the file '{filename}' as processed in the database")
    
    try:
        cur.execute("""
                    INSERT INTO processed_files (filename)
                    VALUES (%s) ON CONFLICT (filename) 
                    DO NOTHING""", (filename,)
                    ) 
        
        conn.commit()

        return True
    
    except Exception as e:
        logger.error(f"An error has occurred when marking file {filename} as processed: {e}")
        raise

# Celery Task to process single PDF file using pooled connection
@celery_app.task(bind = True, max_retries = 3, default_retry_delay = 60, acks_late = True)
def process_single_PDF_task(self, file_path):
    filename = os.path.basename(file_path)
    task_id = self.request.id
    logger.info(f"[TASK STARTS]. File: '{filename}' (Task ID: {task_id})")
    final_status_message = "Task failed prior to Database operations."

    try:
        # 1. Extract text
        title, text_content , extraction_status = extract_text_from_PDF(file_path)

        if extraction_status:
            logger.error("[TASK FAILED]. File '%s', Status: %s (Task ID: %s)", filename, extraction_status, task_id)
            return {
                "filename": filename, 
                "status": final_status_message
                }
        
        if not text_content or not text_content.strip():
            logger.warning("TASK WARN. File '%s', Status: %s. Marking processed (Task ID: %s)", filename, ProcessingStatus.EMPTY_CONTENT.name, task_id)
            final_status_message = validate_status(ProcessingStatus.EMPTY_CONTENT) + "_MARKING_ATTEMPTED."

            try:
                with db_connection() as conn:
                    with conn.cursor() as cur:
                        mark_file_as_processed(conn, cur, filename)

                    conn.commit()
                
                final_status_message = validate_status(ProcessingStatus.EMPTY_CONTENT) + "_MARKED."
                logger.info(f"[TASK SUCCESS]. Marked empty file '{filename}' as processed (Task ID: {task_id})")

            except Exception as mark_e:
                logger.critical("[TASK FAILED]. Database error marking empty file '%s': %s (Task ID: %s)", filename, mark_e, task_id, exc_info=True)
                final_status_message = validate_status(ProcessingStatus.EMPTY_CONTENT) + "_MARKING_FAILED."
                raise self.retry(exc=mark_e) from mark_e            # Retry making failures
            
            return {
                "filename": filename, 
                "status": final_status_message
                }
        
        # 2. Perform Database Operations (Load temp -> Chunk text -> Replace -> Mark file)
        logger.info(f"[TASK DATABASE START]. File '{filename}' (Task ID: {task_id})")
        with db_connection() as conn: 
            with conn.cursor() as cur:
                # a. Insert temporary record
                temp_status = 'processing_celery'
                cur.execute(
                    "INSERT INTO passages (title, text, status) VALUES (%s, %s, %s) RETURNING passage_id",
                    (title, text_content, temp_status)
                )
                temp_passage_id = cur.fetchone()[0]
                logger.info("File: %s, Inserted temp passage ID: %s (Task ID: %s)", filename, temp_passage_id, task_id)

                # b. Chunk and replace original (raises exception on error)
                load_and_chunk_db_operations(conn, cur, temp_passage_id, title, text_content)

                # c. Mark file as processed (raises exception on error)
                mark_file_as_processed(conn, cur, filename)
            
            conn.commit()
            final_status_message = validate_status(ProcessingStatus.PROCESSED_CHUNK)
            logger.info(f"[TASK DATABASE SUCCESS]. Transaction committed for file '{filename}' (Task ID: {task_id})")

        logger.info(f"[TASK SUCCESS]. Finished processing file '{filename}' (Task ID: {task_id})")
        return {
            "filename": filename, 
            "status": final_status_message
            }
    
    except (ConnectionError, psycopg2.Error, psycopg_pool.ConnectionError) as db_exec:
        logger.exception("[TASK RETRY]. Database or pooling error when processing file '%s' (Task ID: %s): %s",
                         filename, task_id, db_exec)
        raise self.retry(exc=db_exec)
    
    except Exception as exc:
        logger.exception("[TASK FAILURE]. Unhandled error when processing file '%s' (Task ID: %s): %s",
                         filename, task_id, exc)
    
        try:
            raise self.retry(exc=exc)
        
        except self.MaxRetriesExceededError:
            logger.error(f"[TASK FAILED PERMANENTLY]. Max entries exceeded for file '{filename}' (Task ID: {task_id})")
        
        except Exception as retry_exc:
            logger.exception("An exception has occurred during task retry mechanism for file '%s' (Task ID: %s): %s",
                              filename, task_id, retry_exc)
            
            return {
                "filename": filename, 
                "status": "RETRY_MECHANISM_ERROR", 
                "error": str(retry_exc)
                }
        
# Launch Processing Tasks
def get_unprocessed_files():
    unprocessed_files = []
    processed_filenames = set()
    logger.info("Checking for any unprocessed files")

    try:
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT filename FROM processed_files")
                rows = cur.fetchall()
                processed_filenames = set(row[0] for row in rows)
                
            logger.info(f"Found {len(processed_filenames)} previously processed files")

    except Exception as e:
        logger.error(f"An error has occurred. Failed to fetch processed files from the Database: {e}. Cannot determine unprocessed files")
        
        return []
    
    try:
        folder_path = config.FOLDER_PATH
        
        if not folder_path or not os.isdir(folder_path):
            logger.error(f"An error has occurred. Source PDF not found or not configured: '{folder_path}'.")
            return []
        
        all_pdf_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.pdf')]
        unprocessed_files = [
            os.path.join(folder_path, filename)
            for filename in all_pdf_files if filename not in processed_filenames
        ]

        logger.info(f"Found a total amount of {len(all_pdf_files)} PDF Files, {len(unprocessed_files)} are unprocessed")
        return unprocessed_files
    
    except Exception as e:
        logger.exception(f"Error in listing or checking files in configured PDF folder {folder_path}: {e}")
        return []

# Find unprocessed PDF files and launch Celery tasks for each
def launch_pdf_processing_tasks():
    logger.info("Launching PDF processing tasks dispatch")
    files_to_process = get_unprocessed_files()

    if not files_to_process:
        logger.info("No PDF files found to process")
        return {
            "status": "no_new_files", 
            "tasks_launched": 0
            }
    
    launched_count = 0
    for file_path_to_process in files_to_process:
        try:
            process_single_PDF_task.delay(file_path_to_process)
            logger.info("Dispatch Celery task for: %s", os.path.basename(file_path_to_process))

            launched_count += 1

        except Exception as e:
            logger.exception("Failed to dispatch Celery task for: %s", os.path.basename(file_path_to_process))

    logger.info(f"Finished dispatching {launched_count} Celery tasks")

    return {
        "status": "task_dispatched", 
        "task_launched": launched_count
        }