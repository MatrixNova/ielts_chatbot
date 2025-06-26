import config
from celery import Celery, signals
from backend import db_pool_setup

if not config.CELERY_BROKER_URL:
    raise ValueError("An error has occured. CELERY_BROKER_URL not found in config")

celery_app = Celery(
    config.APP_NAME,
    broker = config.CELERY_BROKER_URL,
    backend = config.CELERY_RESULT_BACKEND,
    include = ['backend.data_preprocessing',
               'backend.text_embedding',
               'backend.query_service',
               'backend.evaluation_service',
               'backend.chatlog_storage']
)

celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Ho_Chi_Minh', 
    enable_utc=True,

    task_routes={
        'backend.data_preprocessing.process_single_PDF_task': {'queue': 'data_preprocessing'},
        'backend.text_embedding.prepare_vectors_task': {'queue': 'embedding'}, 
        'backend.text_embedding.upsert_vectors_task': {'queue': 'embedding'},   
        'backend.query_service.process_query_task': {'queue': 'query'},
        'backend.evaluation_service.evaluate_answers_task': {'queue': 'evaluation'},
        'backend.chatlog_storage.store_batch_chat_logs_task': {'queue': 'logging'},
        'backend.chatlog_storage.flush_all_chat_logs': {'queue': 'periodic_tasks'} 
    },

    task_acks_late = True,
    worker_prefetch_multiplier = 1,
)

import logging
logger = logging.getLogger(config.APP_NAME)

@signals.worker_process_init.connect
def initialize_worker_process(**kwargs):
    logger.info("Initializing Celery worker process")

    try:
        db_pool_setup.initialize_pool()
        logger.info("Database pool initialized for worker process.")

    except Exception as e:
        logger.error(f"Error initializing database pool in worker: {e}", exc_info=True)

@signals.worker_process_shutdown.connect 
def shutdown_worker_process(**kwargs):
    logger.info("Shutting down Celery worker process")

    try:
        db_pool_setup.close_pool()
        logger.info("Database pool closed for worker process.")
    except Exception as e:

        logger.error(f"Error closing database pool in worker: {e}", exc_info=True)