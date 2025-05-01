import config
from celery_app import Celery, signals
from backend import db_pool_setup

if not config.CELERY:
    raise ValueError("An error has occured. CELERY_BROKER_URL not found in config")

celery_app = Celery(
    config.APP_NAME,
    broker = config.CELERY_BROKER_URL,
    backend = config.CELERY_RESULT_BACKEND,
    include = ['backend.data_preprocessing',
               'backend.text_embedding']
)

celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Ho_Chi_Minh', 
    enable_utc=True,

    task_route = {
        'backend.data_preprocessing.process_single_pdf_task': {'queue': 'data_preprocessing'}
    },

    task_acks_late = True,
    worker_prefetch_multiplier = 1,
)

import logging
logger = logging.getLogger(config.APP_NAME)

@signals.worker_process_init.connect
def initialize_worker(**kwargs):
    logger.info("Initialize Celery worker process")
    db_pool_setup.initialize_pool()

@signals.worker_process_close.connect
def close_worker(**kwargs):
    logger.info("Closing down Celery worker process")
    db_pool_setup.close_pool()
