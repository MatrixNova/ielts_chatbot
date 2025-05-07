import argparse
import logging
import os

# Assuming your modules are in a 'backend' directory relative to where main.py is
# Adjust these imports based on your actual project structure
try:
    import config # Your main configuration file
    from backend import data_preprocessing
    from backend import text_embedding
    from backend import db_pool_setup # For initializing/closing the pool if main.py interacts with DB directly
    # from backend.celery_app import celery_app # If you need to inspect tasks, etc.
except ImportError as e:
    print(f"Error importing backend modules. Make sure your PYTHONPATH is set correctly or main.py is in the correct location relative to the 'backend' directory. {e}")
    exit(1)


# Setup logging (ensure setup_logging is defined in your config.py)
try:
    logger = config.setup_logging()
except AttributeError:
    # Basic logging if setup_logging is not found
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    logger.warning("config.setup_logging() not found. Using basic logging configuration.")


def run_pdf_processing():
    """Triggers Celery tasks to process new PDF files."""
    logger.info("Attempting to launch PDF processing tasks...")
    try:
        # Initialize the DB pool if tasks might need it immediately or for checks within launch_pdf_processing_tasks
        # db_pool_setup.initialize_pool() # Often Celery workers handle their own pool initialization
        result = data_preprocessing.launch_pdf_processing_tasks()
        logger.info(f"PDF processing task launch result: {result}")
    except Exception as e:
        logger.error(f"Failed to launch PDF processing tasks: {e}", exc_info=True)
    # finally:
        # db_pool_setup.close_pool() # Close if initialized here

def run_embedding_generation():
    """Triggers Celery tasks to generate embeddings for pending passages."""
    logger.info("Attempting to launch embedding generation tasks...")
    try:
        # Initialize Pinecone index if it's a prerequisite for launching tasks or done by the main thread
        # This is usually handled by the Celery worker itself or during app startup.
        # text_embedding.setup_pinecone_index() # Ensure this is safe to call
        # db_pool_setup.initialize_pool() # If needed by launch_embedding_tasks
        result = text_embedding.launch_embedding_tasks()
        logger.info(f"Embedding generation task launch result: {result}")
    except Exception as e:
        logger.error(f"Failed to launch embedding generation tasks: {e}", exc_info=True)
    # finally:
        # db_pool_setup.close_pool() # Close if initialized here

def main():
    parser = argparse.ArgumentParser(description="IELTS Assistant Admin CLI")
    parser.add_argument(
        "action",
        choices=["process_pdfs", "generate_embeddings", "all"],
        help="The administrative action to perform."
    )

    args = parser.parse_args()

    # Initialize database pool once if multiple actions might use it.
    # However, for launching Celery tasks, often the tasks/workers manage their own connections.
    # Consider if main.py itself needs direct DB access beyond what the launch functions do.
    # If not, these can be omitted here.
    # db_pool_setup.initialize_pool()

    if args.action == "process_pdfs":
        run_pdf_processing()
    elif args.action == "generate_embeddings":
        run_embedding_generation()
    elif args.action == "all":
        logger.info("Running all administrative tasks...")
        run_pdf_processing()
        run_embedding_generation()
        logger.info("All administrative tasks initiated.")
    else:
        logger.error(f"Unknown action: {args.action}")
        parser.print_help()

    # db_pool_setup.close_pool() # Close the pool if it was initialized in main()