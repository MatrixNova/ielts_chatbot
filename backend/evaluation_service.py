import config
from openai import OpenAI

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

# Core Functions
def create_evaluation_prompt:



def initialize_llm_clients():
    if 'OpenAI' in globals() and 'OpenAI' is not None:
        try:
            openai_api_key = os.getenv("OPENAI_API_KEY")
            if not openai_api_key:
                print("An error has occured. OpenAI API key not found.")
            
            else:
                openai_client = OpenAI(api_key=openai_api_key)
                print("OpenAI client succesfully initialized.")

        except Exception as e:
            print(f"An error has occured during OpenAi initialization process: {e}")
    
        try:
            deepseek_api_key = os.getenv("DEEPSEEK_API_KEY")
            if not deepseek_api_key:
                print("An error has occured. DeepSeek API key not found.")

            else:
                deepseek_client = OpenAI(
                    base_url="https://openrouter.ai/api/v1",
                    api_key=deepseek_api_key
                )
        
        except Exception as e:
            print(f"An error has occured during DeepSeek initialization process: {e}")

    else:
        print("Skipping LLM initialization process. OpenAI library imported incorrectly")

    return openai_client, deepseek_client
