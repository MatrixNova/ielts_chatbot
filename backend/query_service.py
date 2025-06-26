import json
import re
import config
import time
from pinecone import Pinecone
from mistralai import Mistral
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_fixed
import logging

from backend import context_layer
from backend import prompt_templates

logger = logging.getLogger(config.APP_NAME)

# Celery App Import
try:
    from celery_app import celery_app
    logger.info("celery_app has been successfully imported from Celery")
except ImportError as e:
    logger.exception("An exception has occurred when importing celery_app instance.")
    raise

# --- Status Enums (No Change) ---
from enum import Enum
class ProcessingStatus(Enum):
    PINECONE_INIT_FAILED = 'pinecone_initialization_failed'
    LLM_INIT_FAILED = 'llm_initialization_failed'
    PASSAGE_GEN_FAILED = 'passage_generation_failed'
    QUESTION_GEN_FAILED = 'question_generation_failed'
    QUERY_SUCCESS = 'query_success'
    UNKNOWN_ERROR = 'unknown_error'

def validate_status(status):
    if isinstance(status, ProcessingStatus):
        return status.value
    raise ValueError(f"Invalid status: {status}. Must be in accordance to ProcessingStatus")

# --- Core Functions (Simplified) ---

def initialize_pinecone():
    # This function remains the same as your original
    try:
        pinecone_api_key = config.PINECONE_API_KEY
        pinecone_index_name = config.PINECONE_INDEX_NAME
        if not pinecone_api_key or not pinecone_index_name:
            logger.critical("Pinecone API Key or Index Name not found in configuration.")
            return None, None
        pc = Pinecone(api_key=pinecone_api_key)
        if pinecone_index_name not in pc.list_indexes().names():
            logger.error(f"Pinecone index '{pinecone_index_name}' does not exist.")
            return None, None
        index = pc.Index(pinecone_index_name)
        logger.info(f"Connecting to Pinecone Index object '{pinecone_index_name}'.")
        return pc, index
    except Exception as e:
        logger.critical("Pinecone initialization failed: %s", e, exc_info=True)
        return None, None

def initialize_selected_llm(model_choice):
    safe_model_choice = model_choice.strip()
    try:
        if safe_model_choice == config.MISTRAL_MODEL_CHOICE.strip():
            api_key = config.MISTRAL_API_KEY
            if not api_key: raise ValueError("Mistral API key not found")
            logger.info("Mistral client initialized.")
            return OpenAI(api_key=api_key, base_url="https://api.mistral.ai/v1")

        elif safe_model_choice == config.OPENAI_MODEL_CHOICE.strip():
            api_key = config.OPENAI_API_KEY
            if not api_key: raise ValueError("OpenAI API key not found")
            logger.info("OpenAI client initialized.")
            return OpenAI(api_key=api_key)

        else:
            raise ValueError(f"Unsupported model for passage generation: '{safe_model_choice}'")

    except Exception as e:
        logger.critical("LLM Client initialization failed: %s", e, exc_info=True)
        return None

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def call_llm_chat(client, model_name, system_prompt, user_prompt):
    try:
        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )
        return response.choices[0].message.content
    except Exception as e:
        raise Exception(f"API call to {model_name} failed: {e}")

# The old 'query_passage' function has been REMOVED from this file.

def generate_reading_passages(model_choice: str, query: str, context: str, llm_client):
    system_prompt, user_prompt = prompt_templates.get_passage_generation_prompts(context, query)

    try:
        model_name = ""

        if model_choice == config.MISTRAL_MODEL_CHOICE: model_name = config.MISTRAL_MODEL
        elif model_choice == config.OPENAI_MODEL_CHOICE: model_name = config.OPENAI_MODEL
        
        return call_llm_chat(llm_client, model_name, system_prompt, user_prompt)
    
    except Exception as e:
        logger.error("Passage generation failed: %s", e, exc_info=True)
        return None

def generate_questions(model_choice, passage, llm_client):
    system_prompt, user_prompt = prompt_templates.get_question_generation_prompts(passage)

    try:
        model_name = ""
        if model_choice == config.MISTRAL_MODEL_CHOICE: model_name = config.MISTRAL_MODEL
        elif model_choice == config.OPENAI_MODEL_CHOICE: model_name = config.OPENAI_MODEL

        raw_output = call_llm_chat(llm_client, model_name, system_prompt, user_prompt)
        cleaned_json_string = re.sub(r'```json\s*|\s*```', '', raw_output.strip(), flags=re.DOTALL)
        return json.loads(cleaned_json_string)
    
    except json.JSONDecodeError as json_e:
        logger.error("Failed to parse JSON from LLM: %s. Raw output was: %s", json_e, raw_output, exc_info=True)
        raise
    
    except Exception as e:
        logger.error("Question generation process failed: %s", e, exc_info=True)
        raise

@celery_app.task(bind = True, max_retries = 3, default_retry_delay = 60, acks_late = True)
def process_query_task(self, query, chosen_LLM):
    task_id = self.request.id
    logger.info(f"[PROCESS QUERY TASK STARTS]. Task ID: {task_id}, Query: '{query}', Chosen LLM: {chosen_LLM}")
    task_result = {}

    try:
        pc, index = initialize_pinecone()
        llm_client = initialize_selected_llm(chosen_LLM)

        if not pc or not index or not llm_client:
            task_result['status'] = validate_status(ProcessingStatus.MAIN_EXEC_FAILED)
            task_result['error_message'] = "A required service (Pinecone or LLM) failed to initialize."
            logger.critical(f"Task {task_id} failed: {task_result['error_message']}")
            return task_result

        # 1. Get context from the new dedicated builder
        passage_context = context_layer.get_context_for_query(query, pc, index)
        
        # 2. Generate the passage using the context (which may be empty)
        generated_passage = generate_reading_passages(chosen_LLM, query, passage_context, llm_client)
        if not generated_passage:
            task_result['status'] = validate_status(ProcessingStatus.PASSAGE_GEN_FAILED)
            task_result['error_message'] = "The LLM failed to generate a reading passage."
            return task_result

        # 3. Generate questions for the new passage
        generated_questions_list = generate_questions(chosen_LLM, generated_passage, llm_client)
        if not generated_questions_list:
            task_result['status'] = validate_status(ProcessingStatus.QUESTION_GEN_FAILED)
            task_result['error_message'] = "The LLM failed to generate valid questions."
            return task_result

        task_result['status'] = validate_status(ProcessingStatus.QUERY_SUCCESS)
        task_result['passage'] = generated_passage
        task_result['questions'] = generated_questions_list
        logger.info(f"[PROCESS QUERY TASK SUCCESSFUL]. Task ID: {task_id}.")
        return task_result
    
    except Exception as e:
        logger.error(f"An unhandled exception occurred in process_query_task {task_id}: {e}", exc_info=True)
        raise self.retry(exc=e, countdown=30)