import json
import re
import config
import time
from pinecone import Pinecone
from mistralai import Mistral
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_fixed

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

# Status Constants
from enum import Enum

class ProcessingStatus(Enum):
    PINECONE_INIT_FAILED = 'pinecone_initialization_failed'
    LLM_INIT_FAILED = 'llm_initialization_failed'
    PINECONE_QUERY_FAILED = 'pinecone_query_failed'
    PASSAGE_GEN_FAILED = 'passage_generation_failed'
    QUESTION_GEN_FAILED = 'question_generation_failed'
    MAIN_EXEC_FAILED = 'main_execution_failed'
    UNKNOWN_ERROR = 'unknown_error' 
    QUERY_SUCCESS = 'query_success'

def validate_status(status):
    if isinstance(status, ProcessingStatus):
        return status.value
    
    raise ValueError(f"Invalid status: {status}. Must be in accordance to ProcessingStatus")

# Core Functions:
def initialize_pinecone():
    try:
        pinecone_api_key = config.PINECONE_API_KEY
        pinecone_index_name = config.PINECONE_INDEX_NAME

        if not pinecone_api_key:
            logger.critical("An error has occurred. Pinecone API key not found")
            return None, None
        
        if not pinecone_index_name:
            logger.critical("An error has occurred. Pinecone index not found in configuration")
            return None, None

        pc = Pinecone(api_key=pinecone_api_key)
        indexes = pc.list_indexes()

        logger.debug(f"Verifying Pinecone Index. Create one if needed.")

        if not any (idx.name == pinecone_index_name for idx in indexes):
            try:
                logger.info(f"Pinecone Index {pinecone_index_name} not found. Creating one")
                pc.create_index(
                    name = pinecone_index_name,
                    cloud = config.PINECONE_INDEX_CLOUD,
                    region = config.PINECONE_INDEX_REGION,
                    embed={
                        "model": config.PINECONE_INDEX_MODEL,
                        "field_map": {"text": "text"}
                    }
                )
                logger.info(f"Index {pinecone_index_name} created in Pinecone")

            except Exception as e:
                logger.error(f"Failed to create Pinecone index: {e}")

                return None, None
        
        else:
            logger.info(f"Index {pinecone_index_name} already exists in Pinecone")

        index = pc.Index(pinecone_index_name)
        logger.info(f"Connecting to Pinecone Index object '{pinecone_index_name}'.")

        return pc, index

    except ValueError as e:
        logger.critical(f"Configuration error: {e}")
        return None, None
    
    except Exception as e:
        logger.critical("An error has occurred. %s - %s", validate_status(ProcessingStatus.PINECONE_INIT_FAILED), e, exc_info=True)
        return None, None
    
def initialize_selected_llm(model_choice):
    try:
        if model_choice == config.MISTRAL_MODEL_CHOICE:
            mistral_api_key = config.MISTRAL_API_KEY

            if not mistral_api_key:
                raise ValueError("An error has occurred. Mistral API key not found")
            
            logger.info("Mistral client has been successfully initialized")
            return Mistral(api_key = mistral_api_key)

        if model_choice == config.OPENAI_MODEL_CHOICE:
            openai_api_key = config.OPENAI_API_KEY
            
            if not openai_api_key:
                raise ValueError("An error has occurred. OpenAI API key not found")
            
            logger.info("OpenAI client has been successfully initialized")
            return OpenAI(api_key = openai_api_key)

        else:
            raise ValueError(f"Unsupported model {model_choice}. Please choose {config.MISTRAL_MODEL_CHOICE} or {config.OPENAI_MODEL_CHOICE}")
        
    except Exception as e:
        logger.critical("%s - An error has occured when initnializing LLM: %s", validate_status(ProcessingStatus.LLM_INIT_FAILED), e, exc_info=True)

        return None

@retry(stop = stop_after_attempt(3), wait = wait_fixed(2))
def call_llm_chat(client, model_name, system_prompt, user_prompt):
    try:
        response = client.chat.complete(
            model = model_name,
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )

        return response.choices[0].message.content
    
    except Exception as e:
        raise Exception(f"An error has occurred during the {model_name} API call: {e}")

def query_passage(query, pc, index, top_k: int = 3):
    if not pc or not index:
        logger.error("An error has occured. Pinecone client or index cannot be initialize globally")
        return None
    
    if not isinstance(top_k, int) or top_k <= 0:
        raise ValueError("top_k must be a positive integer")
    
    try:
        embedding_responses = pc.inference.embed(
            model = config.PINECONE_INDEX_MODEL,
            inputs = [query],
            parameters = {"input_type": "query"}
            )

        if (
            not embedding_responses 
            or not getattr(embedding_responses, 'data', None) 
            or not embedding_responses.data[0]['values']
            ):

            logger.error("%s - An error has occured. Failure in generating query embedding",
                         validate_status(ProcessingStatus.PINECONE_QUERY_FAILED))
            
            return None
        
        query_embedding = embedding_responses.data[0]['values']

        query_responses = index.query(
            vector = query_embedding,
            top_k = top_k,
            include_metadata = True,
            namespace = config.PINECONE_NAMESPACE
        )    
        
        passages = [match["metadata"]["text"] for match in query_responses["matches"]]
        return passages
    
    except AttributeError:
        logger.error("%s - An error has occured when trying to access Pinecone embedding response.", 
                     validate_status(ProcessingStatus.PINECONE_QUERY_FAILED))   

    except (AttributeError, Exception) as e:
        logger.error("%s - An error has occured during Pinecone query or embedding process: %s",
                     validate_status(ProcessingStatus.PINECONE_QUERY_FAILED), e, exc_info=True)
        
        raise

def generate_reading_passages(model_choice, query, passages, llm_client):
    context = " ".join(passages).strip()

    # Fallback if Pinecone cannot find the suitable passages
    if not context:
        context = f"A passage about: {query}"

    system_prompt = ("""You are an IELTs Reading expert. Your task is to generate an IELTS-style academic reading passage.""")

    user_prompt = (
        f"""Based on the following context, generate an IELTS-style academic reading passage.

        \"\"\"{context}\"\"\"

        Please follow this structure:

        Passage Guidelines:
        - The passage should be approximately 700–800 words long.
        - Use an academic tone, similar to passages found in the IELTS Reading section.
        - Organize the content into 4–6 paragraphs.
        - Include a title that reflects the main idea of the passage.
        - Do NOT include any questions or answers.
        - Do NOT add extra instructions, labels, or headings such as “Questions” or “Answers.”

        Do not include explanations or justifications unless explicitly asked by the user. Your output must be directly usable in an IELTS reading practice application.
        """
    )

    try:    
        model = config.MISTRAL_MODEL if model_choice == config.MISTRAL_MODEL_CHOICE else config.OPENAI_MODEL
        return call_llm_chat(llm_client, model, system_prompt, user_prompt)
    
    except Exception as e:
        logger.error("%s - Error occured during API call for %s. Passage generation failed.",
                     validate_status(ProcessingStatus.PASSAGE_GEN_FAILED), model_choice)
        
        return None

def generate_questions(model_choice, passage, llm_client):
    system_prompt = ("""You are an IELTS Reading expert that generates IELTs-style reading questions based on a provided passage.""")

    user_prompt = (
        f""" Your task is to output ONLY a valid JSON array containing exactly 10 object questions based on the passage below:

        \"\"\"{passage}\"\"\"

        Please follow this structure:

        Question Types:
        Alternate between the following IELTS question types:
        1. Multiple choice (4 options: A, B, C, D)
        2. Identifying information (True / False / Not Given)
        3. Identifying writer’s views/claims (Yes / No / Not Given)
        4. Matching headings / information / features / sentence endings
        5. Sentence completion
        6. Short-answer questions

        Instructions:
        - Generate a well-balanced mix of these types.
        - Ensure questions reflect the style and difficulty of the IELTS Reading section.
        - Each question should be clear and concise.
        - For multiple choice, label the options clearly as A, B, C, D.
        - Do NOT provide answers at the end.

        Output Formatting Requirement (MANDATORY):
        - Your **ENTIRE** responses MUST be a single, valid JSON array.
        - The response MUST start with '[' and end with ']'.
        - Each element in the array MUST be a JSON object with the following EXACT keys:
            1. "number": (integer) The question number (01-10).
            2. "type": (string) The specific question types (e.g. "Multiple Choices", "True/False/Not Given, etc.")
            3. "text": (string) The full question texts (including options A, B, C, D for multiple choice questions).
        - **CRITICAL**: DO NOT include ANY text, explanation, introduction, section headers (such as "Section 1: Multiple Choices"),
        or ANY other content BEFORE the opening '['or AFTER the closing ']'.
        - The output MUST be machine-readable JSON only.

        Example of a single object within the required JSON array format:
            "number": 1,
            "type": "Multiple choice",
            "text": "What is the primary topic discussed?\\nA) Option A text\\nB) Option B text\\nC) Option C text\\nD) Option D text"

        Generate the JSON array based on the provided passage.
            
        """
    )

    try:
        model = config.MISTRAL_MODEL if model_choice == config.MISTRAL_MODEL_CHOICE else config.OPENAI_MODEL

        raw_output = call_llm_chat(llm_client, model, system_prompt, user_prompt)
        cleaned = json.loads(raw_output)
        logger.debug(f"Raw LLM JSON response:\n{cleaned}")

        return cleaned
    
    except json.JSONDecodeError as json_e:
        logger.error("%s - Invalid JSON from LLM: %s", validate_status(ProcessingStatus.QUESTION_GEN_FAILED), json_e, exc_info=True)
        raise

    except Exception as e:
        logger.error("%s - An error has occurred during questions generation process: %s", 
                     validate_status(ProcessingStatus.QUESTION_GEN_FAILED), e, exc_info=True)

        raise
    
@celery_app.task(bind = True, max_retries = 3, default_retry_delay = 60, acks_late = True)
# Celery task to process query AND passage AND questions generation
def process_query_task(self, query, chosen_LLM):
    task_id = self.request.id
    logger.info(f"[PROCESS QUERY TASK STARTS]. Task ID: {task_id}, Query: {query}, Chosen LLM: {chosen_LLM}")
    task_result = {}

    try:
        pc, index = initialize_pinecone()
        llm_client = initialize_selected_llm(chosen_LLM)

        if not pc or not index:
            task_result['status'] = validate_status(ProcessingStatus.PINECONE_INIT_FAILED)
            task_result['error_message'] = "Pinecone client or index failed to initialized"
            logger.critical(f"[PROCESS QUERY TASK FAILED]. Task ID: {task_id}, Task result: {task_result['error_message']}")

            return task_result
        
        if not llm_client:
            task_result['status']  = validate_status(ProcessingStatus.LLM_INIT_FAILED)
            task_result['error_message'] = "LLM model failed to initialized"
            logger.critical(f"[PROCESS QUERY TASK FAILED]. Task ID: {task_id}, Task result: {task_result['error_message']}")

            return task_result

        start = time.time()
        retrieved_passage = query_passage(query, pc, index)
        logger.info("Query took %.2f seconds", time.time() - start)

        if not retrieved_passage:
            task_result['status'] = validate_status(ProcessingStatus.PINECONE_QUERY_FAILED)
            task_result['error_message'] = "Failed to retrieve passages from Pinecone"
            logger.error(f"[PROCESS QUERY TASK FAILED]. Task ID: {task_id}, Task result: {task_result['error_message']}")

            return task_result
            
        generated_passage = generate_reading_passages(chosen_LLM, query, retrieved_passage)

        if not generated_passage:
            task_result['status'] = validate_status(ProcessingStatus.PASSAGE_GEN_FAILED)
            task_result['error_message'] = "Failed to generate reading passage"
            logger.error(f"[PRORCESS QUERY TASK FAILED]. Task ID: {task_id}, Task result: {task_result['error_message']}")

            return task_result
            
        generate_questions_list = generate_questions(chosen_LLM, generated_passage)

        if not generate_questions_list:
            task_result['status'] = validate_status(ProcessingStatus.QUESTION_GEN_FAILED)
            task_result['error_message'] = "Failed to generate questions"
            logger.error(f"[PROCESS QUERY TASK FAILED]. Task ID: {task_id}, Task result: {task_result['error_message']}")

            return task_result
            
        task_result['status'] = validate_status(ProcessingStatus.QUERY_SUCCESS)
        task_result['passage'] = generated_passage
        task_result['questions'] = generate_questions_list
        logger.info(f"[PROCESS QUERY TASK SUCCESSFUL]. Task ID: {task_id}. Sucessfully processed query.")

        return task_result
    
    except Exception as e:
        task_result['status'] = validate_status(ProcessingStatus.UNKNOWN_ERROR)
        task_result['error_message'] = f"An error has occurred during main execution: {e}"
        logger.error(f"[PROCESS QUERY TASK FAILED]. Task ID: {task_id}, Task result: {task_result['error_message']}", exc_info=True)
        raise self.retry(exc=e, countdown = 30)