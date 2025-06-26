import json
import re
import config
from openai import OpenAI
from collections import Counter

from backend import prompt_templates

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

# Core Functions
def initialize_llm_clients(model_choice):
    safe_model_choice = model_choice.strip()

    try:
        if safe_model_choice == config.OPENAI_MODEL_CHOICE.strip():
            api_key = config.OPENAI_API_KEY
            if not api_key: raise ValueError("OpenAI API key not found")
            client = OpenAI(api_key=api_key)
            logger.info("OpenAI client initialized for evaluation.")
            return client

        elif safe_model_choice == config.DEEPSEEK_MODEL_CHOICE.strip():
            api_key = config.DEEPSEEK_API_KEY
            if not api_key: raise ValueError("DeepSeek API key not found")
            client = OpenAI(
                base_url=config.DEEPSEEK_BASEURL,
                api_key=api_key
            )
            logger.info("DeepSeek client initialized for evaluation.")
            return client

        else:
            raise ValueError(f"Unsupported model for evaluation: '{safe_model_choice}'")
            
    except Exception as e:
        logger.critical(f"An error occurred when initializing LLM for evaluation: {e}", exc_info=True)

        return None

def call_llm_chat(client, model_name, system_prompt, user_prompt):
    try:
        response = client.chat.completions.create(
            model = model_name,
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )

        return response.choices[0].message.content
    
    except Exception as e:
        raise Exception(f"An error has occurred during the {model_name} API call: {e}")
    
def get_feedback(evaluation_results, questions_data):
    # Error handling for input
    if not isinstance(evaluation_results, list) or not isinstance(questions_data, list):
        logger.error("Invalid input format for get_feedback.")
        return ("Error generating feedback due to invalid input format.", [])
    
    if not evaluation_results:
        logger.warning("No evaluation results to process for feedback.")

        return ("Could not parse the evaluation results to provide feedback.", [])

    # Map question numbers to types
    questions_type_map = {
        q.get('number'): q.get('type')
        for q in questions_data
        if isinstance(q, dict) and 'number' in q and 'type' in q
    }

    incorrect_answers_count_by_type = Counter()
    struggling_type = set()
    found_incorrect = False 

    for result in evaluation_results:
        if isinstance(result, dict) and result.get('evaluation') == 'Incorrect':
            found_incorrect = True
            q_num = result.get('number')
            q_type = questions_type_map.get(q_num, "Unknown Type")
            incorrect_answers_count_by_type[q_type] += 1
            struggling_type.add(q_type)

    struggling_type_list = sorted(list(struggling_type))
    if "Unknown Type" in struggling_type_list and len(struggling_type_list) > 1:
        struggling_type_list.remove("Unknown Type")

    feedback_text = "===FEEDBACK===\n"
    
    if struggling_type_list:
        feedback_text += "Based on your performance, the types of questions you might want to practice more are:\n"
        for q_type in struggling_type_list:
            count = incorrect_answers_count_by_type[q_type]
            feedback_text += f"- {q_type} ({count} incorrect)\n"
    elif found_incorrect:
        feedback_text += "You have some incorrect answers. Review the detailed evaluation above to see them.\n"
    else:
        feedback_text += "Excellent! It appears you have answered all questions correctly.\n"

    return feedback_text, struggling_type_list

def parse_evaluation_string(evaluation_string):
    results = []
    question_pattern = re.compile(r"Question (\d+):")
    answer_pattern = re.compile(r"- Your answer: (.*?)\n- Evaluation: (.*?)\n- Correct answer: (.*?)\n- Explanation: (.*)")

    # Split and remove the first empty string
    questions = question_pattern.split(evaluation_string)[1:]
    for i in range(0, len(questions), 2):
        try:
            number = questions[i]
            match = answer_pattern.search(questions[i + 1])
            if match:
                your_answer, evaluation, correct_answer, explanation = match.groups()
                results.append({
                    "number": int(number),
                    "your_answer": your_answer.strip(),
                    "evaluation": evaluation.strip(),
                    "correct_answer": correct_answer.strip(),
                    "explanation": explanation.strip()
                })

            else:
                logger.warning(f"Could not parse evaluation for Question {number}. Skipping.")

        except (ValueError, IndexError) as e:
            logger.error(f"An error has occurred when trying to parse questions {i // 2 + 1}: {e}")
            continue

    return results

@celery_app.task(bind=True, max_retries=3, default_retry_delay=60, acks_late=True)
def evaluate_answers_task(self, model_choice, passage_content, questions_string, user_answers, questions_data):
    task_id = self.request.id
    logger.info(f"[EVALUATION TASK START]. Task ID: {task_id}.")
    
    try:
        llm_client = initialize_llm_clients(model_choice)
        if not llm_client:
            raise ValueError("LLM client initialization has failed")
        
        # Determine the specific API model name to use
        model_name_for_api = ""
        safe_model_choice = model_choice.strip()
        if safe_model_choice == config.OPENAI_MODEL_CHOICE.strip():
            model_name_for_api = config.OPENAI_MODEL
        elif safe_model_choice == config.DEEPSEEK_MODEL_CHOICE.strip():
            model_name_for_api = config.DEEPSEEK_MODEL
        
        if not model_name_for_api:
            raise ValueError(f"Could not determine the API model name for the evaluation choice: {model_choice}")

        # Get the prompts from our new centralized file
        system_prompt, user_prompt = prompt_templates.get_evaluation_prompts(
            passage_content, questions_string, user_answers
        )

        evaluation = call_llm_chat(llm_client, model_name_for_api, system_prompt, user_prompt)
        evaluation_results = parse_evaluation_string(evaluation)
        feedback, struggling_types = get_feedback(evaluation_results, questions_data)   

        result = {
            "evaluation": evaluation, 
            "feedback": feedback, 
            "struggling_types": struggling_types
        }

        logger.info(f"[EVALUATION TASK SUCCESS]. Task ID: {task_id}")
        return result

    except Exception as e:
        logger.error(f"[EVALUATE TASK FAILED]. Task ID: {task_id}, Error: {e}", exc_info=True)
        raise self.retry(exc=e, countdown=30)