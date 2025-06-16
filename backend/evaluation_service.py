import json
import re
import config
from openai import OpenAI
from collections import Counter

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
    try:
        if model_choice == config.OPENAI_MODEL_CHOICE:
            openai_api_key = config.OPENAI_API_KEY

            if not openai_api_key:
                raise ValueError("An error has occurred. OpenAI API key not found")
            
            client = OpenAI(api_key = openai_api_key)
            logger.info("OpenAI client has been successfully initialized")

            return client

        elif model_choice == config.DEEPSEEK_API_KEY:
            deepseek_api_key = config.DEEPSEEK_API_KEY
            
            if not deepseek_api_key:
                raise ValueError("An error has occurred. DeepSeek API key not found")

            client = OpenAI(
                    base_url = config.DEEPSEEK_BASEURL,
                    api_key = deepseek_api_key
                )
            
            logger.info("DeepSeek client has been successfully initialized")

            return client
        
        else:
            raise ValueError(f"Unsupported model {model_choice}. Please choose {config.OPENAI_MODEL_CHOICE} or {config.DEEPSEEK_MODEL_CHOICE}")
            
    except Exception as e:
        logger.critical(f"An error has occured when initializing LLM: {e}")

        return None
    
def create_evaluation_prompt(passage_content, questions_string, user_answers):
    system_prompt = (
        """You are an IELTs Reading Expert. Your task is to evaluate the user's answers based on the provided passage and questions. 
        Provide the correct answers and grade the user's submission.
        """
    )

    user_prompt = (
        f""" You are an IELTs Reading Expert. Your task is to evaluate the user's answers based on the provided passage and questions. 
        Provide the correct answers and grade the user's submission.

        Passage:
        \"\"\"{passage_content}\"\"\"

        Questions:
        \"\"\"{questions_string}\"\"\"

        User's answers:
        \"\"\"{user_answers}\"\"\"

        Please follow this structure:
        
        Instructions:
        1. Go through each question number found in the 'Questions' section.
        2. For each question number, find the corresponding answer in the 'User's answers' section.
        3. Evaluate if the user's answer is correct or incorrect based ONLY on the provided Passage.
        4. State the correct answer based ONLY on the provided passage.
        5. If the user's answer is incorrect, provide a brief explanation referencing the SPECIFIC part of the passage that supports the correct answer. 
        Keep explanations concise.
        6. After evaluating all questions, count the number of 'Correct' answers.
        7. Calculate the total number of questions evaluated based on the 'Questions' input section.
        8. Calculate the score percentage (Number Correct / Total Questions * 100), rounded to the nearest whole number.
        9. Format the output exactly as specified below, including the Detailed Evaluation and the Final Grade with the score percentage.

        Format the output clearly as shown below:
        ===DETAILED EVALUATION===
        Question 1:
        - Your answer: [user's answer for Question 1.]
        - Evaluation: [Correct/Incorrect.]
        - Correct answer: [Correct answer for Question 1.]
        - Explanation: [Brief explanation referencing the passage if the user's answer is incorrect.
        Write N/A if the answer is correct.]

        Question 2:
        - Your answer: [user's answer for Question 2.]
        - Evaluation: [Correct/Incorrect.]
        - Correct answer: [Correct answer for Question 2.]
        - Explanation: [Brief explanation referencing the passage if the user's answer is incorrect.
        Write N/A if the answer is correct.]

        (Repeat this process for ALL questions numbered in the 'Question' input)

        ===FINAL GRADE===
        Total questions answered correctly: [Number of correct questions] / [Total number of questions]
        Score Percentage: [Calculated Percentage]%

        """
    )

    return system_prompt, user_prompt

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
    
def get_feedback(evaluation_results, questions_data):
    # Error handling for input
    if not isinstance(evaluation_results, list) or not isinstance(questions_data, list):
        logger.error("An error has occured. Invalid input format for get_feedback. Expecting list,"
                     "got evaluation_results = %s, questions_data = %s", 
                     type(evaluation_results), type(questions_data))
        
        return ("Error generating feedback due to invalid input format", [])
    
    if not evaluation_results:
        if questions_data:
            logger.warning("Could not parse the evaluation results. Unable to provide feedback")
            return ("Could not parse the evaluation results. Unable to provide feedback", [])
        
        else:
            logger.warning("No question or results detected to be evaluate")
            return ("No question or results detected to be evaluate", [])

    # Map question numbers to types
    questions_type_map = {
        q.get('number'): q.get('type')
        for q in questions_data
        if isinstance(q, dict) and 'number' in q and 'type' in q
    }

    unknown_type_label = "Unknown types of questions"
    unknown_types = not bool(questions_type_map) and bool(questions_data)

    if unknown_types:
        logger.warning("Question formatting error. Could not map question types.")
    
    # Count the incorrect answers by type
    incorrect_answers_count_by_type = Counter()
    struggling_type = set()
    found_incorrect = False 

    for result in evaluation_results:
        if isinstance(result, dict) and result.get('evaluation') == 'Incorrect':
            found_incorrect = True
            q_num = result.get('number')
            q_type = questions_type_map.get(q_num) if q_num is not None else None

            if q_type:
                incorrect_answers_count_by_type[q_type] += 1
                struggling_type.add(q_type)

            else:
                """ Only count unknown if if q_num exists but not in the map
                or if q_num was missing in results
                """

                incorrect_answers_count_by_type[unknown_type_label] += 1
                struggling_type.add(unknown_type_label)

        elif not isinstance(result, dict) or 'evaluation' not in result:
            logger.warning(f"Skipping invalid item in evaluation results: {result}")

    # Refining the list of struggling questions types
    struggling_type_list = sorted(list(struggling_type))

    if unknown_type_label in struggling_type and len(struggling_type) > 1:
        struggling_type.remove(unknown_type_label)
        struggling_type_list = sorted(list(struggling_type))

    # Feedback message generation:
    feedback = "===FEEDBACK===\n"
    struggling_questions_identified = struggling_type_list and struggling_type_list != [unknown_type_label]

    if struggling_questions_identified:
        feedback.append("Based on your performance, the types of questions you might want to practice more are: \n")

        for q_type in struggling_type_list:
            count = incorrect_answers_count_by_type[q_type]
            feedback.append(f"- {q_type} ({count} incorrect)\n")

        if unknown_types and incorrect_answers_count_by_type[unknown_type_label] > 0:
             feedback.append(f"- (Also {incorrect_answers_count_by_type[unknown_type_label]}"
                             "incorrect answers where the type couldn't be identified.)\n")
        feedback.append("\n")

    # Incorrect answers exist, but their types could not be identified    
    elif found_incorrect:
        count_unknown = incorrect_answers_count_by_type[unknown_type_label]
        
        if count_unknown > 0:
            feedback.append(f"There are {count_unknown} incorrect answers." 
                            "However, the types of questions cannot be specified for feedback this time\n")

        else:
            feedback.append("There are some incorrect answers, but their specific types cannot be determined.")
    
    # No incorrect answer
    elif evaluation_results:
        feedback.append("Excellent! It looks like you have answered all questions correctly\n")

    else:
        feedback.append("No specific areas for improvements identified from this evaluation\n")
    
    # Generate follow up question
    follow_up_questions = "Congratulation on completing the Reading passage. What would you like to do next?\n"
    option_letter = 'A'

    # Option A: Practice with the types of questions the user is struggling with
    if struggling_questions_identified:
        weak_types_str = ", ".join(struggling_type_list)
        follow_up_questions .append(
            f"{next_option}. Practice {weak_types_str} questions.\n"
        )
        next_option = chr(ord(next_option) + 1)

        follow_up_questions.append(
        f"{next_option}. Try a new passage and questions.\n"
    )
    next_option = chr(ord(next_option) + 1)

    follow_up_questions.append(
        f"{next_option}. Retry this passage with new questions.\n"
    )
    next_option = chr(ord(next_option) + 1)

    follow_up_questions.append("D. End session.\n")

    valid_choices = [chr(ord("A") + i) for i in range(ord(next_option) - ord("A"))]
    follow_up_questions.append(f"Enter choice ({', '.join(valid_choices)}): \n")

    full_response = "".join(feedback + follow_up_questions)
    practice_types = struggling_type_list if struggling_questions_identified else []

    return full_response, practice_types

def parse_evaluation_string(evaluation_string):
    results = []
    question_pattern = re.compile(r"Question (\d+):")
    answer_pattern = re.compile(r"- Your answer: (.*?)\n- Evaluation: (.*?)\n- Correct answer: (.*?)\n- Explanation: (.*)")

    questions = question_pattern.split(evaluation_string)[1:]  # Split and remove the first empty string
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

@celery_app.task(bind = True, max_retries = 3, default_retry_delay = 60, acks_late = True)
def evaluate_answers_task(self, model_choice, passage_content, questions_string, user_answers, questions_data):
    task_id = self.request.id
    logger.info(f"[EVALUATION TASK START]. Task ID: {task_id}.")
    
    try:
        # Step 1: CORRECTLY initialize the LLM client
        llm_client = initialize_llm_clients(model_choice)

        if not llm_client:
            raise ValueError("LLM client initialization has failed")
        
        # Step 2: Determine the correct API model name based on the choice
        model_name_for_api = ""
        if model_choice == config.OPENAI_MODEL_CHOICE:
            model_name_for_api = config.OPENAI_MODEL
        elif model_choice == config.DEEPSEEK_MODEL_CHOICE:
            model_name_for_api = config.DEEPSEEK_MODEL
        elif model_choice == config.MISTRAL_MODEL_CHOICE:
            # This logic was missing before
            model_name_for_api = config.MISTRAL_MODEL
        
        if not model_name_for_api:
            raise ValueError(f"Could not determine the API model name for the choice: {model_choice}")

        # Step 3: Create the prompts
        system_prompt, user_prompt = create_evaluation_prompt(passage_content, questions_string, user_answers)

        # Step 4: Call the LLM with the correct client and model name
        evaluation = call_llm_chat(llm_client, model_name_for_api, system_prompt, user_prompt)

        # Step 5: Process the results (no changes here)
        evaluation_results = parse_evaluation_string(evaluation)
        feedback, struggling_types = get_feedback(evaluation_results, questions_data)   

        result =  {
            "evaluation": evaluation, 
            "feedback": feedback, 
            "struggling_types": struggling_types
            }

        logger.info(f"[EVALUATION TASK SUCCESS]. Task ID: {task_id}")

        return result

    except Exception as e:
        logger.error(f"[EVALUATE TASK FAILED]. Task ID: {task_id}, Error: {e}", exc_info=True)
        # Using raise self.retry() will allow Celery to attempt the task again
        raise self.retry(exc=e, countdown=30)