import sys
import os
import logging
import random
import time
import json

import chainlit as cl
from chainlit.element import TaskList, Task
from chainlit.message import Message
from celery.result import AsyncResult

# Project Root Path
project_root = None
try:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
except Exception as e:
    logging.basicConfig(level=logging.ERROR)
    logging.error(f"CRITICAL: Failed to set project root path: {e}", exc_info=True)
    raise

try:
    import config
    logger = logging.getLogger(config.APP_NAME)
    if hasattr(config, 'setup_logging') and callable(config.setup_logging):
        config.setup_logging()
        logger.info(f"Logging configured. Project root '{project_root}' in sys.path.")
    else:
        logger.warning(f"config.setup_logging() not found/callable.")
except Exception as e:
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)
    logger.error(f"Error during config/logging setup: {e}. Using basic logger.", exc_info=True)

# Import Celery
try:
    from celery_app import celery_app
    from backend.query_service import process_query_task
    from backend.evaluation_service import evaluate_answers_task
    from backend.chatlog_storage import buffer_chat_log
    logger.info("Successfully imported application modules (Celery, backend).")
except ImportError as e:
    logger.error("ImportError while loading application modules.", exc_info=True)
    raise

# Helper Functions
def format_questions_for_display(questions_list):
    if not questions_list: return "No questions were generated."
    display_text = "Here are your questions:\n\n"
    for q_item in questions_list:
        display_text += f"**Question {q_item.get('number', 'N/A')}: ({q_item.get('type', 'Unknown Type')})**\n"
        display_text += f"{q_item.get('text', 'Error: Question text missing.')}\n\n"
    return display_text

def format_evaluation_for_display(evaluation_string):
    if not evaluation_string: return "No evaluation data received."
    return evaluation_string

# Define the actions
INITIAL_ACTIONS_LIST = [
    cl.Action(name="generate_new_passage", value="new_passage", payload={'value': "new_passage"}, label="📚 Practice with a new passage (random topic)"),
    cl.Action(name="generate_custom_passage", value="custom_passage", payload={'value': "custom_passage"}, label="✏️ Enter a topic for a new passage"),
    cl.Action(name="change_llm", value="change_llm", payload={'value': "change_llm"}, label="⚙️ Change LLM Model")
]
# Automatic result display
async def await_task_result(task_id: str, task_list_ui: TaskList):
    """
    Polls a Celery task until it's ready and returns the result.
    Updates the UI with the task status.
    """
    result_obj = AsyncResult(task_id, app=celery_app)
    try:
        while not result_obj.ready():
            await cl.sleep(2)
        
        task_list_ui.tasks[0].status = cl.TaskStatus.DONE
        await task_list_ui.send()

        if result_obj.successful():
            return result_obj.get()
        else:
            logger.error(f"Task {task_id} failed with traceback: {result_obj.traceback}")
            return {"status": "TASK_FAILED", "error_message": "The background task failed. Please check the logs."}
    except Exception as e:
        logger.error(f"Error while awaiting task {task_id}: {e}", exc_info=True)
        return {"status": "TASK_FAILED", "error_message": "An unexpected error occurred while monitoring the task."}

async def run_and_display_task(task_type: str, task_callable, **kwargs):
    task_list = TaskList(tasks=[
        Task(title=f"Running {task_type} task...", status=cl.TaskStatus.RUNNING)
    ])
    await task_list.send()

    try:
        task = task_callable.delay(**kwargs)
        if not task or not task.id:
            raise ConnectionError("Failed to dispatch task to Celery.")

        result = await await_task_result(task.id, task_list)
        
        # Process the result based on task type
        if task_type == "Passage Generation":
            if result and result.get("status") == "query_success":
                passage = result.get("passage")
                questions_list = result.get("questions")
                
                cl.user_session.set("current_passage", passage)
                cl.user_session.set("current_questions_data", questions_list)
                cl.user_session.set("current_questions_str", format_questions_for_display(questions_list))

                await cl.Message(content=f"### Your Reading Passage\n\n{passage}").send()
                await cl.Message(content=format_questions_for_display(questions_list)).send()
                await cl.Message(content="Please provide your answers in a single message, one per line.").send()
                cl.user_session.set("state", "AWAITING_ANSWERS")
            else:
                error_msg = result.get('error_message', 'An unknown error occurred.')
                await cl.Message(content=f"Sorry, the passage generation task failed: {error_msg}").send()
                await cl.Message(content="Please choose an action:", actions=INITIAL_ACTIONS_LIST).send()
                cl.user_session.set("state", "INITIAL")

        elif task_type == "Evaluation":
            if result and result.get("evaluation"):
                # Display the evaluation and the simplified feedback text
                await cl.Message(content=result.get("evaluation")).send()
                await cl.Message(content=result.get("feedback")).send()

                # Follow up actions
                follow_up_actions = []
                struggling_types = result.get("struggling_types", [])

                # 1. Add "Practice struggling types" button ONLY if there are any
                if struggling_types:
                    practice_label = ", ".join(struggling_types)
                    follow_up_actions.append(
                        cl.Action(name="practice_struggling", value="practice", payload={'value': practice_label}, label=f"🎯 Practice: {practice_label}")
                    )
                
                # 2. Add the standard follow-up buttons
                follow_up_actions.extend([
                    cl.Action(name="generate_new_passage", value="new_passage", payload={'value': "new_passage"}, label="📚 Try a new random passage"),
                    cl.Action(name="retry_passage", value="retry", payload={'value': "retry"}, label="🔄 Retry the same topic"),
                    cl.Action(name="end_session", value="end_session", payload={'value': "end_session"}, label="🏁 End Session")
                ])
                
                # Send the message with the new buttons
                await cl.Message(content="What would you like to do next?", actions=follow_up_actions).send()

            else:
                error_msg = result.get('error_message', 'An unknown error occurred.')
                await cl.Message(content=f"Sorry, the evaluation task failed: {error_msg}").send()
                # If eval fails, show the initial actions
                await cl.Message(content="Please choose an action:", actions=INITIAL_ACTIONS_LIST).send()
            
            cl.user_session.set("state", "INITIAL")

    except Exception as e:
        logger.error(f"Failed to run or display task '{task_type}': {e}", exc_info=True)
        await task_list.remove() # Clean up the UI element on error
        await cl.Message(content=f"An error occurred: {e}").send()


# Action Callbacks
@cl.on_chat_start
async def start_chat():
    chat_id_val = str(time.time())
    cl.user_session.set("chat_id", chat_id_val)
    cl.user_session.set("state", "INITIAL")

    llm_model_choice = getattr(config, 'OPENAI_MODEL_CHOICE', 'GPT 4.1')
    cl.user_session.set("llm_choice", llm_model_choice)
    logger.info(f"Chat started. Session ID: {chat_id_val}, LLM: {llm_model_choice}")
    await cl.Message(content=f"Welcome to the IELTS Reading Practice Assistant! (Using {llm_model_choice})\nWhat would you like to do?").send()
    buffer_chat_log(chat_id_val, "System", "Chat started. Offered initial actions.")
    await cl.Message(content="Please choose an action:", actions=INITIAL_ACTIONS_LIST).send()

@cl.action_callback("generate_new_passage")
async def on_new_passage(action: cl.Action):
    random_topic = random.choice(["history of artificial intelligence", "marine biology", "climate change effects"])
    await cl.Message(content=f"Alright! Generating a passage about '{random_topic}'...").send()
    await run_and_display_task(
        task_type="Passage Generation",
        task_callable=process_query_task,
        query=random_topic,
        chosen_LLM=cl.user_session.get("llm_choice")
    )

@cl.action_callback("generate_custom_passage")
async def on_custom_passage(action: cl.Action):
    cl.user_session.set("state", "AWAITING_TOPIC")
    await cl.Message(content="Great! Please enter the topic you'd like the passage to be about.").send()

@cl.action_callback("change_llm")
async def on_change_llm(action: cl.Action):
    actions = [
        cl.Action(name="llm_selected", value=config.OPENAI_MODEL_CHOICE, payload={'value': config.OPENAI_MODEL_CHOICE}, label=f"🤖 {config.OPENAI_MODEL_CHOICE}"),
        cl.Action(name="llm_selected", value=config.MISTRAL_MODEL_CHOICE, payload={'value': config.MISTRAL_MODEL_CHOICE}, label=f"🤖 {config.MISTRAL_MODEL_CHOICE}")
    ]
    await cl.Message(content="Select the LLM model for passage generation:", actions=actions).send()

@cl.action_callback("llm_selected")
async def on_llm_selected(action: cl.Action):
    chosen_llm = action.payload.get('value')
    cl.user_session.set("llm_choice", chosen_llm)
    await cl.Message(content=f"LLM model changed to: {chosen_llm}").send()
    cl.user_session.set("state", "INITIAL")
    await cl.Message(content="Please choose an action:", actions=INITIAL_ACTIONS_LIST).send()

@cl.action_callback("eval_model_selected")
async def on_eval_model_selected(action: cl.Action):
    eval_model_choice = action.payload.get("value")
    await cl.Message(content=f"Okay, evaluating with **{eval_model_choice}**...").send()
    
    await run_and_display_task(
        task_type="Evaluation",
        task_callable=evaluate_answers_task,
        model_choice=eval_model_choice,
        passage_content=cl.user_session.get("current_passage"),
        questions_string=cl.user_session.get("current_questions_str"),
        user_answers=cl.user_session.get("user_answers"),
        questions_data=cl.user_session.get("current_questions_data")
    )

@cl.on_message
async def main_logic(message: Message):
    current_state = cl.user_session.get("state")
    user_message_content = message.content
    logger.info(f"Chat ID {cl.user_session.get('chat_id')}: Received '{user_message_content}' in state '{current_state}'")

    if current_state == "AWAITING_TOPIC":
        cl.user_session.set("last_query", user_message_content)

        await cl.Message(content="Generating your passage and questions...").send()
        await run_and_display_task(
            task_type="Passage Generation",
            task_callable=process_query_task,
            query=user_message_content,
            chosen_LLM=cl.user_session.get("llm_choice")
        )

    elif current_state == "AWAITING_ANSWERS":
        cl.user_session.set("user_answers", user_message_content) # Save the answers
        
        eval_actions = [
            cl.Action(name="eval_model_selected", value=config.OPENAI_MODEL_CHOICE, payload={"value": config.OPENAI_MODEL_CHOICE}, label=f"🤖 {config.OPENAI_MODEL_CHOICE}"),
            cl.Action(name="eval_model_selected", value=config.DEEPSEEK_MODEL_CHOICE, payload={"value": config.DEEPSEEK_MODEL_CHOICE}, label=f"🤖 {config.DEEPSEEK_MODEL_CHOICE}"),
        ]
        await cl.Message(
            content="Your answers have been submitted. Please choose a model to evaluate them:",
            actions=eval_actions
        ).send()
        cl.user_session.set("state", "AWAITING_EVAL_MODEL_CHOICE")

    else:
        await cl.Message(content="Please choose an action from the buttons above.").send()