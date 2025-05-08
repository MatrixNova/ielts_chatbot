import sys
import os
import logging

# --- 1. Setup Project Root Path ---
# (Keep this as it was working)
project_root = None
try:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
except Exception as e:
    logging.basicConfig(level=logging.ERROR)
    logging.error(f"CRITICAL: Failed to set project root path: {e}", exc_info=True)
    raise

# --- 2. Import 'config' and Initialize Logger ---
# (Keep this as it was working)
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

# --- 3. Standard Library Imports ---
import chainlit as cl
from celery.result import AsyncResult
import time
import json

# --- 4. Application-Specific Imports ---
# (Keep this as it was working)
try:
    from celery_app import celery_app
    from backend.query_service import process_query_task
    from backend.evaluation_service import evaluate_answers_task
    from backend.chatlog_storage import buffer_chat_log
    logger.info("Successfully imported application modules (Celery, backend).")
except ImportError as e:
    logger.error("ImportError while loading application modules.", exc_info=True)
    raise

# --- Helper Functions (Unchanged) ---
def format_questions_for_display(questions_list):
    # ... (same as before)
    if not questions_list: return "No questions were generated."
    display_text = "Here are your questions:\n\n"
    for q_item in questions_list:
        display_text += f"**Question {q_item.get('number', 'N/A')}: ({q_item.get('type', 'Unknown Type')})**\n"
        display_text += f"{q_item.get('text', 'Error: Question text missing.')}\n\n"
    return display_text

def format_evaluation_for_display(evaluation_string):
    # ... (same as before)
    if not evaluation_string: return "No evaluation data received."
    return evaluation_string

# --- Define Default Actions ---
INITIAL_ACTIONS_LIST = [
    cl.Action(name="generate_new_passage", value="new_passage", payload={'value': "new_passage"}, label="📚 Practice with a new passage (random topic)"),
    cl.Action(name="generate_custom_passage", value="custom_passage", payload={'value': "custom_passage"}, label="✏️ Enter a topic for a new passage"),
    cl.Action(name="change_llm", value="change_llm", payload={'value': "change_llm"}, label="⚙️ Change LLM Model")
]

# --- Action Callback Handlers (Chainlit 2.x Style) ---

@cl.action_callback("llm_selected") # Handles clicks on any button named "llm_selected"
async def on_llm_selected(action: cl.Action):
    chat_id = cl.user_session.get("chat_id")
    
    # MODIFIED: Retrieve value from action.payload
    chosen_llm = action.payload.get('value') # Get the value (model choice string) from the action's payload

    logger.info(f"Chat ID {chat_id}: Action '{action.name}' clicked. Chosen LLM: {chosen_llm}")
    buffer_chat_log(chat_id, "User", f"Clicked action: {action.name}, Value: {chosen_llm}")

    cl.user_session.set("llm_choice", chosen_llm)
    await cl.Message(content=f"LLM model changed to: {chosen_llm}").send()
    buffer_chat_log(chat_id, "System", f"LLM changed to {chosen_llm}.")
    cl.user_session.set("state", "INITIAL") # Reset state

    # Resend initial actions
    await cl.Message(
        content="Please choose an action:",
        actions=INITIAL_ACTIONS_LIST 
    ).send()
    # await action.remove() # Optional

@cl.action_callback("generate_custom_passage")
async def on_custom_passage(action: cl.Action):
    chat_id = cl.user_session.get("chat_id")
    logger.info(f"Chat ID {chat_id}: Action '{action.name}' clicked.")
    buffer_chat_log(chat_id, "User", f"Clicked action: {action.name}")
    cl.user_session.set("state", "AWAITING_TOPIC")
    await cl.Message(content="Great! Please enter the topic you'd like the passage to be about.").send()
    # await action.remove() # Optional

@cl.action_callback("change_llm")
async def on_change_llm(action: cl.Action):
    chat_id = cl.user_session.get("chat_id")
    logger.info(f"Chat ID {chat_id}: Action '{action.name}' clicked.")
    buffer_chat_log(chat_id, "User", f"Clicked action: {action.name}")
    # Ask user to choose LLM
    actions = [
        cl.Action(name="llm_selected", value=config.OPENAI_MODEL_CHOICE, payload={'value': config.OPENAI_MODEL_CHOICE}, label=f"🤖 {config.OPENAI_MODEL_CHOICE}"),
        cl.Action(name="llm_selected", value=config.MISTRAL_MODEL_CHOICE, payload={'value': config.MISTRAL_MODEL_CHOICE}, label=f"🤖 {config.MISTRAL_MODEL_CHOICE}"),
        cl.Action(name="llm_selected", value=config.DEEPSEEK_MODEL_CHOICE, payload={'value': config.DEEPSEEK_MODEL_CHOICE}, label=f"🤖 {config.DEEPSEEK_MODEL_CHOICE}")
    ]
    await cl.Message(content="Select the LLM model you would like to use:", actions=actions).send()
    # State doesn't strictly need changing here, the "llm_selected" action callback will handle the choice
    # await action.remove() # Optional

@cl.action_callback("llm_selected") # Handles clicks on any button named "llm_selected"
async def on_llm_selected(action: cl.Action):
    chat_id = cl.user_session.get("chat_id")
    chosen_llm = action.value # Get the value (model choice string) from the clicked action
    logger.info(f"Chat ID {chat_id}: Action '{action.name}' clicked. Chosen LLM: {chosen_llm}")
    buffer_chat_log(chat_id, "User", f"Clicked action: {action.name}, Value: {chosen_llm}")

    cl.user_session.set("llm_choice", chosen_llm)
    await cl.Message(content=f"LLM model changed to: {chosen_llm}").send()
    buffer_chat_log(chat_id, "System", f"LLM changed to {chosen_llm}.")
    cl.user_session.set("state", "INITIAL") # Reset state

    # Resend initial actions
    await cl.Message(
        content="Please choose an action:",
        actions=INITIAL_ACTIONS_LIST # MODIFIED
    ).send()
    # await action.remove() # Optional

@cl.action_callback("end_session")
async def on_end_session(action: cl.Action):
    chat_id = cl.user_session.get("chat_id")
    logger.info(f"Chat ID {chat_id}: Action '{action.name}' clicked.")
    buffer_chat_log(chat_id, "User", f"Clicked action: {action.name}")
    await cl.Message(content="Thank you for practicing! Your session has ended.").send()
    buffer_chat_log(chat_id, "System", "Session ended by user.")
    cl.user_session.set("state", "ENDED")
    # await action.remove() # Optional


# --- Chainlit Lifecycle Hooks ---

@cl.on_chat_start
async def start_chat():
    """Handles the beginning of a new chat session."""
    chat_id_val = str(time.time())
    cl.user_session.set("chat_id", chat_id_val)
    cl.user_session.set("state", "INITIAL")
    llm_model_choice = getattr(config, 'OPENAI_MODEL_CHOICE', 'default_llm')
    cl.user_session.set("llm_choice", llm_model_choice)

    logger.info(f"Chat started. Session ID: {chat_id_val}, LLM: {llm_model_choice}")
    await cl.Message(
        content=f"Welcome to the IELTS Reading Practice Assistant! (Using {llm_model_choice})\nWhat would you like to do?"
    ).send()
    buffer_chat_log(chat_id_val, "System", "Chat started. Offered initial actions.")

    # Present initial actions - names must match @cl.action_callback decorators
    await cl.Message(
        content="Please choose an action:",
        actions=INITIAL_ACTIONS_LIST # MODIFIED
    ).send()

@cl.on_message
async def main_logic(message: cl.Message):
    """Handles incoming messages from the user."""
    chat_id = cl.user_session.get("chat_id")
    current_state = cl.user_session.get("state")
    llm_choice = cl.user_session.get("llm_choice")
    user_message_content = message.content

    logger.info(f"Chat ID {chat_id}: Received message '{user_message_content}' in state '{current_state}'")
    buffer_chat_log(chat_id, "User", user_message_content)

    if current_state == "AWAITING_TOPIC":
        await cl.Message(content="Generating your passage and questions...").send()
        buffer_chat_log(chat_id, "System", f"User provided topic: {user_message_content}. Triggering passage generation.")
        try:
            task = process_query_task.delay(query=user_message_content, chosen_LLM=llm_choice)
            if task and task.id:
                logger.info(f"Chat ID {chat_id}: Dispatched process_query_task. Task ID: {task.id}")
                cl.user_session.set("last_task_id", task.id)
                cl.user_session.set("state", "AWAITING_PASSAGE_RESULT")
                await cl.Message(content=f"Task submitted (ID: {task.id}). You can type 'status' or wait.").send()
            else:
                logger.error(f"Chat ID {chat_id}: process_query_task.delay() did NOT return a valid task object or ID.")
                await cl.Message(content="Error: Failed to get task ID.").send()
                cl.user_session.set("state", "INITIAL") # Reset state
                await cl.Message(content="Please choose an action:", actions=INITIAL_ACTIONS_LIST).send() # MODIFIED
        except Exception as dispatch_error:
            logger.error(f"Chat ID {chat_id}: FAILED to dispatch process_query_task!", exc_info=True)
            await cl.Message(content="Sorry, error submitting request.").send()
            cl.user_session.set("state", "INITIAL") # Reset state
            await cl.Message(content="Please choose an action:", actions=INITIAL_ACTIONS_LIST).send() # MODIFIED


    elif current_state == "AWAITING_ANSWERS":
        await cl.Message(content="Evaluating your answers...").send()
        buffer_chat_log(chat_id, "System", "User submitted answers. Triggering evaluation.")
        passage_content = cl.user_session.get("current_passage")
        questions_string_for_eval = cl.user_session.get("current_questions_str")
        questions_data_for_eval = cl.user_session.get("current_questions_data")

        if not all([passage_content, questions_string_for_eval, questions_data_for_eval]):
            logger.warning(f"Chat ID {chat_id}: Missing data for evaluation. Resetting.")
            await cl.Message(content="Error: Could not find data for evaluation. Please start over.").send()
            cl.user_session.set("state", "INITIAL")
            await cl.Message(content="Please choose an action:", actions=INITIAL_ACTIONS_LIST).send() # MODIFIED
            return
        try:
            task = evaluate_answers_task.delay(
                model_choice=llm_choice, passage_content=passage_content,
                questions_string=questions_string_for_eval, user_answers=user_message_content,
                questions_data=questions_data_for_eval
            )
            if task and task.id:
                logger.info(f"Chat ID {chat_id}: Dispatched evaluate_answers_task. Task ID: {task.id}")
                cl.user_session.set("last_task_id", task.id)
                cl.user_session.set("state", "AWAITING_EVALUATION_RESULT")
                await cl.Message(content=f"Evaluation task (ID: {task.id}) submitted. Type 'status' or wait.").send()
            else:
                logger.error(f"Chat ID {chat_id}: evaluate_answers_task.delay() did NOT return valid task object/ID.")
                await cl.Message(content="Error: Failed to get task ID for evaluation.").send()
                cl.user_session.set("state", "INITIAL")
                await cl.Message(content="Please choose an action:", actions=INITIAL_ACTIONS_LIST).send() # MODIFIED
        except Exception as dispatch_error:
            logger.error(f"Chat ID {chat_id}: FAILED to dispatch evaluate_answers_task!", exc_info=True)
            await cl.Message(content="Sorry, error submitting evaluation.").send()
            cl.user_session.set("state", "INITIAL")
            await cl.Message(content="Please choose an action:", actions=INITIAL_ACTIONS_LIST).send() # MODIFIED


    elif user_message_content.lower() == "status":
        task_id = cl.user_session.get("last_task_id")
        if not task_id:
            await cl.Message(content="No active task to check.").send()
            return

        logger.info(f"Chat ID {chat_id}: Checking status for task {task_id}")
        task_result_obj = AsyncResult(task_id, app=celery_app)
        status_msg = f"Status for task {task_id}: {task_result_obj.state}."

        if task_result_obj.successful():
            result = task_result_obj.get()
            status_msg += " Task completed successfully!"
            logger.info(f"Chat ID {chat_id}: Task {task_id} successful.")
            await cl.Message(content=status_msg).send()

            current_task_type_state = cl.user_session.get("state")
            if current_task_type_state == "AWAITING_PASSAGE_RESULT":
                if result and result.get("status") == "query_success":
                    passage = result.get("passage")
                    questions_list = result.get("questions")
                    cl.user_session.set("current_passage", passage)
                    cl.user_session.set("current_questions_data", questions_list)
                    formatted_q_str = format_questions_for_display(questions_list)
                    cl.user_session.set("current_questions_str", formatted_q_str)
                    await cl.Message(content=f"**Your Reading Passage:**\n\n{passage}").send()
                    await cl.Message(content=formatted_q_str).send()
                    await cl.Message(content="Please provide answers...").send()
                    cl.user_session.set("state", "AWAITING_ANSWERS")
                    buffer_chat_log(chat_id, "System", "Passage/questions delivered.")
                else:
                    error_msg = result.get('error_message', 'Unknown error') if result else 'No result'
                    logger.error(f"Chat ID {chat_id}: Passage task {task_id} error: {error_msg}")
                    await cl.Message(content=f"Error processing query: {error_msg}. Please try again.").send()
                    cl.user_session.set("state", "INITIAL")
                    await cl.Message(content="Please choose an action:", actions=INITIAL_ACTIONS_LIST).send() # MODIFIED
                    buffer_chat_log(chat_id, "System", f"Passage gen failed: {error_msg}")

            elif current_task_type_state == "AWAITING_EVALUATION_RESULT":
                evaluation_output = result.get("evaluation")
                feedback_output = result.get("feedback")
                if evaluation_output is None or feedback_output is None:
                    logger.error(f"Chat ID {chat_id}: Eval task {task_id} result incomplete. Result: {result}")
                    await cl.Message(content="Issue with evaluation results. Please try again.").send()
                else:
                    full_response = f"{format_evaluation_for_display(evaluation_output)}\n\n{feedback_output}"
                    await cl.Message(content=full_response).send()
                    buffer_chat_log(chat_id, "System", "Evaluation delivered.")
                cl.user_session.set("state", "INITIAL") # Always reset state after evaluation
                # Present follow-up actions
                await cl.Message(
                    content="What would you like to do next?",
                    actions=[ # Names must match action callbacks
                        cl.Action(name="generate_new_passage", value="new_passage", payload={'value': "new_passage"}, label="📚 Practice with another new passage"),
                        cl.Action(name="generate_custom_passage", value="custom_passage", payload={'value': "custom_passage"}, label="✏️ Enter a new topic"),
                        cl.Action(name="change_llm", value="change_llm", payload={'value': "change_llm"}, label="⚙️ Change LLM Model"),
                        cl.Action(name="end_session", value="end_session", payload = {'value': "end_session"}, label="🏁 End Session")
                    ]
                ).send()

        elif task_result_obj.failed():
            status_msg += f" Task failed. Reason: {task_result_obj.traceback}" # Note: traceback might be None
            logger.error(f"Chat ID {chat_id}: Task {task_id} failed. Traceback: {task_result_obj.traceback}")
            await cl.Message(content=status_msg).send()
            cl.user_session.set("state", "INITIAL")
            await cl.Message(content="Please choose an action:", actions=INITIAL_ACTIONS_LIST).send() # MODIFIED
            buffer_chat_log(chat_id, "System", f"Task {task_id} failed.")
        else: # PENDING, RETRY, STARTED etc.
            logger.info(f"Chat ID {chat_id}: Task {task_id} status: {task_result_obj.state}")
            await cl.Message(content=status_msg).send()

    else: # Unhandled state or message
        if cl.user_session.get("state") == "INITIAL":
            await cl.Message(content="Please choose an action from the buttons above, or type 'status' if you are waiting for a result.").send()
        elif cl.user_session.get("state") == "ENDED":
            await cl.Message(content="Your session has ended.").send()
        else:
            logger.warning(f"Chat ID {chat_id}: Unhandled message in state '{current_state}'. Offering main menu.")
            await cl.Message(content="I'm not sure how to handle that right now. Let's go back to the main options.").send()
            cl.user_session.set("state", "INITIAL")
            await cl.Message(content="Please choose an action:", actions=INITIAL_ACTIONS_LIST).send() # MODIFIED

# --- End of chainlit_app.py ---