import chainlit as cl
from celery.result import AsyncResult
import time
import json # For parsing question data if stored as JSON string in user_session

# Assuming your Celery app and tasks are accessible
# Adjust imports based on your project structure
try:
    from celery_app import celery_app # Your Celery application instance
    from backend.query_service import process_query_task # Celery task for passage/question generation
    from backend.evaluation_service import evaluate_answers_task # Celery task for evaluation
    from backend.chatlog_storage import buffer_chat_log # For logging interactions
    import config # For LLM choices, etc.
except ImportError as e:
    print(f"Error importing backend modules or Celery app. Ensure paths are correct. {e}")
    # A simple fallback if imports fail, Chainlit might not fully work.
    # This helps in identifying import issues when starting Chainlit.
    # You might want to raise the error or handle it more gracefully.
    raise

# --- Helper Functions ---
def format_questions_for_display(questions_list):
    """Formats the JSON questions list into a readable string."""
    if not questions_list:
        return "No questions were generated."
    display_text = "Here are your questions:\n\n"
    for q_item in questions_list:
        display_text += f"**Question {q_item.get('number', 'N/A')}: ({q_item.get('type', 'Unknown Type')})**\n"
        display_text += f"{q_item.get('text', 'Error: Question text missing.')}\n\n"
    return display_text

def format_evaluation_for_display(evaluation_string):
    """Placeholder for formatting the raw evaluation string if needed.
       Currently, the LLM is prompted to format it well."""
    if not evaluation_string:
        return "No evaluation data received."
    # The evaluation string from the LLM is expected to be pre-formatted.
    # You might add specific parsing here if you need to extract parts of it.
    return evaluation_string


# --- Chainlit Event Handlers ---

@cl.on_chat_start
async def start_chat():
    cl.user_session.set("chat_id", str(time.time())) # Simple unique ID for chat session
    cl.user_session.set("state", "INITIAL")
    cl.user_session.set("llm_choice", config.OPENAI_MODEL_CHOICE) # Default LLM

    await cl.Message(
        content=f"Welcome to the IELTS Reading Practice Assistant! (Using {cl.user_session.get('llm_choice')})\nWhat would you like to do?"
    ).send()
    await cl. lựa_chọn( # Using cl.Action for choices
        name="initial_actions",
        content="Please choose an action:",
        actions=[
            cl.Action(name="generate_new_passage", value="new_passage", label="📚 Practice with a new passage (random topic)"),
            cl.Action(name="generate_custom_passage", value="custom_passage", label="✏️ Enter a topic for a new passage"),
            cl.Action(name="change_llm", value="change_llm", label="⚙️ Change LLM Model")
        ]
    ).send()
    buffer_chat_log(cl.user_session.get("chat_id"), "System", "Chat started. Offered initial actions.")

@cl.on_message
async def main_logic(message: cl.Message):
    chat_id = cl.user_session.get("chat_id")
    current_state = cl.user_session.get("state")
    llm_choice = cl.user_session.get("llm_choice")
    user_message_content = message.content

    # Log user message
    buffer_chat_log(chat_id, "User", user_message_content)

    if current_state == "AWAITING_TOPIC":
        await cl.Message(content="Generating your passage and questions. This may take a moment...").send()
        buffer_chat_log(chat_id, "System", f"User provided topic: {user_message_content}. Triggering passage generation.")
        # Trigger Celery task for passage and question generation
        task = process_query_task.delay(query=user_message_content, chosen_LLM=llm_choice)
        cl.user_session.set("last_task_id", task.id)
        cl.user_session.set("state", "AWAITING_PASSAGE_RESULT")
        await cl.Message(content=f"Task submitted (ID: {task.id}). I'll let you know when it's ready. You can also type 'status'.").send()
        # We don't block here; user can type 'status' or wait for proactive update (more complex)

    elif current_state == "AWAITING_ANSWERS":
        await cl.Message(content="Evaluating your answers. This might take a moment...").send()
        buffer_chat_log(chat_id, "System", f"User submitted answers. Triggering evaluation.")
        passage_content = cl.user_session.get("current_passage")
        questions_string_for_eval = cl.user_session.get("current_questions_str") # String version for LLM
        questions_data_for_eval = cl.user_session.get("current_questions_data") # Parsed list for feedback logic

        if not passage_content or not questions_string_for_eval or not questions_data_for_eval:
            await cl.Message(content="Error: Could not find the passage or questions for evaluation. Please start over.").send()
            cl.user_session.set("state", "INITIAL")
            return

        task = evaluate_answers_task.delay(
            model_choice=llm_choice, # Or a specific evaluation model from config
            passage_content=passage_content,
            questions_string=questions_string_for_eval, # The formatted string of questions
            user_answers=user_message_content,
            questions_data=questions_data_for_eval # The structured list of question objects
        )
        cl.user_session.set("last_task_id", task.id)
        cl.user_session.set("state", "AWAITING_EVALUATION_RESULT")
        await cl.Message(content=f"Evaluation task submitted (ID: {task.id}). I'll let you know when it's ready. You can also type 'status'.").send()

    elif user_message_content.lower() == "status":
        task_id = cl.user_session.get("last_task_id")
        if not task_id:
            await cl.Message(content="No active task to check status for.").send()
            return

        task_result = AsyncResult(task_id, app=celery_app) # Pass your Celery app instance
        status_message = f"Status for task {task_id}: {task_result.state}."

        if task_result.successful():
            result = task_result.get()
            status_message += " Task completed successfully!"
            await cl.Message(content=status_message).send() # Send status first

            # Process based on which task it was (passage or evaluation)
            current_task_type_state = cl.user_session.get("state") # e.g., AWAITING_PASSAGE_RESULT

            if current_task_type_state == "AWAITING_PASSAGE_RESULT":
                if result.get("status") == "query_success":
                    passage = result.get("passage")
                    questions_list = result.get("questions") # This should be a list of dicts

                    cl.user_session.set("current_passage", passage)
                    cl.user_session.set("current_questions_data", questions_list) # Store structured data
                    
                    formatted_q_str = format_questions_for_display(questions_list)
                    cl.user_session.set("current_questions_str", formatted_q_str) # Store formatted string for display/evaluation

                    await cl.Message(content=f"**Your Reading Passage:**\n\n{passage}").send()
                    await cl.Message(content=formatted_q_str).send()
                    await cl.Message(content="Please provide your answers in a single message, numbered (e.g., 1. Answer A, 2. True, ...).").send()
                    cl.user_session.set("state", "AWAITING_ANSWERS")
                    buffer_chat_log(chat_id, "System", "Passage and questions delivered.")
                else:
                    await cl.Message(content=f"Error processing your query: {result.get('error_message', 'Unknown error')}. Please try again.").send()
                    cl.user_session.set("state", "INITIAL") # Reset state
                    buffer_chat_log(chat_id, "System", f"Passage generation failed: {result.get('error_message')}")

            elif current_task_type_state == "AWAITING_EVALUATION_RESULT":
                evaluation_output = result.get("evaluation") # The detailed evaluation string from LLM
                feedback_output = result.get("feedback")     # The feedback and next steps string

                full_response = f"{format_evaluation_for_display(evaluation_output)}\n\n{feedback_output}"
                await cl.Message(content=full_response).send()
                buffer_chat_log(chat_id, "System", "Evaluation delivered.")
                # Offer next actions based on feedback (this part of feedback generation is in evaluation_service.py)
                # For simplicity, just resetting here. A more robust app would parse feedback for actions.
                cl.user_session.set("state", "INITIAL")
                await cl. lựa_chọn(
                    name="follow_up_actions",
                    content="What would you like to do next?",
                    actions=[
                        cl.Action(name="generate_new_passage", value="new_passage", label="📚 Practice with another new passage"),
                        cl.Action(name="generate_custom_passage", value="custom_passage", label="✏️ Enter a new topic"),
                        cl.Action(name="change_llm", value="change_llm", label="⚙️ Change LLM Model"),
                        cl.Action(name="end_session", value="end_session", label="🏁 End Session")
                    ]
                ).send()


        elif task_result.failed():
            status_message += f" Task failed. Reason: {task_result.traceback}"
            await cl.Message(content=status_message).send()
            cl.user_session.set("state", "INITIAL") # Reset state on failure
            buffer_chat_log(chat_id, "System", f"Task {task_id} failed.")
        else:
            status_message += " Task is still processing or in an unknown state."
            await cl.Message(content=status_message).send()
    else:
        # Fallback for unhandled states or messages
        await cl.Message(content="I'm not sure how to handle that. Please choose an action or type 'status' if you are waiting for a result.").send()
        # Resend initial actions if in a confused state.
        if cl.user_session.get("state") == "INITIAL":
            await cl. lựa_chọn(
                name="initial_actions",
                content="Please choose an action:",
                actions=[
                    cl.Action(name="generate_new_passage", value="new_passage", label="📚 Practice with a new passage (random topic)"),
                    cl.Action(name="generate_custom_passage", value="custom_passage", label="✏️ Enter a topic for a new passage"),
                    cl.Action(name="change_llm", value="change_llm", label="⚙️ Change LLM Model")
                ]
            ).send()

@cl.on_action_clicked
async def handle_action(action: cl.Action):
    chat_id = cl.user_session.get("chat_id")
    buffer_chat_log(chat_id, "User", f"Clicked action: {action.name} (Value: {action.value})")

    if action.value == "new_passage":
        await cl.Message(content="Okay, generating a new passage and questions on a random IELTS-like topic. This may take a moment...").send()
        # For a truly random topic, you might have a predefined list or let the LLM choose.
        # Here, we'll just use a generic query for `process_query_task`.
        task = process_query_task.delay(query="a typical IELTS academic reading topic", chosen_LLM=cl.user_session.get("llm_choice"))
        cl.user_session.set("last_task_id", task.id)
        cl.user_session.set("state", "AWAITING_PASSAGE_RESULT")
        await cl.Message(content=f"Task submitted (ID: {task.id}). Type 'status' to check progress.").send()
        return cl.message.Message(content="") # Consume the action message

    elif action.value == "custom_passage":
        cl.user_session.set("state", "AWAITING_TOPIC")
        await cl.Message(content="Great! Please enter the topic you'd like the passage to be about.").send()
        return cl.message.Message(content="") 

    elif action.value == "change_llm":
        cl.user_session.set("state", "AWAITING_LLM_CHOICE")
        await cl. lựa_chọn(
            name="llm_selection",
            content="Select the LLM model you'd like to use:",
            actions=[
                cl.Action(name=config.OPENAI_MODEL_CHOICE, value=config.OPENAI_MODEL_CHOICE, label=f"🤖 {config.OPENAI_MODEL_CHOICE}"),
                cl.Action(name=config.MISTRAL_MODEL_CHOICE, value=config.MISTRAL_MODEL_CHOICE, label=f"🌬️ {config.MISTRAL_MODEL_CHOICE}"),
                # Add DeepSeek if it's used for generation/evaluation too
            ]
        ).send()
        return cl.message.Message(content="") 
    
    elif cl.user_session.get("state") == "AWAITING_LLM_CHOICE":
        # This action is the LLM selected by the user
        chosen_llm = action.value
        cl.user_session.set("llm_choice", chosen_llm)
        await cl.Message(content=f"LLM model changed to: {chosen_llm}").send()
        buffer_chat_log(chat_id, "System", f"LLM changed to {chosen_llm}.")
        cl.user_session.set("state", "INITIAL")
        # Resend initial actions
        await cl. lựa_chọn(
            name="initial_actions",
            content="Please choose an action:",
            actions=[
                cl.Action(name="generate_new_passage", value="new_passage", label="📚 Practice with a new passage (random topic)"),
                cl.Action(name="generate_custom_passage", value="custom_passage", label="✏️ Enter a topic for a new passage"),
                cl.Action(name="change_llm", value="change_llm", label="⚙️ Change LLM Model")
            ]
        ).send()
        return cl.message.Message(content="") 

    elif action.value == "end_session":
        await cl.Message(content="Thank you for practicing! Your session has ended.").send()
        buffer_chat_log(chat_id, "System", "Session ended by user.")
        # Optionally clear user session, though Chainlit handles sessions.
        # cl.user_session.clear() # This might not be what you want if you want to review later.
        cl.user_session.set("state", "ENDED")
        return cl.message.Message(content="") 

    # Fallback for other actions or states
    await cl.Message(content=f"Action '{action.name}' received.").send()
    return cl.message.Message(content="") # Important to return or send a message to acknowledge


# To run this Chainlit app:
# 1. Save as chainlit_app.py (or any other name)
# 2. Open your terminal in the directory of this file.
# 3. Run: chainlit run chainlit_app.py -w
# The -w flag enables auto-reloading when you change the file.