def get_passage_generation_prompts(context: str, query: str) -> tuple[str, str]:
    """
    Generates the system and user prompts for creating an IELTS reading passage.
    """
    system_prompt = "You are an IELTS Reading expert. Your task is to generate an IELTS-style academic reading passage."
    
    if context:
        prompt_context = context
    else:
        prompt_context = f"A comprehensive, 700-800 word IELTS-style academic reading passage about the topic: {query}"

    user_prompt = (
        f"""Based on the following, generate an IELTS-style academic reading passage.
        The passage should be approximately 700–800 words long, organized into 4–6 paragraphs, with an academic tone and a suitable title.
        Do NOT include any questions, answers, or extra instructions.

        Context:
        \"\"\"{prompt_context}\"\"\"
        """
    )
    return system_prompt, user_prompt

def get_question_generation_prompts(passage: str) -> tuple[str, str]:
    """
    Generates the system and user prompts for creating IELTS questions from a passage.
    """
    system_prompt = "You are an IELTS Reading expert tasked with generating 10 IELTS-style questions for the provided passage."
    user_prompt = (
        f"""Your task is to output ONLY a valid JSON array of 10 question objects based on the passage below.
        Alternate between question types like Multiple choice, True/False/Not Given, Matching, and Completion.
        Each object MUST have these keys: "number" (integer), "type" (string), "text" (string).
        CRITICAL: Your entire response must be ONLY the JSON array, starting with '[' and ending with ']'. No markdown, no commentary.

        Passage:
        \"\"\"{passage}\"\"\"
        """
    )
    return system_prompt, user_prompt

def get_evaluation_prompts(passage_content: str, questions_string: str, user_answers: str) -> tuple[str, str]:
    """
    Generates the system and user prompts for evaluating user answers.
    This is the improved prompt from our last discussion.
    """
    system_prompt = (
        """You are an IELTs Reading Expert. Your task is to evaluate the user's answers based on the provided passage and questions. 
        You will provide the correct answers and grade the user's submission. Your output format must be perfect for automated parsing.
        """
    )
    user_prompt = (
        f"""You are a machine that outputs structured text for an application. Follow all formatting rules exactly.

        **CRITICAL INSTRUCTIONS:**
        1. Go through each question from the 'Questions' section.
        2. Evaluate if the user's answer is Correct or Incorrect based ONLY on the Passage.
        3. State the Correct Answer based ONLY on the Passage.
        4. Provide a brief explanation ONLY if the user's answer is Incorrect. If the answer is Correct, the explanation MUST be exactly "N/A".
        5. Your ENTIRE output MUST begin with "===DETAILED EVALUATION===" and end with "===FINAL GRADE===".
        6. DO NOT add any conversational text, introductions, or summaries.
        7. DO NOT change the labels (e.g., "- Your answer:", "- Evaluation:").

        **Passage:**
        \"\"\"{passage_content}\"\"\"

        **Questions:**
        \"\"\"{questions_string}\"\"\"

        **User's answers:**
        \"\"\"{user_answers}\"\"\"

        **REQUIRED OUTPUT FORMAT:**
        ===DETAILED EVALUATION===
        Question 1:
        - Your answer: [The user's answer for Question 1]
        - Evaluation: [Correct/Incorrect]
        - Correct answer: [The correct answer for Question 1]
        - Explanation: [Brief explanation if incorrect, otherwise MUST be "N/A"]
        
        (Repeat this exact structure for ALL questions)

        ===FINAL GRADE===
        Total questions answered correctly: [Number of correct questions] / [Total number of questions]
        Score Percentage: [Calculated Percentage]%
        """
    )
    return system_prompt, user_prompt