import os
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

AI_INTEGRATIONS_OPENAI_API_KEY = os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY")
AI_INTEGRATIONS_OPENAI_BASE_URL = os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL")

# Make OpenAI client optional - only initialize if API key is provided
openai_client = None
if AI_INTEGRATIONS_OPENAI_API_KEY:
    openai_client = OpenAI(
        api_key=AI_INTEGRATIONS_OPENAI_API_KEY,
        base_url=AI_INTEGRATIONS_OPENAI_BASE_URL
    )


def is_rate_limit_error(exception):
    error_msg = str(exception)
    return (
        "429" in error_msg
        or "RATELIMIT_EXCEEDED" in error_msg
        or "quota" in error_msg.lower()
        or "rate limit" in error_msg.lower()
        or (hasattr(exception, "status_code") and exception.status_code == 429)
    )


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    retry=retry_if_exception(is_rate_limit_error),
    reraise=True
)
def generate_code(prompt: str, code_type: str = "python", context: dict = None) -> dict:
    """Generate SQL or Python code from natural language prompt."""
    
    if openai_client is None:
        return {
            "error": "OpenAI API key not configured. Set AI_INTEGRATIONS_OPENAI_API_KEY environment variable to use LLM features.",
            "code": "",
            "explanation": "LLM service unavailable",
            "required_inputs": []
        }
    
    context_str = ""
    if context:
        context_str = f"\n\nAvailable variables from previous nodes:\n{context}"
    
    system_prompt = f"""You are an expert code generator. Generate {code_type} code based on the user's request.
{context_str}

Rules:
1. Return ONLY valid {code_type} code
2. Do not include markdown code blocks
3. For SQL: Return only the SQL query
4. For Python: Return executable Python code
5. Use any available variables from previous nodes in your code
6. Be concise and efficient

Output format:
Return a JSON object with:
- "code": The generated code
- "explanation": Brief explanation of what the code does
- "required_inputs": List of input variables needed from previous nodes
"""
    
    # the newest OpenAI model is "gpt-5" which was released August 7, 2025.
    # do not change this unless explicitly requested by the user
    response = openai_client.chat.completions.create(
        model="gpt-5",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ],
        response_format={"type": "json_object"},
        max_completion_tokens=4096
    )
    
    import json
    result = json.loads(response.choices[0].message.content)
    return result


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    retry=retry_if_exception(is_rate_limit_error),
    reraise=True
)
def analyze_output(output: str, analysis_prompt: str) -> dict:
    """Analyze output from a node execution using LLM."""
    
    if openai_client is None:
        return {
            "error": "OpenAI API key not configured. Set AI_INTEGRATIONS_OPENAI_API_KEY environment variable to use LLM features.",
            "answer": "LLM service unavailable",
            "extracted_values": {},
            "summary": "Cannot analyze without OpenAI API key"
        }
    
    system_prompt = """You are an expert data analyst. Analyze the provided output and answer the user's question.

Output format:
Return a JSON object with:
- "answer": Your analysis result
- "extracted_values": Any specific values extracted from the output
- "summary": Brief summary of the output
"""
    
    # the newest OpenAI model is "gpt-5" which was released August 7, 2025.
    # do not change this unless explicitly requested by the user
    response = openai_client.chat.completions.create(
        model="gpt-5",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Output to analyze:\n{output}\n\nQuestion: {analysis_prompt}"}
        ],
        response_format={"type": "json_object"},
        max_completion_tokens=2048
    )
    
    import json
    result = json.loads(response.choices[0].message.content)
    return result
