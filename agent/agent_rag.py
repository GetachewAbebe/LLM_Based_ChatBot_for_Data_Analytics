from langchain.agents import initialize_agent, Tool
from langchain.agents import AgentType
from langchain.llms import OpenAI
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_connection():
    connection = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DATABASE"),
        user=os.getenv("POSTGRES_USERNAME"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT")
    )
    return connection

def run_agent(query):
    # Initialize LangChain LLM
    llm = OpenAI(temperature=0)

    # Define a custom tool for generating SQL queries based on the user's query
    tools = [
        Tool(
            name="SQL Generator",
            func=lambda query: generate_sql(query),
            description="Generates SQL queries based on the user's question."
        )
    ]

    # Initialize LangChain agent with the tool
    agent = initialize_agent(tools, llm, agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION, verbose=True)
    
    # Run the agent with the user query
    result = agent.run(query)
    return result

def generate_sql(query):
    """
    This function generates SQL queries based on the user input query.
    For this case, generating a query for the top 10 cities with the most views.
    """
    if "cities with most views" in query.lower():
        sql_query = """
        SELECT city, SUM(views) AS total_views
        FROM cities
        GROUP BY city
        ORDER BY total_views DESC
        LIMIT 10;
        """
        return sql_query
    else:
        return "Query not recognized. Please ask about the top cities with most views."
