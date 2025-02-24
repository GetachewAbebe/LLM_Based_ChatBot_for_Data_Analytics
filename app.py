import streamlit as st
from agent.agent_rag import run_agent

def main():
    st.title("LangChain Chatbot")
    user_query = st.text_input("Ask a question:")

    if user_query:
        sql_query = run_agent(user_query)
        st.write(f"Generated SQL Query: {sql_query}")

if __name__ == "__main__":
    main()
