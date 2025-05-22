import streamlit as st
import openai
import snowflake.connector
import pandas as pd
import os

# Title
st.title("🔍 Natural Language to SQL with Snowflake + OpenAI")

# User input
query = st.text_input("Enter your question about the dataset:")

# OpenAI setup
openai.api_key = st.secrets["openai_api_key"]

def generate_sql(nl_query):
    prompt = f"Convert this natural language question into SQL:\nQuestion: {nl_query}\nSQL:"
    response = openai.Completion.create(
        engine="text-davinci-003",
        prompt=prompt,
        max_tokens=150,
        temperature=0
    )
    return response.choices[0].text.strip()

def run_sql(sql):
    conn = snowflake.connector.connect(
        user=st.secrets["snowflake_user"],
        password=st.secrets["snowflake_password"],
        account=st.secrets["snowflake_account"],
        warehouse=st.secrets["snowflake_warehouse"],
        database=st.secrets["snowflake_database"],
        schema=st.secrets["snowflake_schema"]
    )
    cur = conn.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    return pd.DataFrame(data, columns=cols)

# Main logic
if query:
    with st.spinner("Generating SQL..."):
        sql = generate_sql(query)
        st.code(sql, language="sql")

    try:
        with st.spinner("Running SQL on Snowflake..."):
            result_df = run_sql(sql)
            st.success("Query executed successfully!")
            st.dataframe(result_df)
    except Exception as e:
        st.error(f"Error: {e}")
