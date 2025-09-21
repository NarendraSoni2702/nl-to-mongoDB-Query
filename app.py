import streamlit as st
from parser import parse_aggregation_query
from schema import SCHEMA

st.title("Natural Language to MongoDB Aggregation Pipeline")

nl_query = st.text_area("Enter your query:", height=200)

if st.button("Parse"):
    if nl_query.strip():
        result = parse_aggregation_query(nl_query, schema=SCHEMA)
        st.subheader("MongoDB Aggregation Pipeline Output")
        st.json(result)
    else:
        st.warning("Please enter a natural language query.")
