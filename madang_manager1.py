import streamlit as st
import duckdb
import pandas as pd

# DuckDB 파일에 연결 (없으면 자동 생성)
conn = duckdb.connect("madang.duckdb")

# Streamlit UI
name = st.text_input("고객명")

if name:
    sql = f"""
        SELECT c.name, b.bookname, o.orderdate, o.saleprice
        FROM Customer c
        JOIN Orders o ON c.custid = o.custid
        JOIN Book b ON o.bookid = b.bookid
        WHERE c.name = '{name}'
    """
    result = conn.execute(sql).df()
    st.write(result)

