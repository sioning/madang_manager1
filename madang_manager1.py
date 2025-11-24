import streamlit as st
import duckdb
import pandas as pd

# ----------------------------
# 1) DuckDB 연결
# ----------------------------
conn = duckdb.connect("madang.duckdb")

# ----------------------------
# 2) 테이블 생성 (없으면 자동 생성)
# ----------------------------
conn.execute("""
CREATE TABLE IF NOT EXISTS Customer(
    custid INTEGER PRIMARY KEY,
    name VARCHAR
);
""")

conn.execute("""
CREATE TABLE IF NOT EXISTS Book(
    bookid INTEGER PRIMARY KEY,
    bookname VARCHAR
);
""")

conn.execute("""
CREATE TABLE IF NOT EXISTS Orders(
    orderid INTEGER PRIMARY KEY,
    custid INTEGER,
    bookid INTEGER,
    saleprice INTEGER,
    orderdate DATE
);
""")

# ----------------------------
# 3) 기본 데이터 삽입 (없을 때만)
# ----------------------------
# Customer
conn.execute("""
INSERT OR IGNORE INTO Customer VALUES
(1, '박나은'),
(2, '김철수'),
(3, '이영희'),
(4, '홍길동'),
(5, '최지우'),
(6, '정우성');
""")

# Book
conn.execute("""
INSERT OR IGNORE INTO Book VALUES
(1, '축구의 역사'),
(2, '축구 아는 여자'),
(3, '축구의 신'),
(4, '야구의 신'),
(5, '야구의 역사'),
(6, '테니스 바이블'),
(7, '피겨퀸'),
(8, '피겨 교과서'),
(9, '농구의 신'),
(10, '농구의 역사');
""")

# Orders
conn.execute("""
INSERT OR IGNORE INTO Orders VALUES
(1, 1, 1, 6000,  '2014-07-01'),
(2, 1, 3, 21000, '2014-07-03'),
(3, 2, 5, 8000,  '2014-07-03'),
(4, 3, 6, 6000,  '2014-07-04'),
(5, 4, 7, 20000, '2014-07-05'),
(6, 1, 2, 12000, '2014-07-07'),
(7, 4, 8, 13000, '2014-07-07'),
(8, 3, 10,12000, '2014-07-08'),
(9, 2, 10,7000,  '2014-07-09'),
(10,3, 8, 13000, '2014-07-10'),
(11,6, 8, 14000, '2014-07-11');
""")

# ----------------------------
# 4) Streamlit UI
# ----------------------------
st.title("고객 주문 조회 (DuckDB 버전)")

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
    if result.empty:
        st.write("해당 고객의 주문 기록이 없습니다.")
    else:
        st.write(result)
