import streamlit as st
import pymysql
import pandas as pd
import time
import duckdb
import socket


# =========================
# 🌐 URL 자동 표시
# =========================
hostname = socket.gethostname()
ipaddr = socket.gethostbyname(hostname)
st.info(f"📌 앱 URL: http://{ipaddr}:8501")


# =========================
# 🐬 MySQL 연결
# =========================
dbConn = pymysql.connect(
    user='remote_user',
    passwd='chari0t12#4%',
    host='192.168.145.128',
    db='madang',
    charset='utf8'
)
cursor = dbConn.cursor(pymysql.cursors.DictCursor)

def query(sql):
    cursor.execute(sql)
    return cursor.fetchall()


# =========================
# 🦆 DuckDB 연결 및 CSV 로딩
# =========================
duck = duckdb.connect("madang.duckdb")

duck.execute("""
    CREATE TABLE IF NOT EXISTS Customer AS
    SELECT * FROM read_csv_auto('C:/Users/lovew/Downloads/Customer_madang.csv');
""")

duck.execute("""
    CREATE TABLE IF NOT EXISTS Book AS
    SELECT * FROM read_csv_auto('C:/Users/lovew/Downloads/Book_madang.csv');
""")

duck.execute("""
    CREATE TABLE IF NOT EXISTS Orders AS
    SELECT * FROM read_csv_auto('C:/Users/lovew/Downloads/Orders_madang.csv');
""")


# =========================
# 📚 책 리스트 (MySQL)
# =========================
books = [None]
result = query("select concat(bookid, ',', bookname) as item from Book")
for res in result:
    books.append(res["item"])


# =========================
# 🏷 탭 구성
# =========================
tab1, tab2, tab3 = st.tabs(["고객조회", "거래 입력", "DuckDB 조회"])


# =========================
# 🔍 고객 조회 (MySQL)
# =========================
name = tab1.text_input("고객명 입력")
custid = None

if len(name) > 0:
    sql = f"""
        select c.custid, c.name, b.bookname, o.orderdate, o.saleprice
        from Customer c
        join Orders o on c.custid = o.custid
        join Book b on o.bookid = b.bookid
        where c.name = '{name}';
    """
    result = query(sql)

    if len(result) == 0:
        tab1.warning("❗ 고객이 존재하지 않습니다.")
    else:
        df = pd.DataFrame(result)
        tab1.write(df)
        custid = df["custid"][0]
        
# 🧾 거래 입력 (MySQL)
# =========================
if custid:
    tab2.write(f"📌 고객번호: {custid}")
    tab2.write(f"📌 고객명: {name}")

    select_book = tab2.selectbox("구매 서적:", books)

    if select_book:
        bookid = select_book.split(",")[0]
        today = time.strftime('%Y-%m-%d')
        orderid = query("select max(orderid) as oid from Orders")[0]["oid"] + 1
        price = tab2.text_input("금액")

        if tab2.button("거래 입력"):
            sql = f"""
                insert into Orders (orderid, custid, bookid, saleprice, orderdate)
                values ({orderid}, {custid}, {bookid}, {price}, '{today}');
            """
            cursor.execute(sql)
            dbConn.commit()
            tab2.success("거래가 입력되었습니다! 🎉")


# =========================
# 🦆 DuckDB 조회 기능
# =========================
tab3.header("🦆 DuckDB 데이터 조회")

if tab3.button("Customer 조회 (DuckDB)"):
    df = duck.execute("SELECT * FROM Customer").df()
    tab3.dataframe(df)

if tab3.button("Book 조회 (DuckDB)"):
    df = duck.execute("SELECT * FROM Book").df()
    tab3.dataframe(df)

if tab3.button("Orders 조회 (DuckDB)"):
    df = duck.execute("SELECT * FROM Orders").df()
    tab3.dataframe(df)