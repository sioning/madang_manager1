import streamlit as st 
import pymysql
import pandas as pd
dbConn = pymysql.connect(user='remote_user', passwd='chari0t12#4%', host='192.168.145.128', db='madang', charset='utf8')
cursor = dbConn.cursor(pymysql.cursors.DictCursor)

name = st.text_input("고객명")
if name is not None:
    sql = "select c.name, b.bookname, o.orderdate, o.saleprice from Customer c, Book b, Orders o \
           where c.custid = o.custid and o.bookid = b.bookid and name = '" + name + "';"
    cursor.execute(sql)
    result = cursor.fetchall()
    result = pd.DataFrame(result)
    st.write(result)
