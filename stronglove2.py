import streamlit as st
import pandas as pd
import time
import duckdb
import os

# =========================
#  앱 기본 정보
# =========================
st.set_page_config(page_title="Madang Manager", page_icon="📚")

st.title("📚 Madang Manager (DuckDB 버전)")
st.caption("만든 사람: 박나은 ✨")

st.info(
    "📌 이 앱은 DuckDB 파일(`madang.duckdb`)과 CSV 파일을 사용해 동작해요.\n"
    "현재 보고 있는 브라우저 주소(URL)가 배포된 앱의 주소예요."
)

# =========================
#  DuckDB 연결 & 초기화
# =========================
# 리포지터리 루트에 madang.duckdb 파일을 저장합니다.
duck = duckdb.connect("madang.duckdb")


def ensure_table_from_csv(table_name: str, csv_file: str):
    """
    DuckDB 내부에 table_name 이 없으면,
    같은 디렉토리의 csv_file 을 읽어서 테이블을 생성한다.
    """
    result = duck.execute(
        "SELECT COUNT(*) AS cnt FROM information_schema.tables WHERE table_name = ?",
        [table_name.lower()],
    ).fetchone()
    exists = result[0] if result is not None else 0

    if exists == 0:
        if not os.path.exists(csv_file):
            st.error(f"❗ `{csv_file}` 파일을 찾을 수 없어요. GitHub 리포지터리에 올려주세요.")
            return
        duck.execute(
            f"""
            CREATE TABLE {table_name} AS
            SELECT * FROM read_csv_auto(?)
            """,
            [csv_file],
        )


# CSV로부터 초기 테이블 생성 (처음 한 번만 생성됨)
ensure_table_from_csv("Customer", "Customer_madang.csv")
ensure_table_from_csv("Book", "Book_madang.csv")
ensure_table_from_csv("Orders", "Orders_madang.csv")


def duck_query_df(sql: str, params=None) -> pd.DataFrame:
    if params is None:
        params = []
    return duck.execute(sql, params).df()


# =========================
#  책 리스트 (DuckDB)
# =========================
books = [None]
df_books = duck_query_df("SELECT bookid, bookname FROM Book ORDER BY bookid")
for _, row in df_books.iterrows():
    # bookid,bookname 형태로 표시
    books.append(f"{row['bookid']},{row['bookname']}")


# =========================
#  탭 구성
# =========================
tab1, tab2, tab3 = st.tabs(["고객 조회 / 추가", "거래 입력", "DuckDB 전체 조회"])

# 세션 상태에 custid 저장해서 탭 간에 공유
if "selected_custid" not in st.session_state:
    st.session_state.selected_custid = None
if "selected_name" not in st.session_state:
    st.session_state.selected_name = None


# =========================
#  🔍 고객 조회 + 새 고객 추가 (DuckDB)
# =========================
with tab1:
    st.subheader("🔍 고객 조회")

    name_input = st.text_input("고객명 입력")

    if len(name_input) > 0:
        sql = """
            SELECT c.custid,
                   c.name,
                   c.address,
                   c.phone,
                   b.bookname,
                   o.orderdate,
                   o.saleprice
            FROM Customer c
            JOIN Orders o ON c.custid = o.custid
            JOIN Book b   ON o.bookid = b.bookid
            WHERE c.name = ?
            ORDER BY o.orderdate;
        """
        df = duck_query_df(sql, [name_input])

        if df.empty:
            st.warning("❗ 해당 이름의 고객이 존재하지 않습니다.")
            st.session_state.selected_custid = None
            st.session_state.selected_name = None
        else:
            st.success(f"✅ {name_input} 고객의 거래 내역입니다.")
            st.dataframe(df)
            st.session_state.selected_custid = int(df["custid"].iloc[0])
            st.session_state.selected_name = df["name"].iloc[0]

    st.markdown("---")
    st.subheader("➕ 새 고객 추가 (기본값: 박나은)")

    new_name = st.text_input("새 고객 이름", value="박나은")
    new_address = st.text_input("주소", value="")
    new_phone = st.text_input("전화번호", value="")

    if st.button("새 고객 추가"):
        # 새로운 custid 생성
        df_max = duck_query_df("SELECT COALESCE(MAX(custid), 0) + 1 AS next_id FROM Customer")
        next_id = int(df_max["next_id"].iloc[0])

        duck.execute(
            """
            INSERT INTO Customer (custid, name, address, phone)
            VALUES (?, ?, ?, ?)
            """,
            [next_id, new_name, new_address, new_phone],
        )
        duck.commit()

        st.success(f"🎉 고객이 추가되었습니다! (custid={next_id}, 이름={new_name})")


# =========================
#  🧾 거래 입력 (DuckDB)
# =========================
with tab2:
    st.subheader("🧾 거래 입력")

    custid = st.session_state.selected_custid
    name = st.session_state.selected_name

    if custid is None:
        st.info("먼저 1번 탭에서 고객을 조회하거나, 새 고객을 추가해 주세요.")
    else:
        st.write(f"📌 선택된 고객번호: **{custid}**")
        st.write(f"📌 선택된 고객명: **{name}**")

        select_book = st.selectbox("구매 서적 선택", books)

        if select_book:
            bookid = int(select_book.split(",")[0])
            today = time.strftime("%Y-%m-%d")

            df_max_oid = duck_query_df("SELECT COALESCE(MAX(orderid), 0) + 1 AS next_oid FROM Orders")
            orderid = int(df_max_oid["next_oid"].iloc[0])

            price = st.text_input("금액 (정수로 입력)", value="0")

            if st.button("거래 입력"):
                try:
                    saleprice = int(price)
                except ValueError:
                    st.error("❗ 금액은 숫자로만 입력해 주세요.")
                else:
                    duck.execute(
                        """
                        INSERT INTO Orders (orderid, custid, bookid, saleprice, orderdate)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        [orderid, custid, bookid, saleprice, today],
                    )
                    duck.commit()
                    st.success("🎉 거래가 DuckDB에 입력되었습니다!")


# =========================
#  🦆 DuckDB 조회 탭
# =========================
with tab3:
    st.header("🦆 DuckDB 테이블 조회")

    if st.button("Customer 조회"):
        df = duck_query_df("SELECT * FROM Customer ORDER BY custid")
        st.dataframe(df)

    if st.button("Book 조회"):
        df = duck_query_df("SELECT * FROM Book ORDER BY bookid")
        st.dataframe(df)

    if st.button("Orders 조회"):
        df = duck_query_df("SELECT * FROM Orders ORDER BY orderid")
        st.dataframe(df)
