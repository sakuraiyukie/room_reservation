import datetime
import pandas as pd
import streamlit as st
import random
import requests
import json

page = st.sidebar.selectbox("Choose your page", ["users", "rooms", "bookings"])

if page == "users":
    st.title("ユーザー登録画面")

    with st.form(key="user"):
        user_name: str = st.text_input("ユーザー名", max_chars=24)
        data = {
            "user_name": user_name,
        }
        submit_button = st.form_submit_button(label="リクエスト送信")

    if submit_button:
        url = "https://sql_app-1-h3138769.deta.app/users"
        res = requests.post(
            url,
            data=json.dumps(data),
            headers={"Content-Type": "application/json; charset=utf-8"},
            verify=False
        )
        if res.status_code == 200:
            st.success("ユーザー登録完了")
        st.json(res.json())

elif page == "rooms":
    st.title("会議室登録画面")

    with st.form(key="user"):
        room_name: str = st.text_input("会議室名", max_chars=24)
        capacity: int = st.number_input("定員", step=1)
        data = {
            "room_name": room_name,
            "capacity": capacity,
        }
        submit_button = st.form_submit_button(label="リクエスト送信")

    if submit_button:
        url = "https://sql_app-1-h3138769.deta.app/rooms"
        res = requests.post(
            url,
            data=json.dumps(data),
            headers={"Content-Type": "application/json; charset=utf-8"},
            verify=False
        )
        if res.status_code == 200:
            st.success("会議室登録完了")
        st.json(res.json())

elif page == "bookings":
    st.title("会議室予約登録")
    # ユーザー一覧を取得
    url_users = "https://sql_app-1-h3138769.deta.app/users"
    res = requests.get(url_users, verify=False)
    users = res.json()
    # ユーザー名をキー、ユーザーIDをバリューに変更
    user2id = {user["user_name"]:user["user_id"] for user in users}

    # 会議室の取得
    url_rooms = "https://sql_app-1-h3138769.deta.app/rooms"
    res = requests.get(url_rooms, verify=False)
    rooms = res.json()
    # 会議室名をキー、会議室ID、定員をバリューに変更
    room2id = {
        room["room_name"]:{
            "room_id":room["room_id"],
            "capacity":room["capacity"]
        } for room in rooms
    }
    st.write("### 会議室一覧")
    df_rooms = pd.DataFrame(rooms)
    df_rooms = df_rooms.rename(columns={"room_name":"会議室名","capacity":"定員","room_id":"会議室ID"})
    st.table(df_rooms)

# 予約一覧の取得
    url_bookings = "https://sql_app-1-h3138769.deta.app//bookings"
    res = requests.get(url_bookings, verify=False)
    bookings = res.json()
    df_bookings = pd.DataFrame(bookings)
    id2user = {v: k for k, v in user2id.items()}
    id2room = {v["room_id"]:k for k, v in room2id.items()}
    if len(bookings)!=0:
        df_bookings["user_id"] = df_bookings["user_id"].map(id2user)
        df_bookings["room_id"] = df_bookings["room_id"].map(id2room)
        df_bookings["start_datetime"] = df_bookings["start_datetime"].apply(lambda x: datetime.datetime.fromisoformat(x).strftime("%Y/%m/%d %H:%M"))
        df_bookings["end_datetime"] = df_bookings["end_datetime"].apply(lambda x: datetime.datetime.fromisoformat(x).strftime("%Y/%m/%d %H:%M"))

        df_bookings = df_bookings.rename(columns={
            "user_id": "予約者名",
            "room_id": "会議室名",
            "booked_num": "予約人数",
            "start_datetime": "開始時刻",
            "end_datetime": "終了時刻",
            "booking_id": "予約番号",
        })

    st.write("### 予約一覧")
    st.table(df_bookings)
    #========↑この部分を追加========

    with st.form(key="booking"):
        user_name: str = st.selectbox("予約者名",user2id.keys())
        room_name: str = st.selectbox("会議室名",room2id.keys())
        booked_num: int = st.number_input("予約人数", step=1, min_value=1)
        date = st.date_input("日付", min_value=datetime.date.today())
        start_time = st.time_input("開始時刻", value=datetime.time(hour=9, minute=0))
        end_time = st.time_input("終了時刻", value=datetime.time(hour=20, minute=0))
        submit_button = st.form_submit_button(label="リクエスト送信")

    if submit_button:
        user_id: int = user2id[user_name]
        room_id: int = room2id[room_name]["room_id"]
        capacity: int = room2id[room_name]["capacity"]
        data = {
            "user_id": user_id,
            "room_id": room_id,
            "booked_num": booked_num,
            "start_datetime": datetime.datetime(
                year=date.year,
                month=date.month,
                day=date.day,
                hour=start_time.hour,
                minute=start_time.minute,
            ).isoformat(),
            "end_datetime": datetime.datetime(
                year=date.year,
                month=date.month,
                day=date.day,
                hour=end_time.hour,
                minute=end_time.minute,
            ).isoformat()
        }
# 定員より多い予約人数の場合
        if booked_num > capacity:
            st.error(f'{room_name}の定員は、{capacity}名です。{capacity}名以下の予約人数のみ受け付けております。')
        # 開始時刻 >= 終了時刻
        elif start_time >= end_time:
            st.error('開始時刻が終了時刻を越えています')
        elif start_time < datetime.time(hour=9, minute=0, second=0) or end_time > datetime.time(hour=20, minute=0, second=0):
            st.error('利用時間は9:00~20:00になります。')
        else:
            # 会議室予約
            url = 'https://sql_app-1-h3138769.deta.app/bookings'
            res = requests.post(
                url,
                data=json.dumps(data),
                headers={"Content-Type": "application/json; charset=utf-8"},
                verify=False
            )
            if res.status_code == 200:
                st.success('予約完了しました')            
            elif res.status_code == 404 and res.json()['detail'] == 'Already booked' :
                st.error('指定の時間にはすでに予約が入っています。')
