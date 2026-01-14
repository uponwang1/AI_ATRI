from flask import Flask, render_template, jsonify
import sqlite3
import requests
import os
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

app = Flask(__name__)

# =============================
# 固定路徑（避免你跑在不同資料夾時 DB 亂長）
# =============================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "CWB.db")

# 你原本的 API KEY（建議之後改用環境變數，但先確保可用）
API_KEY = os.getenv("CWA_API_KEY", "CWA-28C75F2E-322C-4DA2-8558-BD212BBBDAAC")
STATION = os.getenv("CWA_STATION_ID", "C0D680")  # 香山濕地

# 記錄最後更新時間（給頁面顯示用）
last_update_time = None

# CWA API
CWA_URL = "https://opendata.cwa.gov.tw/api/v1/rest/datastore/O-A0001-001"


# ---------------------- 建立資料庫 ----------------------
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS weather (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                obs_date TEXT UNIQUE,
                temperature REAL,
                humidity REAL
            )
            """
        )
        conn.commit()


# ---------------------- 解析 WeatherElement（兼容 dict / list） ----------------------
def extract_weather_element(station_obj: dict, key: str):
    """
    O-A0001-001 的 WeatherElement 有時是 dict、有時是 list。
    這裡做最大相容。
    """
    we = station_obj.get("WeatherElement")

    # 情況 1：dict（你原本寫法）
    if isinstance(we, dict):
        return we.get(key)

    # 情況 2：list（常見格式：[{ElementName, ElementValue}, ...]）
    if isinstance(we, list):
        for item in we:
            if item.get("ElementName") == key:
                val = item.get("ElementValue")
                # 可能是字串，轉 float
                try:
                    return float(val)
                except Exception:
                    return val

    return None


# ---------------------- 抓 API ----------------------
def fetch_station_data():
    params = {
        "Authorization": API_KEY,
        "StationId": STATION,
        "limit": 1000
    }
    resp = requests.get(CWA_URL, params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()


# ---------------------- 寫入資料庫 ----------------------
def insert_to_db(payload: dict) -> int:
    """
    把 API 回來的資料寫入 SQLite（INSERT OR IGNORE 避免重複）
    回傳：本次成功新增筆數
    """
    global last_update_time

    stations = payload.get("records", {}).get("Station", [])
    if not isinstance(stations, list):
        return 0

    rows = []

    for st in stations:
        if st.get("StationId") != STATION:
            continue

        obs_time = st.get("ObsTime", {}).get("DateTime")
        temp = extract_weather_element(st, "AirTemperature")
        rh = extract_weather_element(st, "RelativeHumidity")

        if not obs_time:
            continue

        # temp / rh 可能是字串
        try:
            temp = float(temp)
            rh = float(rh)
        except Exception:
            continue

        # API 可能給 Z（UTC），轉成台灣時間顯示/存入（+08:00）
        try:
            dt_utc = datetime.fromisoformat(obs_time.replace("Z", "+00:00"))
        except Exception:
            continue

        dt_tw = dt_utc.astimezone(timezone(timedelta(hours=8)))
        obs_date = dt_tw.strftime("%Y-%m-%d %H:%M")

        rows.append((obs_date, temp, rh))

    if not rows:
        return 0

    inserted = 0
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        for obs_date, temp, rh in rows:
            c.execute(
                """
                INSERT OR IGNORE INTO weather (obs_date, temperature, humidity)
                VALUES (?, ?, ?)
                """,
                (obs_date, temp, rh)
            )
            if c.rowcount == 1:
                inserted += 1

        conn.commit()

    last_update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return inserted


# ---------------------- 一次抓取 + 寫入（供排程呼叫） ----------------------
def job_fetch_and_save():
    try:
        init_db()
        payload = fetch_station_data()
        inserted = insert_to_db(payload)
        print(f"✅ CWB8.1 更新完成：新增 {inserted} 筆（DB: {DB_PATH}）")
    except Exception as e:
        print(f"❌ CWB8.1 更新失敗：{e}")


# ---------------------- 網頁：即時監控頁 ----------------------
@app.route("/")
def index():
    """
    讀取最近 7~10 天資料（前端也會再篩 7 天+斷線）
    """
    init_db()

    labels, temps, hums = [], [], []
    err_msg = None

    try:
        with sqlite3.connect(DB_PATH) as conn:
            # 取近 10 天，避免資料太大
            # obs_date 格式 YYYY-MM-DD HH:MM → 字串可直接比大小
            cutoff = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d %H:%M")
            cur = conn.cursor()
            cur.execute(
                """
                SELECT obs_date, temperature, humidity
                FROM weather
                WHERE obs_date >= ?
                ORDER BY obs_date ASC
                """,
                (cutoff,)
            )
            rows = cur.fetchall()

        labels = [r[0] for r in rows]
        temps = [r[1] for r in rows]
        hums = [r[2] for r in rows]

    except Exception as e:
        err_msg = f"讀取資料庫失敗：{e}"

    # 你既有的模板 indexCWB.html_v8.1 只要能吃 labels/temps/hums 就能跑
    # 我多塞 err_msg / last_update_time，不用也不會壞
    return render_template(
        "indexCWB.html_v8.1",
        labels=labels,
        temps=temps,
        hums=hums,
        err_msg=err_msg,
        last_update_time=last_update_time
    )


# ---------------------- 手動更新（debug/緊急用） ----------------------
@app.route("/fetch_now")
def fetch_now():
    job_fetch_and_save()
    return jsonify({"ok": True, "last_update_time": last_update_time})


# ---------------------- 啟動排程 ----------------------
def start_scheduler():
    scheduler = BackgroundScheduler(timezone="Asia/Taipei")

    # 你 v8.1 的習慣：每小時第 10 分抓一次（可自行改）
    scheduler.add_job(
        job_fetch_and_save,
        CronTrigger(minute=10)
    )

    scheduler.start()
    return scheduler


if __name__ == "__main__":
    init_db()

    # 啟動時先抓一次（讓 DB 立刻有資料）
    job_fetch_and_save()

    # 啟動排程
    sched = start_scheduler()

    # use_reloader=False 很重要：避免 scheduler 被跑兩次
    app.run(host="0.0.0.0", port=5003, debug=True, use_reloader=False)



