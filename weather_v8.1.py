from flask import Flask, render_template, request, jsonify, redirect, url_for
import pandas as pd
import sqlite3
import os
import numpy as np

app = Flask(__name__)

DB_FILE = "weather_1021.db"
UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


# ---------------- 初始化資料庫 ----------------
def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute(
            """
        CREATE TABLE IF NOT EXISTS weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            obs_date TEXT,
            temperature REAL,
            humidity REAL,
            tmax REAL,
            tmin REAL
        )
        """
        )
        conn.commit()


init_db()


# ---------------- CSV 上傳處理（v5 模式） ----------------
def process_csv(filepath, filename):
    name_part = os.path.splitext(filename)[0]
    try:
        year = name_part.split("-")[1]
        month = name_part.split("-")[2]
    except IndexError:
        print(f"⚠️ 檔名格式錯誤：{filename}")
        return

    try:
        df = pd.read_csv(filepath, header=[0, 1])
    except Exception as e:
        print(f"❌ 無法讀取檔案 {filename}：{e}")
        return

    # 展平雙表頭欄位
    df.columns = ["_".join(col).strip() for col in df.columns.values]

    # 模糊找欄位（兼容 ObsTime/氣溫/相對溼度/最高氣溫/最低氣溫）
    def find_col(keywords):
        for col in df.columns:
            for k in keywords:
                if k in col:
                    return col
        raise KeyError(f"找不到欄位: {keywords}")

    col_obs = find_col(["觀測時間", "ObsTime"])
    col_temp = find_col(["氣溫", "Temperature"])
    col_rh = find_col(["相對溼度", "RH"])
    col_tmax = find_col(["最高氣溫", "T Max"])
    col_tmin = find_col(["最低氣溫", "T Min"])

    df_new = df[[col_obs, col_temp, col_rh, col_tmax, col_tmin]].copy()
    df_new.columns = ["obs_time", "temperature", "humidity", "tmax", "tmin"]

    # 建立日期欄（取日、補零）
    def build_date(day_val):
        try:
            d = int(str(day_val).strip().split()[0])
            return f"{year}-{month}-{d:02d}"
        except Exception:
            return f"{year}-{month}-01"

    df_new["obs_date"] = df_new["obs_time"].apply(build_date)
    df_new = df_new[["obs_date", "temperature", "humidity", "tmax", "tmin"]].dropna()

    with sqlite3.connect(DB_FILE) as conn:
        existing = pd.read_sql_query("SELECT obs_date FROM weather", conn)
        exist_set = set(existing["obs_date"].tolist())
        new_data = df_new[~df_new["obs_date"].isin(exist_set)]
        new_data.to_sql("weather", conn, if_exists="append", index=False)

    print(f"✅ 匯入完成 {filename}（新增 {len(df_new)} 筆）")


# ---------------- 主頁 ----------------
@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        if "files" in request.files:
            files = request.files.getlist("files")
            for f in files:
                if f and f.filename.endswith(".csv"):
                    path = os.path.join(UPLOAD_FOLDER, f.filename)
                    f.save(path)
                    process_csv(path, f.filename)

    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT obs_date, temperature, humidity, tmax, tmin FROM weather ORDER BY obs_date ASC")
        data = c.fetchall()

    return render_template("index.html_v8.1", data=data)


# ---------------- 清除資料 ----------------
@app.route("/clear", methods=["POST"])
def clear_data():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("DELETE FROM weather")
        conn.commit()
    print("🧹 資料已清除")
    return redirect(url_for("index"))


# ---------------- 三段積溫比較 ----------------
@app.route("/gdd_compare", methods=["POST"])
def gdd_compare():
    data = request.get_json()
    r1, r2, r3 = data["range1"], data["range2"], data["range3"]

    with sqlite3.connect(DB_FILE) as conn:
        df = pd.read_sql_query("SELECT * FROM weather", conn)
    df["obs_date"] = pd.to_datetime(df["obs_date"])

    results = []
    for Tb in range(0, 21):
        def calc_gdd(start, end):
            mask = (df["obs_date"] >= start) & (df["obs_date"] <= end)
            sub = df.loc[mask]
            gdd = ((sub["tmax"] + sub["tmin"]) / 2 - Tb).clip(lower=0).sum()
            return round(gdd, 2)

        g1 = calc_gdd(r1[0], r1[1])
        g2 = calc_gdd(r2[0], r2[1])
        g3 = calc_gdd(r3[0], r3[1])
        std = np.std([g1, g2, g3])
        results.append({
            "Tb": Tb,
            "GDD1": g1,
            "GDD2": g2,
            "GDD3": g3,
            "std": round(std, 2)
        })

    best = min(results, key=lambda x: x["std"])
    return jsonify({"table": results, "best": best})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
