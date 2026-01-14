from flask import Flask, render_template
import os

app = Flask(__name__)

# 你可以改成你的網域或 IP；也可以留空，前端用相對網址組合
ANALYSIS_PORT = 5001   # weather8.1.py
REALTIME_PORT = 5003   # CWB8.1.py

@app.route("/")
def home():
    # 用 JS 取得目前 host，再組合到不同 port，避免你換 IP 還要改字串
    return render_template("wmindex_home_v1.html",
                           analysis_port=ANALYSIS_PORT,
                           realtime_port=REALTIME_PORT)

if __name__ == "__main__":
    # 首頁用 5000
    app.run(host="0.0.0.0", port=5000, debug=True)



