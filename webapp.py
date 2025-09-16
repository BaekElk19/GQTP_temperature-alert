import os
import sqlite3
from flask import Flask, render_template
from datetime import datetime
import json

app = Flask(__name__)

# 配置
CONFIG = json.load(open("config.json", encoding="utf-8"))
DB_PATH = CONFIG["sqlite"]

# 全局状态记录
system_status = {
    "online": False,           # 是否连接过远程 MySQL
    "last_sync": None,         # 最近同步时间
}


def query(sql, params=()):
    """执行查询，返回结果"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return rows


def ensure_index(table):
    """为表创建必要索引，加快查询速度"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_sn_time_{table} ON {table}(FD_INFO_SN, FD_LAST_TM)")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_temp_{table} ON {table}(FD_TEMPERATURE)")
        conn.commit()
    except Exception as e:
        print(f"⚠️ 创建索引失败 {table}: {e}")
    conn.close()


@app.route("/")
def index():
    rows = query("SELECT mo_name, device_count, last_time, warn_count FROM mo_summary")
    result = []
    for r in rows:
        result.append({
            "name": r[0],
            "count": r[1],
            "last_time": r[2],
            "warn_count": r[3]
        })
    return render_template("index.html", mos=result, threshold=CONFIG["temperature_threshold"])



@app.route("/<mo>")
def sn_list(mo):
    ensure_index(mo)
    # 子查询 + JOIN，避免重复扫描
    rows = query(f"""
        SELECT s.FD_INFO_SN, s.FD_LAST_TM, t.FD_TEMPERATURE
        FROM (
            SELECT FD_INFO_SN, MAX(FD_LAST_TM) AS FD_LAST_TM
            FROM {mo}
            GROUP BY FD_INFO_SN
        ) s
        LEFT JOIN {mo} t
        ON s.FD_INFO_SN = t.FD_INFO_SN AND s.FD_LAST_TM = t.FD_LAST_TM
    """)
    sns = []
    for r in rows:
        if r[0] is not None:
            sns.append({
                "sn": r[0],
                "last_time": r[1],
                "temp": r[2],
                "warn": (r[2] is not None and r[2] >= CONFIG["temperature_threshold"])
            })
    return render_template("sn_list.html", mo=mo, sns=sns, threshold=CONFIG["temperature_threshold"])


@app.route("/<mo>/<sn>")
def sn_curve(mo, sn):
    ensure_index(mo)
    # 只取最近 2000 条，避免浏览器卡死
    rows = query(
        f"""
        SELECT FD_LAST_TM, FD_TEMPERATURE
        FROM {mo}
        WHERE FD_INFO_SN=?
        ORDER BY FD_LAST_TM DESC
        LIMIT 2000
        """,
        (sn,)
    )
    rows = rows[::-1]  # 翻转为升序

    times = [r[0] for r in rows]
    temps = [r[1] for r in rows]

    avg_temp = sum(temps) / len(temps) if temps else 0
    max_temp = max(temps) if temps else 0
    warn_count = sum(1 for t in temps if t is not None and t >= CONFIG["temperature_threshold"])

    return render_template(
        "sn_curve.html",
        mo=mo,
        sn=sn,
        times=times,
        temps=temps,
        avg_temp=round(avg_temp, 1),
        max_temp=max_temp,
        warn_count=warn_count,
        threshold=CONFIG["temperature_threshold"]
    )


@app.route("/favicon.ico")
def favicon():
    return "", 204


@app.route("/status")
def status():
    tables = query("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'tb_tt_tboard_mo%'")
    table_count = len(tables)
    return {
        "online": system_status["online"],
        "last_sync": system_status["last_sync"],
        "local_tables": table_count
    }


@app.route("/<mo>/dist")
def sn_temp_distribution(mo):
    ensure_index(mo)
    # 每台 SN 的最高温度
    rows = query(f"""
        SELECT FD_INFO_SN, MAX(FD_TEMPERATURE)
        FROM {mo}
        GROUP BY FD_INFO_SN
    """)
    sns = [r[0] for r in rows if r[0] is not None]
    max_temps = {r[0]: r[1] for r in rows if r[0] is not None and r[1] is not None}

    # 每台 SN 的前 20 分钟升温速度（限制范围，避免全量扫描）
    rise_rates = {}
    for sn in sns:
        sub = query(f"""
            SELECT FD_LAST_TM, FD_TEMPERATURE
            FROM {mo}
            WHERE FD_INFO_SN=? 
              AND FD_LAST_TM <= DATETIME(
                  (SELECT MIN(FD_LAST_TM) FROM {mo} WHERE FD_INFO_SN=?), '+20 minutes'
              )
            ORDER BY FD_LAST_TM ASC
        """, (sn, sn))
        if len(sub) >= 2:
            try:
                t0 = datetime.fromisoformat(str(sub[0][0]))
                temp0 = sub[0][1]
                t_end, temp_end = None, None
                for t, temp in sub:
                    if not t or temp is None:
                        continue
                    try:
                        t_dt = datetime.fromisoformat(str(t))
                    except Exception:
                        t_dt = datetime.strptime(str(t), "%Y-%m-%d %H:%M:%S")
                    if (t_dt - t0).total_seconds() <= 20 * 60:
                        t_end, temp_end = t_dt, temp
                if t_end and temp_end and temp0 is not None:
                    minutes = (t_end - t0).total_seconds() / 60
                    if minutes > 0:
                        rise_rates[sn] = (temp_end - temp0) / minutes
            except Exception:
                continue

    scatter_x, scatter_y, scatter_labels = [], [], []
    for sn in sns:
        if sn in rise_rates and sn in max_temps:
            scatter_x.append(rise_rates[sn])
            scatter_y.append(max_temps[sn])
            scatter_labels.append(sn)

    return render_template(
        "sn_distribution.html",
        mo=mo,
        sns=sns,
        temps=list(max_temps.values()),
        scatter_x=scatter_x,
        scatter_y=scatter_y,
        scatter_labels=scatter_labels,
        threshold=CONFIG["temperature_threshold"]
    )
