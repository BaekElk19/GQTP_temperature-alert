import sqlite3
import json

# 读取配置
CONFIG = json.load(open("config.json", encoding="utf-8"))
DB_PATH = CONFIG["sqlite"]

def create_indexes():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 找出所有 tb_tt_tboard_mo* 表
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'tb_tt_tboard_mo%'")
    tables = [r[0] for r in cur.fetchall()]

    for table in tables:
        try:
            # 索引1：设备号+时间
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_sn_time_{table} ON {table}(FD_INFO_SN, FD_LAST_TM)")
            # 索引2：温度
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_temp_{table} ON {table}(FD_TEMPERATURE)")
            print(f"✅ {table} 已创建索引")
        except Exception as e:
            print(f"⚠️ {table} 创建索引失败: {e}")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_indexes()
    print("🎉 所有索引创建完成")
