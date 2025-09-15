import pymysql, sqlite3, json, logging, re
from dbutils.pooled_db import PooledDB   # 注意小写 dbutils
from datetime import date

# === 配置 ===
CONFIG = json.load(open("config.json", encoding="utf-8"))

# 日志（含 warning.log）
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("warning.log", "a", "utf-8")
    ]
)

# === MySQL 连接池（延迟创建） ===
mysql_pool = None

def init_mysql_pool():
    """尝试初始化连接池"""
    global mysql_pool
    if mysql_pool:
        return mysql_pool
    try:
        mysql_pool = PooledDB(
            creator=pymysql,
            maxconnections=5,
            mincached=1,
            maxcached=3,
            blocking=True,
            ping=1,
            host=CONFIG["mysql"]["host"],
            port=CONFIG["mysql"]["port"],
            user=CONFIG["mysql"]["user"],
            password=CONFIG["mysql"]["password"],
            database=CONFIG["mysql"]["database"],
            charset="utf8mb4",
            autocommit=True
        )
        logging.info("MySQL 连接池创建成功")
        return mysql_pool
    except Exception as e:
        logging.warning(f"MySQL 连接失败，将仅使用本地缓存: {e}")
        return None

def get_mysql_conn():
    """获取一个 MySQL 连接"""
    pool = init_mysql_pool()
    if pool:
        try:
            return pool.connection()
        except Exception as e:
            logging.warning(f"MySQL 获取连接失败: {e}")
    return None

# === SQLite 本地缓存 ===
def get_sqlite_conn():
    return sqlite3.connect(CONFIG["sqlite"], timeout=30)

def ensure_table(sqlite_conn, table_name, mysql_conn):
    """确保本地 SQLite 有和远程一致的表结构（只在本地不存在时创建）"""
    cur = sqlite_conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    exists = cur.fetchone()
    if exists:
        return  # 已存在，不需要重新建表

    if not mysql_conn:
        logging.warning(f"无法创建表 {table_name}（MySQL 不可用）")
        return

    with mysql_conn.cursor() as cur:
        cur.execute(f"SHOW CREATE TABLE {table_name}")
        _, create_sql = cur.fetchone()

        # 去掉 MySQL 专有语法
        for bad in ["ENGINE=", "AUTO_INCREMENT", "CHARSET=", "ROW_FORMAT="]:
            if bad in create_sql:
                create_sql = create_sql.split(bad)[0]

        create_sql = create_sql.replace("InnoDB", "").strip()
        create_sql = re.sub(r"COMMENT\s+'[^']*'", "", create_sql, flags=re.IGNORECASE)
        create_sql = re.sub(r"USING\s+BTREE", "", create_sql, flags=re.IGNORECASE)
        create_sql = create_sql.replace("`", "")  # 去掉反引号

        try:
            sqlite_conn.execute(create_sql)
            sqlite_conn.commit()
            logging.info(f"本地新建表 {table_name}")
        except Exception as e:
            logging.error(f"创建表 {table_name} 失败: {e}\nSQL: {create_sql}")

def sync_table(table_name, last_time):
    """同步单个表的数据，并在同步时做温度预警（只预警当天数据）"""
    mysql_conn = get_mysql_conn()
    if not mysql_conn:
        logging.info(f"跳过表 {table_name} 的同步（MySQL 不可用）")
        return 0

    sqlite_conn = get_sqlite_conn()
    ensure_table(sqlite_conn, table_name, mysql_conn)

    with mysql_conn.cursor() as cur:
        cur.execute(
            f"SELECT * FROM {table_name} WHERE FD_LAST_TM > %s ORDER BY FD_LAST_TM",
            (last_time,)
        )
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]

        if rows:
            # 插入 SQLite（避免重复 ID 报错）
            placeholders = ",".join(["?"] * len(rows[0]))
            sqlite_conn.executemany(
                f"INSERT OR IGNORE INTO {table_name} VALUES ({placeholders})", rows
            )
            sqlite_conn.commit()

            # 温度预警（只处理当天数据）
            if {"FD_INFO_SN", "FD_TEMPERATURE", "FD_LAST_TM"}.issubset(col_names):
                sn_idx = col_names.index("FD_INFO_SN")
                temp_idx = col_names.index("FD_TEMPERATURE")
                time_idx = col_names.index("FD_LAST_TM")

                today_str = str(date.today())  # 比如 "2025-09-15"

                for r in rows:
                    sn = r[sn_idx]
                    temp = r[temp_idx]
                    tm = r[time_idx]

                    if temp is not None and temp >= CONFIG["temperature_threshold"]:
                        tm_date = str(tm).split(" ")[0]
                        if tm_date == today_str:
                            logging.warning(
                                f"⚠️ 预警: 表={table_name}, SN={sn}, 温度={temp}, 时间={tm}"
                            )

    mysql_conn.close()
    sqlite_conn.close()
    return len(rows)

def get_all_mo_tables():
    """查询远程数据库所有 tb_tt_tboard_mo* 表"""
    conn = get_mysql_conn()
    if not conn:
        logging.info("MySQL 不可用，返回空表列表")
        return []
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables "
                "WHERE table_schema=%s AND table_name LIKE 'tb_tt_tboard_mo%%'",
                (CONFIG["mysql"]["database"],))
    tables = [row[0] for row in cur.fetchall()]
    conn.close()
    return tables
