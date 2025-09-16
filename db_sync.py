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

def ensure_index(sqlite_conn, table_name):
    """为表创建必要索引，加快查询"""
    try:
        cur = sqlite_conn.cursor()
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_sn_time_{table_name} ON {table_name}(FD_INFO_SN, FD_LAST_TM)")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_temp_{table_name} ON {table_name}(FD_TEMPERATURE)")
        sqlite_conn.commit()
    except Exception as e:
        logging.warning(f"创建索引失败 {table_name}: {e}")

def ensure_table(sqlite_conn, table_name, mysql_conn):
    """确保本地 SQLite 有和远程一致的表结构（只在本地不存在时创建）"""
    cur = sqlite_conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    exists = cur.fetchone()
    if exists:
        # 确保已有表也有索引
        ensure_index(sqlite_conn, table_name)
        return

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
        create_sql = re.sub(r"COLLATE\s+\w+", "", create_sql, flags=re.IGNORECASE)

        try:
            sqlite_conn.execute(create_sql)
            sqlite_conn.commit()
            logging.info(f"本地新建表 {table_name}")
            # 新建表后立刻加索引
            ensure_index(sqlite_conn, table_name)
        except Exception as e:
            logging.error(f"创建表 {table_name} 失败: {e}\nSQL: {create_sql}")

def update_summary(sqlite_conn, table_name):
    """更新订单汇总信息到 mo_summary 表"""
    sqlite_conn.execute("""
        CREATE TABLE IF NOT EXISTS mo_summary (
            mo_name TEXT PRIMARY KEY,
            device_count INTEGER,
            last_time TEXT,
            warn_count INTEGER
        )
    """)
    sqlite_conn.execute(f"""
        INSERT OR REPLACE INTO mo_summary(mo_name, device_count, last_time, warn_count)
        VALUES (
            ?,
            (SELECT COUNT(DISTINCT FD_INFO_SN) FROM {table_name}),
            (SELECT MAX(FD_LAST_TM) FROM {table_name}),
            (SELECT COUNT(DISTINCT FD_INFO_SN) 
               FROM {table_name} 
              WHERE FD_TEMPERATURE >= ?)
        )
    """, (table_name, CONFIG["temperature_threshold"]))
    sqlite_conn.commit()

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
            placeholders = ",".join(["?"] * len(rows[0]))
            sqlite_conn.executemany(
                f"INSERT OR IGNORE INTO {table_name} VALUES ({placeholders})", rows
            )
            sqlite_conn.commit()

            # 确保插入数据后索引存在
            ensure_index(sqlite_conn, table_name)

            # 更新汇总表
            update_summary(sqlite_conn, table_name)

            # 温度预警（只处理当天数据）
            if {"FD_INFO_SN", "FD_TEMPERATURE", "FD_LAST_TM"}.issubset(col_names):
                sn_idx = col_names.index("FD_INFO_SN")
                temp_idx = col_names.index("FD_TEMPERATURE")
                time_idx = col_names.index("FD_LAST_TM")

                today_str = str(date.today())

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
