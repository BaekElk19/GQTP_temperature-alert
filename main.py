import json, logging
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
from db_sync import sync_table, get_all_mo_tables
from webapp import app

# 日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

CONFIG = json.load(open("config.json", encoding="utf-8"))
last_sync_time = "2000-01-01 00:00:00"  # 初始值


def sync_job():
    """定时同步任务"""
    global last_sync_time
    try:
        tables = get_all_mo_tables()
        if not tables:
            logging.info("没有获取到远程 MO 表，跳过同步（可能是离线状态）")
            return

        logging.info(f"发现 {len(tables)} 个 MO 表需要同步")
        for table in tables:
            count = sync_table(table, last_sync_time)
            if count > 0:
                logging.info(f"表 {table} 同步 {count} 条新记录")

        # 更新时间戳
        last_sync_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    except Exception as e:
        logging.error(f"同步任务失败: {e}")


if __name__ == "__main__":
    # 定时任务（防止任务重叠）
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        sync_job,
        "interval",
        seconds=CONFIG["fetch_interval"],
        max_instances=1,
        coalesce=True
    )
    scheduler.start()

    logging.info("同步任务已启动，Web 服务运行中：http://127.0.0.1:5000")
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
