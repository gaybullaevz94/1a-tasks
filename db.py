import sqlite3
from datetime import datetime

DB_FILE = "tasks.db"

STATUS_NEW = "Новая"
STATUS_IN_PROGRESS = "В процессе"
STATUS_ON_REVIEW = "На проверке"
STATUS_DONE = "Готово"
STATUS_CANCELED = "Отменено"

ACTIVE_STATUSES = (STATUS_NEW, STATUS_IN_PROGRESS, STATUS_ON_REVIEW)


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


def get_conn():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(admin_id: int):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        telegram_id INTEGER PRIMARY KEY,
        full_name TEXT NOT NULL,
        department TEXT NOT NULL,
        role TEXT NOT NULL,
        is_active INTEGER NOT NULL DEFAULT 1
    )
    """)

    # миграция для старых баз: добавить is_active, если нет
    try:
        cur.execute("ALTER TABLE users ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
    except sqlite3.OperationalError:
        pass

    cur.execute("""
    CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        description TEXT NOT NULL,
        status TEXT NOT NULL,
        deadline TEXT NOT NULL,
        owner_telegram_id INTEGER NOT NULL,
        department TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER NOT NULL,
        author_telegram_id INTEGER NOT NULL,
        text TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER NOT NULL,
        uploader_telegram_id INTEGER NOT NULL,
        telegram_file_id TEXT NOT NULL,
        file_name TEXT,
        created_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER,
        actor_telegram_id INTEGER NOT NULL,
        action TEXT NOT NULL,
        details TEXT,
        created_at TEXT NOT NULL
    )
    """)

    cur.execute("SELECT telegram_id FROM users WHERE telegram_id=?", (admin_id,))
    if not cur.fetchone():
        cur.execute(
            "INSERT INTO users(telegram_id, full_name, department, role, is_active) VALUES (?,?,?,?,1)",
            (admin_id, "Админ", "Администрация", "admin"),
        )

    conn.commit()
    conn.close()


def audit(conn, task_id, actor_id, action, details=None):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO audit(task_id, actor_telegram_id, action, details, created_at) VALUES (?,?,?,?,?)",
        (task_id, actor_id, action, details, now_iso()),
    )
    conn.commit()
