import asyncio
import logging
from datetime import datetime, timedelta, time as dtime

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import ADMIN_TELEGRAM_ID, BOT_TOKEN
import db

logging.basicConfig(level=logging.INFO)

WAIT = {}  # tg_id -> state dict


def is_admin(tg_id: int) -> bool:
    return tg_id == ADMIN_TELEGRAM_ID


def get_user(conn, tg_id: int):
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE telegram_id=?", (tg_id,))
    return cur.fetchone()


def is_employee_active(conn, tg_id: int) -> bool:
    u = get_user(conn, tg_id)
    if not u:
        return False
    if u["role"] != "employee":
        return True
    return int(u["is_active"]) == 1


async def notify_admin(bot: Bot, text: str):
    try:
        await bot.send_message(ADMIN_TELEGRAM_ID, text, disable_notification=False)
    except Exception:
        pass


# ---------- Keyboards ----------

def kb_admin_main():
    b = InlineKeyboardBuilder()
    b.button(text="➕ Создать задачу", callback_data="ad:newtask")
    b.button(text="📌 Все активные", callback_data="ad:active")
    b.button(text="🟨 На проверке", callback_data="ad:review")
    b.button(text="✅ Завершенные", callback_data="ad:done")
    b.button(text="🟥 Просроченные", callback_data="ad:overdue")
    b.button(text="👥 Пользователи", callback_data="ad:users")
    b.adjust(2)
    return b.as_markup()


def kb_employee_main():
    b = InlineKeyboardBuilder()
    b.button(text="📌 Мои задачи", callback_data="em:my")
    b.button(text="🟨 Мои на проверке", callback_data="em:myreview")
    b.button(text="✅ Завершенные", callback_data="em:done")
    b.adjust(1)
    return b.as_markup()


def kb_employee_task(task_id: int, status: str):
    b = InlineKeyboardBuilder()
    if status == db.STATUS_NEW:
        b.button(text="▶️ В процессе", callback_data=f"t:{task_id}:inprog")
    if status == db.STATUS_IN_PROGRESS:
        b.button(text="🟨 На проверке", callback_data=f"t:{task_id}:review")
    b.button(text="💬 Комментарий", callback_data=f"t:{task_id}:comment")
    b.button(text="📎 Файл", callback_data=f"t:{task_id}:file")
    b.adjust(2)
    return b.as_markup()


def kb_admin_task(task_id: int, status: str):
    b = InlineKeyboardBuilder()
    if status == db.STATUS_ON_REVIEW:
        b.button(text="✅ Принять (Готово)", callback_data=f"t:{task_id}:done")
        b.button(text="↩️ Вернуть (В процессе)", callback_data=f"t:{task_id}:back")
    b.button(text="🗓 Изменить срок", callback_data=f"t:{task_id}:chgdl")
    b.button(text="🗑 Отменить задачу", callback_data=f"t:{task_id}:cancel")
    b.adjust(2)
    return b.as_markup()


def kb_pick_employee(active_users):
    b = InlineKeyboardBuilder()
    for u in active_users:
        title = f"{u['full_name']} ({u['department']})"
        b.button(text=title, callback_data=f"ad:pick:{u['telegram_id']}")
    b.button(text="❌ Отмена", callback_data="ad:pickcancel")
    b.adjust(1)
    return b.as_markup()


def kb_users_list(employees):
    """
    Список сотрудников кнопками.
    employees: list[sqlite3.Row] columns: telegram_id, full_name, department, is_active
    """
    b = InlineKeyboardBuilder()
    for u in employees:
        icon = "🟢" if int(u["is_active"]) == 1 else "🔴"
        text = f"{icon} {u['full_name']} — {u['department']}"
        b.button(text=text, callback_data=f"ad:user:{u['telegram_id']}")
    b.button(text="⬅️ Назад в меню", callback_data="ad:back_main")
    b.adjust(1)
    return b.as_markup()


def kb_user_actions(user_row):
    """
    Карточка сотрудника: активировать/удалить(отключить)
    """
    b = InlineKeyboardBuilder()
    tg_id = user_row["telegram_id"]
    if int(user_row["is_active"]) == 1:
        b.button(text="🗑 Удалить сотрудника (отключить)", callback_data=f"ad:deact:{tg_id}")
    else:
        b.button(text="✅ Активировать сотрудника", callback_data=f"ad:act:{tg_id}")
    b.button(text="⬅️ К списку сотрудников", callback_data="ad:users")
    b.adjust(1)
    return b.as_markup()


# ---------- Dates / formatting ----------

def deadline_today():
    d = datetime.now().date()
    return datetime.combine(d, dtime(23, 59)).isoformat(timespec="seconds")


def deadline_end_of_week():
    d = datetime.now().date()
    days_ahead = 6 - d.weekday()
    target = d + timedelta(days=days_ahead)
    return datetime.combine(target, dtime(23, 59)).isoformat(timespec="seconds")


def format_task(row) -> str:
    return (
        f"Задача #{row['id']}\n"
        f"Отдел: {row['department']}\n"
        f"Статус: {row['status']}\n"
        f"Срок: {row['deadline']}\n"
        f"Название: {row['title']}\n"
        f"Описание: {row['description']}"
    )


async def push_task_assigned(bot: Bot, target_id: int, task_row):
    """
    Push = новое сообщение от бота (disable_notification=False).
    """
    await bot.send_message(
        target_id,
        f"🔔 НОВАЯ ЗАДАЧА #{task_row['id']}\n"
        f"Срок: {task_row['deadline']}\n"
        f"Название: {task_row['title']}",
        disable_notification=False
    )
    await bot.send_message(
        target_id,
        format_task(task_row),
        reply_markup=kb_employee_task(task_row["id"], task_row["status"]),
        disable_notification=False
    )


# ---------- Daily report to admin ----------

async def daily_report_loop(bot: Bot):
    last_date = None
    while True:
        now = datetime.now()
        if now.hour == 9 and now.minute == 0:
            if last_date != now.date():
                conn = db.get_conn()
                cur = conn.cursor()

                cur.execute(
                    "SELECT COUNT(*) c FROM tasks WHERE status IN (?,?,?) AND deadline < ?",
                    (*db.ACTIVE_STATUSES, db.now_iso()),
                )
                overdue = cur.fetchone()["c"]

                cur.execute("SELECT COUNT(*) c FROM tasks WHERE status=?", (db.STATUS_ON_REVIEW,))
                review = cur.fetchone()["c"]

                cur.execute("SELECT COUNT(*) c FROM tasks WHERE status IN (?,?,?)", (*db.ACTIVE_STATUSES,))
                active = cur.fetchone()["c"]

                conn.close()

                text = (
                    "Ежедневный отчет 09:00\n"
                    f"Просроченные: {overdue}\n"
                    f"На проверке: {review}\n"
                    f"Активные: {active}"
                )
                await bot.send_message(ADMIN_TELEGRAM_ID, text, disable_notification=False)
                last_date = now.date()
        await asyncio.sleep(20)


# ================== MAIN ==================

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN пустой. Проверь файл .env")
    if ADMIN_TELEGRAM_ID == 0:
        raise RuntimeError("ADMIN_TELEGRAM_ID пустой. Проверь файл .env")

    db.init_db(ADMIN_TELEGRAM_ID)

    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()

    print("Бот запущен. PowerShell не закрывать.")

    # ---------- /start ----------

    @dp.message(Command("start"))
    async def start(message: Message):
        if is_admin(message.from_user.id):
            await message.answer("Админ-режим.", reply_markup=kb_admin_main())
            return

        conn = db.get_conn()
        u = get_user(conn, message.from_user.id)
        conn.close()

        if not u:
            await message.answer(
                "Ты не добавлен в систему.\n"
                "Отправь админу свой Telegram ID:\n"
                f"{message.from_user.id}\n"
                "После добавления напиши /start."
            )
            return

        if u["role"] == "employee" and int(u["is_active"]) == 0:
            await message.answer("Твой доступ отключен админом.")
            return

        await message.answer(
            f"Режим сотрудника: {u['full_name']} ({u['department']})",
            reply_markup=kb_employee_main(),
        )

    # ---------- Users management (commands) ----------

    @dp.message(Command("add_user"))
    async def add_user(message: Message):
        if not is_admin(message.from_user.id):
            return
        payload = message.text[len("/add_user"):].strip()
        try:
            tg_id_s, fio, dept = [x.strip() for x in payload.split("|")]
            tg_id = int(tg_id_s)
        except Exception:
            await message.answer("Формат: /add_user 111|ФИО|Отдел")
            return
        if dept not in ("Снабжение", "Финансы", "Бухгалтерия"):
            await message.answer("Отдел: Снабжение / Финансы / Бухгалтерия")
            return

        conn = db.get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO users(telegram_id, full_name, department, role, is_active)
            VALUES(?,?,?,?,1)
            ON CONFLICT(telegram_id) DO UPDATE SET
                full_name=excluded.full_name,
                department=excluded.department,
                role='employee',
                is_active=1
            """,
            (tg_id, fio, dept, "employee"),
        )
        conn.commit()
        db.audit(conn, None, message.from_user.id, "ADD_USER", f"{tg_id}|{fio}|{dept}")
        conn.close()

        await message.answer(f"Ок. Добавлен/обновлён: {fio} ({dept})")
        await notify_admin(bot, f"✅ УСПЕШНО: сотрудник добавлен/обновлён — {fio} ({dept}) id={tg_id}")
        try:
            await bot.send_message(tg_id, "Тебя добавили в систему. Напиши /start.", disable_notification=False)
        except Exception:
            pass

    # ---------- Admin menu navigation ----------

    @dp.callback_query(F.data == "ad:back_main")
    async def ad_back_main(call: CallbackQuery):
        if not is_admin(call.from_user.id):
            return await call.answer()
        await call.message.answer("Админ-меню:", reply_markup=kb_admin_main())
        await call.answer()

    # ---------- Admin: Users (LIST as buttons) ----------

    @dp.callback_query(F.data == "ad:users")
    async def ad_users(call: CallbackQuery):
        if not is_admin(call.from_user.id):
            return await call.answer()

        conn = db.get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT telegram_id, full_name, department, is_active "
            "FROM users WHERE role='employee' "
            "ORDER BY is_active DESC, department, full_name"
        )
        employees = cur.fetchall()
        conn.close()

        if not employees:
            await call.message.answer("Сотрудников нет. Добавь через /add_user.")
            return await call.answer()

        await call.message.answer("Сотрудники (нажми на человека):", reply_markup=kb_users_list(employees))
        await call.answer()

    # ---------- Admin: User card ----------

    @dp.callback_query(F.data.startswith("ad:user:"))
    async def ad_user_card(call: CallbackQuery):
        if not is_admin(call.from_user.id):
            return await call.answer()

        tg_id = int(call.data.split(":")[2])
        conn = db.get_conn()
        u = get_user(conn, tg_id)

        if not u or u["role"] != "employee":
            conn.close()
            await call.message.answer("Сотрудник не найден.")
            return await call.answer()

        cur = conn.cursor()
        # статистика по задачам
        cur.execute("SELECT COUNT(*) c FROM tasks WHERE owner_telegram_id=?", (tg_id,))
        total = cur.fetchone()["c"]
        cur.execute("SELECT COUNT(*) c FROM tasks WHERE owner_telegram_id=? AND status IN (?,?,?)",
                    (tg_id, *db.ACTIVE_STATUSES))
        active = cur.fetchone()["c"]
        cur.execute("SELECT COUNT(*) c FROM tasks WHERE owner_telegram_id=? AND status=?",
                    (tg_id, db.STATUS_ON_REVIEW))
        review = cur.fetchone()["c"]
        cur.execute("SELECT COUNT(*) c FROM tasks WHERE owner_telegram_id=? AND status=?",
                    (tg_id, db.STATUS_DONE))
        done = cur.fetchone()["c"]
        conn.close()

        status = "АКТИВЕН" if int(u["is_active"]) == 1 else "ОТКЛЮЧЕН"
        text = (
            f"Сотрудник:\n"
            f"ФИО: {u['full_name']}\n"
            f"Отдел: {u['department']}\n"
            f"Telegram ID: {u['telegram_id']}\n"
            f"Статус: {status}\n\n"
            f"Задачи:\n"
            f"Всего: {total}\n"
            f"Активные: {active}\n"
            f"На проверке: {review}\n"
            f"Завершенные: {done}\n\n"
            f"Удаление = отключение доступа. История сохраняется."
        )
        await call.message.answer(text, reply_markup=kb_user_actions(u))
        await call.answer()

    # ---------- Admin: Deactivate (delete) / Activate from buttons ----------

    @dp.callback_query(F.data.startswith("ad:deact:"))
    async def ad_deactivate_btn(call: CallbackQuery):
        if not is_admin(call.from_user.id):
            return await call.answer()
        tg_id = int(call.data.split(":")[2])
        if tg_id == ADMIN_TELEGRAM_ID:
            await call.message.answer("Нельзя отключить админа.")
            return await call.answer()

        conn = db.get_conn()
        u = get_user(conn, tg_id)
        if not u or u["role"] != "employee":
            conn.close()
            await call.message.answer("Сотрудник не найден.")
            return await call.answer()

        cur = conn.cursor()
        cur.execute("UPDATE users SET is_active=0 WHERE telegram_id=?", (tg_id,))
        conn.commit()
        db.audit(conn, None, call.from_user.id, "DEACTIVATE_USER", f"{tg_id}|{u['full_name']}|{u['department']}")
        conn.close()

        WAIT.pop(tg_id, None)

        await call.message.answer(f"✅ УСПЕШНО: сотрудник отключен (удален из доступа).\n{u['full_name']} — {u['department']}")
        await notify_admin(call.bot, f"✅ УСПЕШНО: сотрудник ОТКЛЮЧЕН — {u['full_name']} id={tg_id}")
        try:
            await call.bot.send_message(tg_id, "Твой доступ отключен админом.", disable_notification=False)
        except Exception:
            pass

        await call.answer()

    @dp.callback_query(F.data.startswith("ad:act:"))
    async def ad_activate_btn(call: CallbackQuery):
        if not is_admin(call.from_user.id):
            return await call.answer()
        tg_id = int(call.data.split(":")[2])

        conn = db.get_conn()
        u = get_user(conn, tg_id)
        if not u or u["role"] != "employee":
            conn.close()
            await call.message.answer("Сотрудник не найден.")
            return await call.answer()

        cur = conn.cursor()
        cur.execute("UPDATE users SET is_active=1 WHERE telegram_id=?", (tg_id,))
        conn.commit()
        db.audit(conn, None, call.from_user.id, "ACTIVATE_USER", f"{tg_id}|{u['full_name']}|{u['department']}")
        conn.close()

        await call.message.answer(f"✅ УСПЕШНО: сотрудник активирован.\n{u['full_name']} — {u['department']}")
        await notify_admin(call.bot, f"✅ УСПЕШНО: сотрудник ВКЛЮЧЕН — {u['full_name']} id={tg_id}")
        try:
            await call.bot.send_message(tg_id, "Твой доступ включен. Напиши /start.", disable_notification=False)
        except Exception:
            pass

        await call.answer()

    # ---------- Admin tasks sections ----------

    @dp.callback_query(F.data == "ad:active")
    async def ad_active(call: CallbackQuery):
        if not is_admin(call.from_user.id):
            return await call.answer()

        conn = db.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM tasks WHERE status IN (?,?,?) ORDER BY deadline ASC", (*db.ACTIVE_STATUSES,))
        rows = cur.fetchall()
        conn.close()

        if not rows:
            await call.message.answer("Активных задач нет.")
        else:
            for r in rows[:30]:
                await call.message.answer(format_task(r), reply_markup=kb_admin_task(r["id"], r["status"]))
        await call.answer()

    @dp.callback_query(F.data == "ad:review")
    async def ad_review(call: CallbackQuery):
        if not is_admin(call.from_user.id):
            return await call.answer()

        conn = db.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM tasks WHERE status=? ORDER BY deadline ASC", (db.STATUS_ON_REVIEW,))
        rows = cur.fetchall()
        conn.close()

        if not rows:
            await call.message.answer("Нет задач на проверке.")
        else:
            for r in rows[:30]:
                await call.message.answer(format_task(r), reply_markup=kb_admin_task(r["id"], r["status"]))
        await call.answer()

    @dp.callback_query(F.data == "ad:done")
    async def ad_done(call: CallbackQuery):
        if not is_admin(call.from_user.id):
            return await call.answer()

        conn = db.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM tasks WHERE status=? ORDER BY updated_at DESC", (db.STATUS_DONE,))
        rows = cur.fetchall()
        conn.close()

        if not rows:
            await call.message.answer("Завершенных нет.")
        else:
            for r in rows[:30]:
                await call.message.answer(format_task(r))
        await call.answer()

    @dp.callback_query(F.data == "ad:overdue")
    async def ad_overdue(call: CallbackQuery):
        if not is_admin(call.from_user.id):
            return await call.answer()

        conn = db.get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM tasks WHERE status IN (?,?,?) AND deadline < ? ORDER BY deadline ASC",
            (*db.ACTIVE_STATUSES, db.now_iso()),
        )
        rows = cur.fetchall()
        conn.close()

        if not rows:
            await call.message.answer("Просроченных нет.")
        else:
            for r in rows[:30]:
                await call.message.answer(format_task(r), reply_markup=kb_admin_task(r["id"], r["status"]))
        await call.answer()

    # ---------- Create task: pick employee list ----------

    @dp.callback_query(F.data == "ad:newtask")
    async def ad_newtask(call: CallbackQuery):
        if not is_admin(call.from_user.id):
            return await call.answer()

        conn = db.get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT telegram_id, full_name, department FROM users "
            "WHERE role='employee' AND is_active=1 ORDER BY department, full_name"
        )
        users = cur.fetchall()
        conn.close()

        if not users:
            await call.message.answer("Нет активных сотрудников. Добавь через /add_user.")
            return await call.answer()

        WAIT[call.from_user.id] = {"step": "pick_user"}
        await call.message.answer("Выбери сотрудника:", reply_markup=kb_pick_employee(users))
        await call.answer()

    @dp.callback_query(F.data == "ad:pickcancel")
    async def ad_pickcancel(call: CallbackQuery):
        if not is_admin(call.from_user.id):
            return await call.answer()
        WAIT.pop(call.from_user.id, None)
        await call.message.answer("Отменено.")
        await call.answer()

    @dp.callback_query(F.data.startswith("ad:pick:"))
    async def ad_pick(call: CallbackQuery):
        if not is_admin(call.from_user.id):
            return await call.answer()

        target_id = int(call.data.split(":")[2])

        conn = db.get_conn()
        u = get_user(conn, target_id)
        conn.close()

        if not u or u["role"] != "employee" or int(u["is_active"]) == 0:
            WAIT.pop(call.from_user.id, None)
            await call.message.answer("Сотрудник не найден/не активен.")
            return await call.answer()

        WAIT[call.from_user.id] = {"step": "title", "target_id": target_id, "dept": u["department"]}
        await call.message.answer(f"Выбран: {u['full_name']} ({u['department']})\nНазвание задачи:")
        await call.answer()

    # ---------- Employee lists ----------

    @dp.callback_query(F.data == "em:my")
    async def em_my(call: CallbackQuery):
        conn = db.get_conn()
        if not is_employee_active(conn, call.from_user.id):
            conn.close()
            await call.message.answer("Доступ отключен.")
            return await call.answer()
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM tasks WHERE owner_telegram_id=? AND status IN (?,?,?) ORDER BY deadline ASC",
            (call.from_user.id, *db.ACTIVE_STATUSES),
        )
        rows = cur.fetchall()
        conn.close()
        if not rows:
            await call.message.answer("Нет активных задач.")
        else:
            for r in rows[:30]:
                await call.message.answer(format_task(r), reply_markup=kb_employee_task(r["id"], r["status"]))
        await call.answer()

    @dp.callback_query(F.data == "em:myreview")
    async def em_myreview(call: CallbackQuery):
        conn = db.get_conn()
        if not is_employee_active(conn, call.from_user.id):
            conn.close()
            await call.message.answer("Доступ отключен.")
            return await call.answer()
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM tasks WHERE owner_telegram_id=? AND status=? ORDER BY deadline ASC",
            (call.from_user.id, db.STATUS_ON_REVIEW),
        )
        rows = cur.fetchall()
        conn.close()
        if not rows:
            await call.message.answer("Нет задач на проверке.")
        else:
            for r in rows[:30]:
                await call.message.answer(format_task(r), reply_markup=kb_employee_task(r["id"], r["status"]))
        await call.answer()

    @dp.callback_query(F.data == "em:done")
    async def em_done(call: CallbackQuery):
        conn = db.get_conn()
        if not is_employee_active(conn, call.from_user.id):
            conn.close()
            await call.message.answer("Доступ отключен.")
            return await call.answer()
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM tasks WHERE owner_telegram_id=? AND status=? ORDER BY updated_at DESC",
            (call.from_user.id, db.STATUS_DONE),
        )
        rows = cur.fetchall()
        conn.close()
        if not rows:
            await call.message.answer("Завершенных задач нет.")
        else:
            for r in rows[:30]:
                await call.message.answer(format_task(r))
        await call.answer()

    # ---------- Task buttons ----------

    @dp.callback_query(F.data.startswith("t:"))
    async def task_action(call: CallbackQuery):
        await call.answer()
        _, task_id_s, action = call.data.split(":")
        task_id = int(task_id_s)

        conn = db.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM tasks WHERE id=?", (task_id,))
        t = cur.fetchone()
        if not t:
            conn.close()
            return await call.message.answer("Задача не найдена.")

        admin = is_admin(call.from_user.id)
        owner = (t["owner_telegram_id"] == call.from_user.id)

        if not admin:
            if not is_employee_active(conn, call.from_user.id):
                conn.close()
                return await call.message.answer("Доступ отключен.")
            if not owner:
                conn.close()
                return await call.message.answer("Это не твоя задача.")

        if not admin:
            if action == "inprog" and t["status"] == db.STATUS_NEW:
                cur.execute("UPDATE tasks SET status=?, updated_at=? WHERE id=?",
                            (db.STATUS_IN_PROGRESS, db.now_iso(), task_id))
                conn.commit()
                db.audit(conn, task_id, call.from_user.id, "STATUS", "Новая→В процессе")

            elif action == "review" and t["status"] == db.STATUS_IN_PROGRESS:
                cur.execute("UPDATE tasks SET status=?, updated_at=? WHERE id=?",
                            (db.STATUS_ON_REVIEW, db.now_iso(), task_id))
                conn.commit()
                db.audit(conn, task_id, call.from_user.id, "STATUS", "В процессе→На проверке")
                await notify_admin(call.bot, f"🟨 На проверке: задача #{task_id}")

            elif action == "comment":
                conn.close()
                WAIT[call.from_user.id] = {"step": "comment", "task_id": task_id}
                return await call.message.answer(f"Напиши комментарий для задачи #{task_id}:")

            elif action == "file":
                conn.close()
                WAIT[call.from_user.id] = {"step": "file", "task_id": task_id}
                return await call.message.answer(f"Отправь файл для задачи #{task_id}:")

            cur.execute("SELECT * FROM tasks WHERE id=?", (task_id,))
            t2 = cur.fetchone()
            conn.close()
            return await call.message.edit_text(format_task(t2), reply_markup=kb_employee_task(task_id, t2["status"]))

        # admin actions
        if admin:
            if action == "done" and t["status"] == db.STATUS_ON_REVIEW:
                cur.execute("UPDATE tasks SET status=?, updated_at=? WHERE id=?",
                            (db.STATUS_DONE, db.now_iso(), task_id))
                conn.commit()
                db.audit(conn, task_id, call.from_user.id, "STATUS", "На проверке→Готово")
                try:
                    await call.bot.send_message(t["owner_telegram_id"], f"✅ Задача #{task_id} принята. Статус: Готово.", disable_notification=False)
                except Exception:
                    pass

            elif action == "back" and t["status"] == db.STATUS_ON_REVIEW:
                cur.execute("UPDATE tasks SET status=?, updated_at=? WHERE id=?",
                            (db.STATUS_IN_PROGRESS, db.now_iso(), task_id))
                conn.commit()
                db.audit(conn, task_id, call.from_user.id, "STATUS", "На проверке→В процессе")
                try:
                    await call.bot.send_message(t["owner_telegram_id"], f"↩️ Задача #{task_id} возвращена: В процессе.", disable_notification=False)
                except Exception:
                    pass

            elif action == "chgdl":
                conn.close()
                WAIT[call.from_user.id] = {"step": "chgdl", "task_id": task_id}
                return await call.message.answer("Новый срок: YYYY-MM-DD или YYYY-MM-DD HH:MM")

            elif action == "cancel":
                cur.execute("UPDATE tasks SET status=?, updated_at=? WHERE id=?",
                            (db.STATUS_CANCELED, db.now_iso(), task_id))
                conn.commit()
                db.audit(conn, task_id, call.from_user.id, "STATUS", "→Отменено")
                try:
                    await call.bot.send_message(t["owner_telegram_id"], f"🗑 Задача #{task_id} отменена админом.", disable_notification=False)
                except Exception:
                    pass

            cur.execute("SELECT * FROM tasks WHERE id=?", (task_id,))
            t2 = cur.fetchone()
            conn.close()
            return await call.message.edit_text(format_task(t2), reply_markup=kb_admin_task(task_id, t2["status"]))

    # ---------- Text flow (create task / comment / change deadline) ----------

    @dp.message(F.text)
    async def text_flow(message: Message):
        st = WAIT.get(message.from_user.id)
        if not st:
            return

        if not is_admin(message.from_user.id):
            conn = db.get_conn()
            ok = is_employee_active(conn, message.from_user.id)
            conn.close()
            if not ok:
                WAIT.pop(message.from_user.id, None)
                await message.answer("Доступ отключен.")
                return

        # create task steps (admin)
        if st.get("step") == "title":
            st["title"] = message.text.strip()
            st["step"] = "desc"
            return await message.answer("Описание задачи:")

        if st.get("step") == "desc":
            st["desc"] = message.text.strip()
            st["step"] = "deadline"
            return await message.answer("Срок: today / week / days N (пример: days 5)")

        if st.get("step") == "deadline":
            txt = message.text.strip().lower()
            if txt == "today":
                deadline = deadline_today()
            elif txt == "week":
                deadline = deadline_end_of_week()
            elif txt.startswith("days "):
                try:
                    n = int(txt.split()[1])
                    if n < 1 or n > 60:
                        raise ValueError
                    deadline = (datetime.now() + timedelta(days=n)).replace(hour=23, minute=59, second=0).isoformat(timespec="seconds")
                except Exception:
                    return await message.answer("Неверно. Пример: days 5 (1..60)")
            else:
                return await message.answer("Напиши: today / week / days N")

            conn = db.get_conn()
            cur = conn.cursor()
            created = db.now_iso()
            cur.execute(
                """
                INSERT INTO tasks(title, description, status, deadline, owner_telegram_id, department, created_at, updated_at)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                (st["title"], st["desc"], db.STATUS_NEW, deadline, st["target_id"], st["dept"], created, created),
            )
            task_id = cur.lastrowid
            conn.commit()
            db.audit(conn, task_id, message.from_user.id, "CREATE_TASK", f"to={st['target_id']} deadline={deadline}")
            cur.execute("SELECT * FROM tasks WHERE id=?", (task_id,))
            task_row = cur.fetchone()
            conn.close()

            target_id = st["target_id"]
            WAIT.pop(message.from_user.id, None)

            await message.answer(f"✅ Создана задача #{task_id}.")

            # PUSH сотруднику
            try:
                await push_task_assigned(message.bot, target_id, task_row)
            except Exception:
                await notify_admin(
                    message.bot,
                    f"⚠️ PUSH НЕ ДОСТАВЛЕН сотруднику id={target_id} (он мог не нажать /start или заблокировал бота)."
                )
            return

        # comment (employee)
        if st.get("step") == "comment":
            task_id = st["task_id"]
            conn = db.get_conn()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO comments(task_id, author_telegram_id, text, created_at) VALUES(?,?,?,?)",
                (task_id, message.from_user.id, message.text.strip(), db.now_iso()),
            )
            conn.commit()
            db.audit(conn, task_id, message.from_user.id, "COMMENT", message.text.strip()[:200])
            conn.close()
            WAIT.pop(message.from_user.id, None)
            return await message.answer("Комментарий добавлен.")

        # change deadline (admin)
        if st.get("step") == "chgdl":
            if not is_admin(message.from_user.id):
                WAIT.pop(message.from_user.id, None)
                return
            task_id = st["task_id"]
            raw = message.text.strip()
            try:
                if len(raw) == 10:
                    new_deadline = datetime.strptime(raw, "%Y-%m-%d").replace(hour=23, minute=59, second=0).isoformat(timespec="seconds")
                else:
                    new_deadline = datetime.strptime(raw, "%Y-%m-%d %H:%M").isoformat(timespec="seconds")
            except Exception:
                return await message.answer("Формат: 2026-01-20 или 2026-01-20 18:00")

            conn = db.get_conn()
            cur = conn.cursor()
            cur.execute("SELECT deadline, owner_telegram_id FROM tasks WHERE id=?", (task_id,))
            row = cur.fetchone()
            if not row:
                conn.close()
                WAIT.pop(message.from_user.id, None)
                return await message.answer("Задача не найдена.")
            old = row["deadline"]
            owner_id = row["owner_telegram_id"]
            cur.execute("UPDATE tasks SET deadline=?, updated_at=? WHERE id=?", (new_deadline, db.now_iso(), task_id))
            conn.commit()
            db.audit(conn, task_id, message.from_user.id, "CHANGE_DEADLINE", f"{old}→{new_deadline}")
            conn.close()

            WAIT.pop(message.from_user.id, None)
            await message.answer(f"Ок. Срок обновлен: {old} → {new_deadline}")
            try:
                await message.bot.send_message(owner_id, f"🗓 Срок задачи #{task_id} изменён: {old} → {new_deadline}", disable_notification=False)
            except Exception:
                pass
            return

    # ---------- File flow ----------

    @dp.message(F.document | F.photo)
    async def file_flow(message: Message):
        st = WAIT.get(message.from_user.id)
        if not st or st.get("step") != "file":
            return

        if not is_admin(message.from_user.id):
            conn = db.get_conn()
            ok = is_employee_active(conn, message.from_user.id)
            conn.close()
            if not ok:
                WAIT.pop(message.from_user.id, None)
                await message.answer("Доступ отключен.")
                return

        task_id = st["task_id"]

        if message.document:
            file_id = message.document.file_id
            file_name = message.document.file_name
        else:
            file_id = message.photo[-1].file_id
            file_name = "photo.jpg"

        conn = db.get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO files(task_id, uploader_telegram_id, telegram_file_id, file_name, created_at) VALUES(?,?,?,?,?)",
            (task_id, message.from_user.id, file_id, file_name, db.now_iso()),
        )
        conn.commit()
        db.audit(conn, task_id, message.from_user.id, "ADD_FILE", file_name)
        conn.close()

        WAIT.pop(message.from_user.id, None)
        await message.answer("Файл прикреплён.")

    asyncio.create_task(daily_report_loop(bot))
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
