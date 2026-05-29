import os
import random
import string
import asyncio
import psycopg2
import psycopg2.pool
import psycopg2.extras
from datetime import datetime, date, timedelta
from typing import Optional

_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        _pool = psycopg2.pool.ThreadedConnectionPool(
            2, 10,
            os.environ["DATABASE_URL"],
            cursor_factory=psycopg2.extras.RealDictCursor
        )
    return _pool


def _run(fn):
    return asyncio.to_thread(fn)


def _execute(sql: str, params=(), fetch: str = "none"):
    def _inner():
        pool = _get_pool()
        conn = pool.getconn()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    if fetch == "one":
                        return cur.fetchone()
                    elif fetch == "all":
                        return cur.fetchall()
                    elif fetch == "one_returning":
                        return cur.fetchone()
        finally:
            pool.putconn(conn)
    return asyncio.to_thread(_inner)


async def get_pool():
    return _get_pool()


async def init_db():
    await _execute("""
        CREATE TABLE IF NOT EXISTS game_users (
            telegram_id BIGINT PRIMARY KEY,
            username TEXT,
            first_name TEXT NOT NULL,
            balance INTEGER NOT NULL DEFAULT 1000,
            kills INTEGER NOT NULL DEFAULT 0,
            bounty_amount INTEGER NOT NULL DEFAULT 0,
            job TEXT,
            premium BOOLEAN NOT NULL DEFAULT FALSE,
            premium_expires TIMESTAMP,
            ship_id INTEGER,
            protection_until TIMESTAMP,
            custom_emoji TEXT,
            custom_title TEXT,
            daily_last TIMESTAMP,
            rob_count_today INTEGER NOT NULL DEFAULT 0,
            rob_date DATE
        )
    """)
    await _execute("""
        CREATE TABLE IF NOT EXISTS ships (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            code CHAR(4) NOT NULL UNIQUE,
            captain_id BIGINT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        )
    """)
    await _execute("""
        CREATE TABLE IF NOT EXISTS ship_members (
            id SERIAL PRIMARY KEY,
            ship_id INTEGER NOT NULL,
            user_id BIGINT NOT NULL,
            role TEXT NOT NULL DEFAULT 'member'
        )
    """)
    await _execute("""
        CREATE TABLE IF NOT EXISTS user_items (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            item_name TEXT NOT NULL,
            purchased_at TIMESTAMP NOT NULL DEFAULT NOW()
        )
    """)
    await _execute("""
        CREATE TABLE IF NOT EXISTS balance_codes (
            code TEXT PRIMARY KEY,
            amount INTEGER NOT NULL,
            redeemed BOOLEAN NOT NULL DEFAULT FALSE
        )
    """)
    await _execute("""
        CREATE TABLE IF NOT EXISTS bounty_codes (
            code TEXT PRIMARY KEY,
            amount INTEGER NOT NULL,
            redeemed BOOLEAN NOT NULL DEFAULT FALSE
        )
    """)
    await _execute("""
        CREATE TABLE IF NOT EXISTS group_warns (
            id SERIAL PRIMARY KEY,
            group_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            warn_count INTEGER NOT NULL DEFAULT 0
        )
    """)


def _row_to_dict(row) -> Optional[dict]:
    if row is None:
        return None
    return dict(row)


async def get_or_create_user(telegram_id: int, first_name: str, username: Optional[str] = None) -> dict:
    row = _row_to_dict(await _execute(
        "SELECT * FROM game_users WHERE telegram_id = %s", (telegram_id,), fetch="one"
    ))
    is_owner = username and username.lower() in ["light_speedy", "light_speedi"]
    if row:
        updates = {}
        if row["first_name"] != first_name:
            updates["first_name"] = first_name
        if row["username"] != (username or None):
            updates["username"] = username
        if is_owner and not row["premium"]:
            updates["premium"] = True
            updates["premium_expires"] = None
        if updates:
            cols = list(updates.keys())
            vals = list(updates.values())
            set_clause = ", ".join(f"{c} = %s" for c in cols)
            await _execute(
                f"UPDATE game_users SET {set_clause} WHERE telegram_id = %s",
                (*vals, telegram_id)
            )
            row.update(updates)
        return row
    await _execute(
        """INSERT INTO game_users (telegram_id, first_name, username, balance, kills, bounty_amount, premium)
           VALUES (%s, %s, %s, 1000, 0, 0, %s)
           ON CONFLICT (telegram_id) DO NOTHING""",
        (telegram_id, first_name, username, bool(is_owner))
    )
    row = _row_to_dict(await _execute(
        "SELECT * FROM game_users WHERE telegram_id = %s", (telegram_id,), fetch="one"
    ))
    return row


async def get_user_by_id(telegram_id: int) -> Optional[dict]:
    return _row_to_dict(await _execute(
        "SELECT * FROM game_users WHERE telegram_id = %s", (telegram_id,), fetch="one"
    ))


async def get_user_by_username(username: str) -> Optional[dict]:
    return _row_to_dict(await _execute(
        "SELECT * FROM game_users WHERE username = %s", (username.lstrip("@"),), fetch="one"
    ))


def is_premium_active(user: dict) -> bool:
    if not user.get("premium"):
        return False
    expires = user.get("premium_expires")
    if expires is None:
        return True
    if isinstance(expires, datetime):
        return expires > datetime.utcnow()
    return False


def is_protected(user: dict) -> bool:
    until = user.get("protection_until")
    if not until:
        return False
    if isinstance(until, datetime):
        return until > datetime.utcnow()
    return False


async def get_global_rank(telegram_id: int, balance: int) -> int:
    row = _row_to_dict(await _execute(
        "SELECT COUNT(*) as cnt FROM game_users WHERE balance > %s", (balance,), fetch="one"
    ))
    return int(row["cnt"]) + 1


async def get_kill_rank(telegram_id: int) -> int:
    user = await get_user_by_id(telegram_id)
    if not user:
        return 9999
    row = _row_to_dict(await _execute(
        "SELECT COUNT(*) as cnt FROM game_users WHERE kills > %s", (user["kills"],), fetch="one"
    ))
    return int(row["cnt"]) + 1


def get_kill_tag(kills: int, kill_rank: int) -> str:
    if kills >= 200 and kill_rank <= 7:
        return " [𝗪𝗮𝗿𝗹𝗼𝗿𝗱 𝗼𝗳 𝗦𝗲𝗮]"
    if 100 <= kills < 200 and kill_rank <= 30:
        return " [𝗦𝘄𝗼𝗿𝗱𝘀𝗺𝗮𝗻]"
    return ""


async def get_user_ship(ship_id: Optional[int]) -> Optional[dict]:
    if not ship_id:
        return None
    return _row_to_dict(await _execute(
        "SELECT * FROM ships WHERE id = %s", (ship_id,), fetch="one"
    ))


async def get_ship_balance(ship_id: int) -> int:
    row = _row_to_dict(await _execute(
        """SELECT COALESCE(SUM(gu.balance), 0) as total
           FROM ship_members sm
           JOIN game_users gu ON sm.user_id = gu.telegram_id
           WHERE sm.ship_id = %s""",
        (ship_id,), fetch="one"
    ))
    return int(row["total"])


async def get_ship_member_count(ship_id: int) -> int:
    row = _row_to_dict(await _execute(
        "SELECT COUNT(*) as cnt FROM ship_members WHERE ship_id = %s", (ship_id,), fetch="one"
    ))
    return int(row["cnt"])


async def get_ship_member_role(ship_id: int, user_id: int) -> Optional[str]:
    row = _row_to_dict(await _execute(
        "SELECT role FROM ship_members WHERE ship_id = %s AND user_id = %s",
        (ship_id, user_id), fetch="one"
    ))
    return row["role"] if row else None


async def get_ship_by_code(code: str) -> Optional[dict]:
    return _row_to_dict(await _execute(
        "SELECT * FROM ships WHERE code = %s", (code,), fetch="one"
    ))


async def get_ship_by_name(name: str) -> Optional[dict]:
    return _row_to_dict(await _execute(
        "SELECT * FROM ships WHERE LOWER(name) = LOWER(%s)", (name,), fetch="one"
    ))


async def get_ship_by_id(ship_id: int) -> Optional[dict]:
    return _row_to_dict(await _execute(
        "SELECT * FROM ships WHERE id = %s", (ship_id,), fetch="one"
    ))


async def get_top_ships(limit: int = 30) -> list:
    rows = await _execute("SELECT * FROM ships", fetch="all")
    result = []
    for row in (rows or []):
        s = dict(row)
        s["ship_balance"] = await get_ship_balance(s["id"])
        s["member_count"] = await get_ship_member_count(s["id"])
        result.append(s)
    result.sort(key=lambda x: x["ship_balance"], reverse=True)
    return result[:limit]


async def generate_unique_ship_code() -> str:
    while True:
        code = str(random.randint(1000, 9999))
        if not await get_ship_by_code(code):
            return code


async def get_user_items(user_id: int) -> list:
    rows = await _execute(
        "SELECT item_name FROM user_items WHERE user_id = %s", (user_id,), fetch="all"
    )
    return [r["item_name"] for r in (rows or [])]


async def get_most_expensive_item(user_id: int) -> Optional[str]:
    from constants import ITEMS
    owned = await get_user_items(user_id)
    if not owned:
        return None
    matching = [i for i in ITEMS if i["name"] in owned]
    if not matching:
        return None
    best = sorted(matching, key=lambda x: x["price"], reverse=True)[0]
    return f"{best['emoji']} {best['name']} (${best['price']:,})"


async def get_top_rich(limit: int = 10) -> list:
    rows = await _execute(
        "SELECT * FROM game_users ORDER BY balance DESC LIMIT %s", (limit,), fetch="all"
    )
    return [dict(r) for r in (rows or [])]


async def get_top_killers(limit: int = 10) -> list:
    rows = await _execute(
        "SELECT * FROM game_users ORDER BY kills DESC LIMIT %s", (limit,), fetch="all"
    )
    return [dict(r) for r in (rows or [])]


async def get_top_bounty(limit: int = 10) -> list:
    rows = await _execute(
        "SELECT * FROM game_users ORDER BY bounty_amount DESC LIMIT %s", (limit,), fetch="all"
    )
    return [dict(r) for r in (rows or [])]


async def generate_balance_code(amount: int) -> str:
    code = "BAL-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
    await _execute(
        "INSERT INTO balance_codes (code, amount, redeemed) VALUES (%s, %s, FALSE)",
        (code, amount)
    )
    return code


async def redeem_balance_code(code: str) -> Optional[int]:
    row = _row_to_dict(await _execute(
        "SELECT * FROM balance_codes WHERE code = %s", (code,), fetch="one"
    ))
    if not row or row["redeemed"]:
        return None
    await _execute("UPDATE balance_codes SET redeemed = TRUE WHERE code = %s", (code,))
    return row["amount"]


async def generate_bounty_code(amount: int) -> str:
    code = "BNT-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
    await _execute(
        "INSERT INTO bounty_codes (code, amount, redeemed) VALUES (%s, %s, FALSE)",
        (code, amount)
    )
    return code


async def redeem_bounty_code(code: str) -> Optional[int]:
    row = _row_to_dict(await _execute(
        "SELECT * FROM bounty_codes WHERE code = %s", (code,), fetch="one"
    ))
    if not row or row["redeemed"]:
        return None
    await _execute("UPDATE bounty_codes SET redeemed = TRUE WHERE code = %s", (code,))
    return row["amount"]


async def get_warn_count(group_id: int, user_id: int) -> int:
    row = _row_to_dict(await _execute(
        "SELECT warn_count FROM group_warns WHERE group_id = %s AND user_id = %s",
        (group_id, user_id), fetch="one"
    ))
    return row["warn_count"] if row else 0


async def add_warn(group_id: int, user_id: int) -> int:
    row = _row_to_dict(await _execute(
        "SELECT warn_count FROM group_warns WHERE group_id = %s AND user_id = %s",
        (group_id, user_id), fetch="one"
    ))
    if row:
        new_count = row["warn_count"] + 1
        await _execute(
            "UPDATE group_warns SET warn_count = %s WHERE group_id = %s AND user_id = %s",
            (new_count, group_id, user_id)
        )
    else:
        new_count = 1
        await _execute(
            "INSERT INTO group_warns (group_id, user_id, warn_count) VALUES (%s, %s, 1)",
            (group_id, user_id)
        )
    return new_count


async def remove_warn(group_id: int, user_id: int) -> int:
    row = _row_to_dict(await _execute(
        "SELECT warn_count FROM group_warns WHERE group_id = %s AND user_id = %s",
        (group_id, user_id), fetch="one"
    ))
    if not row or row["warn_count"] <= 0:
        return 0
    new_count = max(0, row["warn_count"] - 1)
    await _execute(
        "UPDATE group_warns SET warn_count = %s WHERE group_id = %s AND user_id = %s",
        (new_count, group_id, user_id)
    )
    return new_count


async def reset_warns(group_id: int, user_id: int):
    await _execute(
        "DELETE FROM group_warns WHERE group_id = %s AND user_id = %s",
        (group_id, user_id)
    )


def rand(min_val: int, max_val: int) -> int:
    return random.randint(min_val, max_val)


def today_date() -> str:
    return date.today().isoformat()


async def update_user(telegram_id: int, **kwargs):
    if not kwargs:
        return
    cols = list(kwargs.keys())
    vals = list(kwargs.values())
    set_clause = ", ".join(f"{c} = %s" for c in cols)
    await _execute(
        f"UPDATE game_users SET {set_clause} WHERE telegram_id = %s",
        (*vals, telegram_id)
    )


async def execute_raw(sql: str, params=(), fetch: str = "none"):
    return await _execute(sql, params, fetch=fetch)
