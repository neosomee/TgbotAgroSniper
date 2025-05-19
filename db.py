import asyncio
import aiosqlite
import logging

DB_PATH = "users.db"

class Database:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self._conn = None

    async def connect(self):
        logging.info("РџРѕРґРєР»СЋС‡РµРЅРёРµ Рє Р±Р°Р·Рµ РґР°РЅРЅС‹С…...")
        self._conn = await aiosqlite.connect(self.db_path)
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await self._conn.commit()
        logging.info("Р‘Р°Р·Р° РґР°РЅРЅС‹С… РїРѕРґРєР»СЋС‡РµРЅР°.")

    async def add_user(self, user_id: int, username: str = None, first_name: str = None, last_name: str = None):
        async with self._conn.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,)) as cursor:
            exists = await cursor.fetchone()
        if not exists:
            await self._conn.execute(
                "INSERT INTO users (user_id, username, first_name, last_name) VALUES (?, ?, ?, ?)",
                (user_id, username, first_name, last_name)
            )
            await self._conn.commit()

    async def get_all_users(self):
        async with self._conn.execute("SELECT user_id, username FROM users") as cursor:
            rows = await cursor.fetchall()
        return rows

    async def is_user_exists(self, user_id: int) -> bool:
        async with self._conn.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone() is not None

    async def close(self):
        if self._conn:
            logging.info("РќР°С‡РёРЅР°СЋ Р·Р°РєСЂС‹С‚РёРµ СЃРѕРµРґРёРЅРµРЅРёСЏ СЃ Р±Р°Р·РѕР№ РґР°РЅРЅС‹С…...")
            try:
                await asyncio.wait_for(self._conn.close(), timeout=5)
                logging.info("РЎРѕРµРґРёРЅРµРЅРёРµ СЃ Р±Р°Р·РѕР№ РґР°РЅРЅС‹С… СѓСЃРїРµС€РЅРѕ Р·Р°РєСЂС‹С‚Рѕ.")
            except asyncio.TimeoutError:
                logging.warning("Р—Р°РєСЂС‹С‚РёРµ СЃРѕРµРґРёРЅРµРЅРёСЏ СЃ Р±Р°Р·РѕР№ РґР°РЅРЅС‹С… Р·Р°РЅСЏР»Рѕ СЃР»РёС€РєРѕРј РјРЅРѕРіРѕ РІСЂРµРјРµРЅРё Рё Р±С‹Р»Рѕ РїСЂРµСЂРІР°РЅРѕ.")
