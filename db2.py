import asyncio
import aiosqlite
import logging

DB_PATH = "users.db"

class Database:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self._conn = None

    async def connect(self):
        logging.info("Подключение к базе данных...")
        self._conn = await aiosqlite.connect(self.db_path)
        # Create users table
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Create products table
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id TEXT PRIMARY KEY,
                sku TEXT,
                name TEXT,
                price REAL,
                quantity INTEGER,
                url TEXT,
                image TEXT,
                category TEXT,
                images TEXT,
                product_images TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await self._conn.commit()
        logging.info("База данных подключена.")

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

    async def upsert_products(self, products: list):
        """
        Insert or update products in the database.
        Replaces existing products to ensure data consistency.
        """
        logging.info(f"Обновление {len(products)} товаров в базе данных...")
        try:
            # Clear existing products
            await self._conn.execute("DELETE FROM products")
            
            # Insert new products
            for product in products:
                await self._conn.execute(
                    """
                    INSERT INTO products (
                        id, sku, name, price, quantity, url, image, category, images, product_images
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(product.get('_ID_', '')),
                        str(product.get('_SKU_', '')),
                        str(product.get('_NAME_', '')),
                        float(product.get('_PRICE_', 0.0)) if product.get('_PRICE_') else 0.0,
                        int(product.get('_QUANTITY_', 0)) if product.get('_QUANTITY_') else 0,
                        str(product.get('_URL_', '')),
                        str(product.get('_IMAGE_', '')),
                        str(product.get('_CATEGORY_', 'Без категории')),
                        str(product.get('_IMAGES_', '')),
                        str(product.get('_PRODUCT_IMAGES_', ''))
                    )
                )
            await self._conn.commit()
            logging.info(f"Успешно обновлено {len(products)} товаров.")
        except Exception as e:
            logging.error(f"Ошибка при обновлении товаров: {e}")
            raise

    async def get_all_products(self):
        """
        Retrieve all products from the database.
        """
        async with self._conn.execute("""
            SELECT id, sku, name, price, quantity, url, image, category, images, product_images
            FROM products
        """) as cursor:
            rows = await cursor.fetchall()
        # Convert rows to list of dictionaries
        columns = ['_ID_', '_SKU_', '_NAME_', '_PRICE_', '_QUANTITY_', '_URL_', '_IMAGE_', '_CATEGORY_', '_IMAGES_', '_PRODUCT_IMAGES_']
        return [dict(zip(columns, row)) for row in rows]

    async def close(self):
        if self._conn:
            logging.info("Начинаю закрытие соединения с базой данных...")
            try:
                await asyncio.wait_for(self._conn.close(), timeout=5)
                logging.info("Соединение с базой данных успешно закрыто.")
            except asyncio.TimeoutError:
                logging.warning("Закрытие соединения с базой данных заняло слишком много времени и было прервано.")
