import asyncio
import math
import logging
import re
import sys
import pandas as pd
import chardet
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, quote
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.enums.parse_mode import ParseMode
import idna
from aiogram.types.input_file import BufferedInputFile
from concurrent.futures import ThreadPoolExecutor
import io
import unicodedata
import aiohttp
import difflib
from aiohttp_retry import RetryClient, ExponentialRetry
from datetime import datetime
from aiogram.utils.markdown import html_decoration as hd
from aiogram.utils.markdown import hbold, hcode, hlink
from db import Database

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)

# Исправление политики цикла событий для Windows
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Инициализация пула потоков
executor = ThreadPoolExecutor(max_workers=5)

# Константы
API_TOKEN = "7626300396:AAHxkGqY2GnarCEoxVlm9IfS-MCAfvG6fSM"
ADMIN_USERNAME = '@lprost'
ORDER_CHANNEL = -1002310332672
MAX_ROWS_PER_FILE = 1000
BASE_URL = "https://xn--80aaijtwglegf.xn--p1ai/"
admin_ids = [5056594883, 6521061663]

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
db = Database()

# Глобальные переменные
categories = []
products = []
products_by_id = {}
categories_dict = {}
user_carts = {}

# Маппинг похожих символов
SIMILAR_CHARS_MAP = {
    'А': 'A', 'В': 'B', 'Е': 'E', 'К': 'K', 'М': 'M', 'Н': 'H',
    'О': 'O', 'Р': 'P', 'С': 'C', 'Т': 'T', 'У': 'Y', 'Х': 'X',
    'а': 'A', 'в': 'B', 'е': 'E', 'к': 'K', 'м': 'M', 'н': 'H',
    'о': 'O', 'р': 'P', 'с': 'C', 'т': 'T', 'у': 'Y', 'х': 'X',
}

# Состояния FSM
class OrderQuantity(StatesGroup):
    waiting_for_quantity = State()
    waiting_for_contact = State()
    waiting_for_address = State()

class UploadStates(StatesGroup):
    waiting_for_categories = State()
    waiting_for_products = State()

class UserStates(StatesGroup):
    waiting_for_article_request = State()
    article_requested_once = State()

class MultipleArticlesStates(StatesGroup):
    waiting_for_file = State()

class OrderStates(StatesGroup):
    waiting_for_contact = State()
    waiting_for_address = State()

class AdminStates(StatesGroup):
    waiting_for_broadcast_content = State()
    waiting_for_categories = State()
    waiting_for_products = State()

# Функции для клавиатур
def remove_keyboard():
    return ReplyKeyboardMarkup(keyboard=[], resize_keyboard=True)

def get_cart_confirmation_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🛒 Перейти в корзину")],
            [KeyboardButton(text="🏠 Основное меню")]
        ],
        resize_keyboard=True
    )

def get_main_menu_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="🔍 Запрос одного артикула"),
                KeyboardButton(text="📊 Просчёт Excel с артикулами"),
            ],
            [
                KeyboardButton(text="🛒 Корзина"),
                KeyboardButton(text="👨‍💻 Связь с поддержкой")
            ]
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие"
    )

def get_back_to_main_menu_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🏠 Основное меню")]],
        resize_keyboard=True
    )

def get_product_keyboard(product_id, quantity_available):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="🛒 Добавить в корзину",
            callback_data=f"add_{product_id}_{quantity_available}"
        )]
    ])

def get_cart_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🗑 Очистить корзину"), KeyboardButton(text="✅ Оформить заказ")],
            [KeyboardButton(text="🏠 Основное меню")]
        ],
        resize_keyboard=True
    )

def get_support_inline_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Написать менеджеру",
            url="https://t.me/zucman61"
        )]
    ])

def get_admin_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📦 Загрузить продукты")],
            [KeyboardButton(text="📊 Статистика")],
            [KeyboardButton(text="📢 Рассылка сообщений")],
            [KeyboardButton(text="🏠 Выход в основное меню")]
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие"
    )

# Утилитные функции
def shorten_url_yandex(long_url: str) -> str | None:
    """Синхронная функция сокращения ссылки через clck.ru"""
    try:
        response = requests.get(f'https://clck.ru/--?url={quote(long_url)}')
        if response.status_code == 200:
            return response.text
        logging.error(f"Ошибка при сокращении ссылки: {response.status_code}")
        return None
    except Exception as e:
        logging.exception("Ошибка при обращении к clck.ru")
        return None

async def shorten_url_yandex_async(long_url: str) -> str | None:
    """Асинхронная обёртка для сокращения ссылок с лимитом 0.2 секунды"""
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(executor, shorten_url_yandex, long_url)
    await asyncio.sleep(0.2)
    return result

def split_message(text: str, max_length: int = 4096) -> list:
    """Разделение длинного сообщения на части"""
    parts = []
    while len(text) > max_length:
        split_pos = text.rfind('\n', 0, max_length)
        if split_pos == -1:
            split_pos = max_length
        parts.append(text[:split_pos])
        text = text[split_pos:]
    parts.append(text)
    return parts

def normalize_article(article: str) -> str:
    """Универсальная нормализация артикула или названия"""
    if not article:
        return ''
    article = str(article)
    article = unicodedata.normalize('NFKC', article).upper()
    # Замена похожих символов
    article = ''.join(SIMILAR_CHARS_MAP.get(ch, ch) for ch in article)
    # Удаляем лишние символы, оставляем буквы, цифры и некоторые разделители
    article = re.sub(r'[^A-Z0-9\s\-\./]', '', article)
    # Удаляем лишние пробелы и нормализуем
    article = ' '.join(article.split())
    return article

def get_product_image_url(product: dict) -> str | None:
    """Получение URL изображения товара"""
    img = product.get('_IMAGE_', '').strip()
    if img:
        return img if img.startswith('http') else urljoin(BASE_URL, img)
    for field in ['_IMAGES_', '_PRODUCT_IMAGES_']:
        imgs = product.get(field)
        if imgs:
            first_img = imgs.split(';')[0].strip()
            if first_img:
                return first_img if first_img.startswith('http') else urljoin(BASE_URL, first_img)
    return None

async def get_image_url_from_product_page(url: str) -> str | None:
    """Получение URL изображения со страницы товара"""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        img_tag = soup.select_one('.product-image img') or soup.select_one('.product-page img')
        if img_tag and img_tag.get('src'):
            img_url = img_tag['src']
            if not img_url.startswith('http'):
                img_url = urljoin(url, img_url)
            return img_url
    except Exception as e:
        logging.warning(f"Ошибка при парсинге фото с сайта {url}: {e}")
    return None

def find_product_by_article(article_query: str, products: list, use_cache=True):
    """Поиск товара по артикулу или названию с использованием кэша и поиска по первому слову"""
    norm_query = normalize_article(article_query)
    query_words = norm_query.split()  # Разбиваем запрос на слова
    first_query_word = query_words[0] if query_words else norm_query  # Берем первое слово или весь запрос, если пусто

    if use_cache:
        if not hasattr(find_product_by_article, '_cache'):
            find_product_by_article._cache = {}
            find_product_by_article._sku_map = {}
            find_product_by_article._name_map = {}
            find_product_by_article._products = products

            for p in products:
                norm_sku = normalize_article(p.get('_SKU_', ''))
                norm_name = normalize_article(p.get('_NAME_', ''))
                if norm_sku:
                    find_product_by_article._cache[norm_sku] = p
                    find_product_by_article._sku_map[norm_sku] = p
                if norm_name:
                    find_product_by_article._cache[norm_name] = p
                    find_product_by_article._name_map[norm_name] = p

        # 1. Точное совпадение по артикулу (SKU)
        if norm_query in find_product_by_article._sku_map:
            return [find_product_by_article._sku_map[norm_query]]

        # 2. Поиск по первому слову названия
        matched_products = []
        for key, prod in find_product_by_article._name_map.items():
            prod_words = key.split()
            if prod_words and first_query_word.lower() == prod_words[0].lower():  # Case-insensitive match
                matched_products.append(prod)

        if matched_products:
            return matched_products

        # 3. Для однословного запроса - поиск всех продуктов, содержащих это слово
        if len(query_words) == 1:
            matched_products = []
            for key, prod in find_product_by_article._name_map.items():
                if first_query_word.lower() in key.lower():  # Case-insensitive search
                    matched_products.append(prod)
            if matched_products:
                return matched_products

        # 4. Точное совпадение по имени
        if norm_query in find_product_by_article._name_map:
            return [find_product_by_article._name_map[norm_query]]

        # 5. Fuzzy поиск по NAME
        name_keys = list(find_product_by_article._name_map.keys())
        close_names = difflib.get_close_matches(norm_query, name_keys, n=1, cutoff=0.6)
        if close_names:
            return [find_product_by_article._name_map[close_names[0]]]

        # 6. Fuzzy поиск по SKU
        sku_keys = list(find_product_by_article._sku_map.keys())
        close_skus = difflib.get_close_matches(norm_query, sku_keys, n=1, cutoff=0.6)
        if close_skus:
            return [find_product_by_article._sku_map[close_skus[0]]]

        return None
    else:
        # Логика без кэша (упрощённая)
        matched_products = []
        for p in products:
            norm_sku = normalize_article(p.get('_SKU_', ''))
            norm_name = normalize_article(p.get('_NAME_', ''))
            prod_words = norm_name.split()
            if prod_words and first_query_word.lower() == prod_words[0].lower():  # Case-insensitive match
                matched_products.append(p)
        if matched_products:
            return matched_products
        return None

def find_product_by_excel_row(row: dict, products: list):
    """
    Ищет товар сначала по артикулу из Excel, затем по названию.
    row - словарь с ключами 'Артикул' и 'Название'
    """
    product = find_product_by_article(row.get('Артикул', ''), products)
    if product is None:
        product = find_product_by_article(row.get('Название', ''), products)
    return product

def clear_find_product_cache():
    """Очистка кэша поиска"""
    if hasattr(find_product_by_article, '_cache'):
        del find_product_by_article._cache

def parse_price(price_str):
    """Парсинг цены из строки"""
    try:
        price_clean = str(price_str).replace(' ', '').replace(',', '.')
        return float(price_clean)
    except:
        return 0.0

def normalize_sku(sku: str) -> str:
    """Нормализация артикула"""
    return str(sku).replace('.', '').strip()

def format_product_info(product: dict, short_url: str = None, sku: str = None, category: str = None) -> str:
    if sku is None:
        sku = product.get('_SKU_', '')
    if isinstance(sku, float) and str(sku).lower() == 'nan':
        sku = ''
    name = product.get('_NAME_', 'Без названия')
    price = product.get('_PRICE_', 'Цена не указана')
    try:
        price_str = f"{float(price):.2f} ₽"
    except (ValueError, TypeError):
        price_str = str(price)
    quantity = product.get('_QUANTITY_', 0)
    text = (
        f"🛠️ Название: {hbold(name)}\n"
        f"🔖 Артикул: {hcode(sku)}\n"
        f"💰 Цена: {hbold(price_str)}\n"
        f"📦 В наличии: {hbold(str(quantity))} шт.\n"
    )
    if category:
        text += f"📂 Категория: {hbold(category)}\n"
    if short_url:
        text += f"🔗 {hlink('Посмотреть на сайте', short_url)}\n"
    return text


async def send_message_in_parts(message: types.Message, text: str, **kwargs):
    """Отправка сообщения частями"""
    for part in split_message(text):
        await message.answer(part, **kwargs)

# Обработчики команд и сообщений
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Обработчик команды /start"""
    user = message.from_user
    await db.add_user(
        user_id=user.id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
    )
    original_url = "https://агроснайпер.рф/image/catalog/logoagro3.png"
    punycode_domain = idna.encode("агроснайпер.рф").decode()
    photo_url = original_url.replace("агроснайпер.рф", punycode_domain)
    caption = (
        "👋 Привет! Добро пожаловать в наш Агроснайпер бот.\n"
        "Сайт: Агроснайпер.рф\n\n"
        "Вот что ты можешь сделать:\n"
        "1️⃣ *🔍 Запрос одного артикула* - введи артикул, чтобы получить информацию и фото товара.\n"
        "2️⃣ *📊 Просчёт Excel с артикулами* - отправь Excel-файл с артикулами и количеством, и я сразу добавлю товары в корзину.\n"
        "3️⃣ *🛒 Корзина* - здесь ты можешь посмотреть добавленные товары, изменить количество или оформить заказ.\n"
        "4️⃣ *👨‍💻 Связь с поддержкой* - контакты менеджера, если нужна помощь.\n\n"
        "🔹 После каждого действия у тебя будет кнопка *🏠 Основное меню* для быстрого возврата сюда.\n"
        "🔹 Чтобы добавить товар в корзину, после запроса артикула нажми на кнопку \"🛒 Добавить в корзину\" и укажи количество.\n"
        "🔹 Для оформления заказа перейди в корзину и следуй инструкциям.\n\n"
        "Если возникнут вопросы - пиши в раздел связи с поддержкой.\n\n"
        "Желаем приятных покупок! 🛍️"
    )
    await message.answer_photo(
        photo=photo_url,
        caption=caption,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_main_menu_keyboard()
    )

@dp.message(F.text == "🏠 Основное меню")
async def back_to_main_menu(message: types.Message, state: FSMContext):
    """Возврат в основное меню"""
    await state.clear()
    await message.answer(
        "👋 Главное меню. Выберите действие:",
        reply_markup=get_main_menu_keyboard()
    )

@dp.message(F.text == "👨‍💻 Связь с поддержкой")
async def contact_support(message: types.Message):
    """Обработчик связи с поддержкой"""
    text = (
        "📞 *Директор ООО Агроснайпер:*\n Юрий Мороз\n"
        "📧 *Электронная почта:* agrosnaiper@yandex.ru\n"
        "📱 *Телефон:* +7 (928) 279-05-29\n\n"
        "Сайт: Агроснайпер.рф"
    )
    await send_message_in_parts(
        message,
        text,
        parse_mode="Markdown",
        reply_markup=get_support_inline_keyboard()
    )

@dp.message(Command("admin"))
async def admin_panel(message: types.Message, state: FSMContext):
    """Обработчик команды /admin"""
    if message.from_user.id in admin_ids:
        await state.clear()
        await message.answer(
            "🛠️ Админ-панель. Что хотите сделать?",
            reply_markup=get_admin_keyboard()
        )
    else:
        await message.answer("❌ У вас нет прав для доступа к админ-панели.")

@dp.message(F.text == "🏠 Выход в основное меню")
async def exit_admin_panel(message: types.Message, state: FSMContext):
    """Выход из админ-панели"""
    if message.from_user.id in admin_ids:
        await state.clear()
        await message.answer(
            "✅ Вы вышли из админ-панели",
            reply_markup=get_main_menu_keyboard()
        )
    else:
        await message.answer("❌ У вас нет прав для этого действия.")

@dp.message(F.text == "📢 Рассылка сообщений")
async def start_broadcast(message: types.Message, state: FSMContext):
    """Начало рассылки сообщений"""
    if message.from_user.id not in admin_ids:
        await message.answer("❌ У вас нет прав для этого действия.")
        return
    await message.answer(
        "✉️ Отправьте сообщение для рассылки...",
        reply_markup=remove_keyboard()
    )
    await state.set_state(AdminStates.waiting_for_broadcast_content)

@dp.message(AdminStates.waiting_for_broadcast_content)
async def process_broadcast_content(message: types.Message, state: FSMContext):
    """Обработка контента для рассылки"""
    if message.from_user.id not in admin_ids:
        await message.answer("❌ У вас нет прав для этого действия.")
        await state.clear()
        return
    await message.answer("⏳ Начинаю рассылку...")
    users = await db.get_all_users()
    success_count = 0
    fail_count = 0
    if message.photo:
        photo = message.photo[-1].file_id
        caption = message.caption or ""
        send_func = bot.send_photo
        send_kwargs = {"photo": photo, "caption": caption}
    elif message.video:
        video = message.video.file_id
        caption = message.caption or ""
        send_func = bot.send_video
        send_kwargs = {"video": video, "caption": caption}
    elif message.text:
        send_func = bot.send_message
        send_kwargs = {"text": message.text}
    else:
        await message.answer("❌ Неподдерживаемый тип сообщения. Пожалуйста, отправьте текст, фото или видео.")
        await state.clear()
        return
    async def send_to_user(user_id):
        nonlocal success_count, fail_count
        try:
            await send_func(chat_id=user_id, **send_kwargs)
            success_count += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            logging.error(f"Ошибка при отправке пользователю {user_id}: {e}")
            fail_count += 1
    tasks = [send_to_user(user_id) for user_id, _ in users]
    await asyncio.gather(*tasks)
    await message.answer(
        f"✅ Рассылка завершена!\nУспешно: {success_count}\nНе удалось: {fail_count}",
        reply_markup=get_admin_keyboard()
    )
    await state.clear()

@dp.message(F.text == "📦 Загрузить продукты")
async def load_products(message: types.Message, state: FSMContext):
    """Загрузка продуктов"""
    if message.from_user.id in admin_ids:
        await message.answer("📁 Отправьте CSV-файл с продуктами.", reply_markup=get_back_to_main_menu_keyboard())
        await state.set_state(UploadStates.waiting_for_products)
    else:
        await message.answer("❌ У вас нет прав для этого действия.", reply_markup=get_back_to_main_menu_keyboard())

@dp.message(F.text == "📊 Статистика")
async def show_stats(message: types.Message):
    """Показ статистики"""
    if message.from_user.id not in admin_ids:
        await message.answer("❌ У вас нет прав для этого действия.", reply_markup=get_back_to_main_menu_keyboard())
        return
    users = await db.get_all_users()
    users_count = len(users)
    await message.answer(
        f"📈 Количество пользователей в боте: {users_count}\n"
        f"📈 Загружено продуктов: {len(products)}",
        reply_markup=get_admin_keyboard()
    )

@dp.message(UploadStates.waiting_for_products, F.document)
async def process_products_file(message: types.Message, state: FSMContext):
    try:
        file_id = message.document.file_id
        file = await bot.get_file(file_id)
        file_path = file.file_path
        file_content = await bot.download_file(file_path)
        raw_data = file_content.read()

        result = chardet.detect(raw_data)
        encoding = result['encoding'] or 'utf-8'

        df = pd.read_csv(io.BytesIO(raw_data), sep=';', encoding=encoding, header=0)
        df.columns = df.columns.str.strip('"').str.strip()

        global products, products_by_id
        products = df.to_dict('records')

        # Сохраняем категорию
        for product in products:
            product['category_name'] = product.get('_CATEGORY_', 'Без категории')

        products_by_id = {str(p['_ID_']): p for p in products if '_ID_' in p}

        clear_find_product_cache()  # Очищаем кэш после обновления продуктов
        await message.answer(f"✅ Загружено {len(products)} товаров.", reply_markup=get_admin_keyboard())
        await state.clear()

    except Exception as e:
        logging.exception("Ошибка при обработке файла товаров")
        await message.answer(f"❌ Ошибка при обработке файла товаров: {e}", reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "🔍 Запрос одного артикула")
async def start_single_article(message: types.Message, state: FSMContext):
    """Начало запроса одного артикула"""
    await message.answer("✏️ Введите артикул для поиска информации и фото товара:", reply_markup=get_back_to_main_menu_keyboard())
    await state.set_state(UserStates.waiting_for_article_request)

@dp.message(UserStates.waiting_for_article_request)
async def handle_article_request(message: types.Message, state: FSMContext):
    """Обработчик запроса одного артикула"""
    print(f"handle_article_request вызван для пользователя {message.from_user.id}")
    text = message.text.strip()

    if text in ["🗑 Очистить корзину", "✅ Оформить заказ", "🛒 Корзина", "🏠 Основное меню"]:
        if text == "🗑 Очистить корзину":
            await clear_cart_func(message, state)
            await state.clear()
        elif text == "✅ Оформить заказ":
            await checkout_func(message, state)
        elif text == "🛒 Корзина":
            await show_cart_func(message, state)
            await state.clear()
        elif text == "🏠 Основное меню":
            await message.answer("🏠 Главное меню", reply_markup=get_main_menu_keyboard())
            await state.clear()
        return

    raw_query = text
    norm_query = normalize_article(raw_query)
    logging.info(f"Поиск товара: raw_query='{raw_query}', norm_query='{norm_query}', first_word='{norm_query.split()[0] if norm_query.split() else ''}'")

    products_found = find_product_by_article(raw_query, products)
    if products_found:
        for product in products_found:
            product_url = product.get('_URL_')
            photo_sent = False

            # Попытка отправить фото
            if product_url:
                try:
                    response = requests.get(product_url, timeout=10)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.content, 'html.parser')
                    img_tag = soup.select_one('.product-image img') or soup.select_one('.product-page img')
                    if not img_tag:
                        img_tags = soup.find_all('img')
                        for tag in img_tags:
                            src = tag.get('src', '')
                            if norm_query.split()[0].lower() in normalize_article(src).lower() or 'product' in src.lower():
                                img_tag = tag
                                break
                    if img_tag and img_tag.get('src'):
                        img_url = img_tag['src']
                        if not img_url.startswith('http'):
                            img_url = urljoin(product_url, img_url)
                        caption = "🖼 Фото товара:"
                        await message.answer_photo(photo=img_url, caption=caption)
                        photo_sent = True
                    else:
                        await message.answer("⚠️ Фото для этого товара не найдено на странице.")
                except Exception as e:
                    await message.answer(f"❌ Ошибка при загрузке фото: {str(e)[:50]}")
            else:
                await message.answer("⚠️ URL товара не найден в данных.")

            # Получаем короткую ссылку
            short_url = await shorten_url_yandex_async(product_url) if product_url else None
            if not short_url or not short_url.startswith('http'):
                short_url = product_url

            # Получаем категорию
            category_name = product.get('category_name', 'Без категории')

            # Формируем текст
            text = (
                f"🛠️ Название: {hbold(product.get('_NAME_', 'Без названия'))}\n"
                f"🔖 Артикул: {hcode(product.get('_SKU_', ''))}\n"
                f"💰 Цена: {hbold(str(product.get('_PRICE_', 'Цена не указана')))} ₽\n"
                f"📦 В наличии: {hbold(str(product.get('_QUANTITY_', 0)))} шт.\n"
                f"📂 Категория: {hbold(category_name)}\n"
            )
            if short_url:
                text += f"🔗 {hlink('Посмотреть на сайте', short_url)}\n"

            logging.debug(f"Отправляемый текст: {text}")
            quantity_available = int(product.get('_QUANTITY_', 0))
            product_id = product.get('_ID_')
            await send_message_in_parts(
                message,
                text,
                reply_markup=get_product_keyboard(product_id, quantity_available),
                parse_mode='HTML'
            )
        await state.update_data(article_requested_once=True)
    else:
        logging.warning(f"Товар не найден для запроса: {raw_query}")
        await message.answer(
            f"❌ Товар с артикулом или названием '{raw_query}' (нормализован как '{norm_query}') не найден.",
            reply_markup=get_back_to_main_menu_keyboard()
        )
        await state.clear()

@dp.message(MultipleArticlesStates.waiting_for_file, F.document)
async def process_multiple_articles_file(message: types.Message, state: FSMContext):
    """Обработка Excel-файла с множеством артикулов"""
    user_id = message.from_user.id
    await message.answer("⏳ Обрабатываю файл, добавляю товары в корзину...", reply_markup=get_back_to_main_menu_keyboard())

    try:
        file_id = message.document.file_id
        file = await bot.get_file(file_id)
        file_content = await bot.download_file(file.file_path)
        raw_data = file_content.read()

        df = pd.read_excel(io.BytesIO(raw_data), dtype=str)

        if df.shape[1] < 3:
            await message.answer("❗ В файле должно быть минимум 3 столбца: Артикул, Название, Количество.", reply_markup=get_back_to_main_menu_keyboard())
            return

        rows = []
        total_sum = 0.0
        total_added_quantity = 0

        if user_id not in user_carts:
            user_carts[user_id] = {}

        for _, row in df.iterrows():
            try:
                quantity_str = str(row.iloc[2]).strip()
                if not quantity_str.isdigit():
                    continue
                quantity = int(quantity_str)

                row_dict = {
                    'Артикул': str(row.iloc[0]),
                    'Название': str(row.iloc[1]) if not pd.isna(row.iloc[1]) else ''
                }

                product = find_product_by_excel_row(row_dict, products)
                if product:
                    product_id = str(product.get('_ID_'))
                    price = parse_price(product.get('_PRICE_', '0'))
                    available = int(product.get('_QUANTITY_', 0))
                    name = row_dict['Название'] if row_dict['Название'] else product.get('_NAME_', 'Без названия')

                    quantity_to_add = min(quantity, available)

                    if product_id in user_carts[user_id]:
                        user_carts[user_id][product_id]['quantity'] += quantity_to_add
                    else:
                        user_carts[user_id][product_id] = {
                            'quantity': quantity_to_add,
                            'price': price,
                            'name': name
                        }

                    sum_price = price * quantity_to_add
                    total_sum += sum_price
                    total_added_quantity += quantity_to_add

                    rows.append({
                        "Артикул": row_dict['Артикул'],
                        "Название": name,
                        "Количество (запрошено)": quantity,
                        "Количество (добавлено)": quantity_to_add,
                        "Цена": price,
                        "Доступно": available,
                        "Сумма": sum_price,
                        "Статус": "Добавлено"
                    })
                else:
                    rows.append({
                        "Артикул": row_dict['Артикул'],
                        "Название": row_dict['Название'],
                        "Количество (запрошено)": quantity,
                        "Количество (добавлено)": 0,
                        "Цена": "Не найдено",
                        "Доступно": "Не найдено",
                        "Сумма": 0,
                        "Статус": "Не найден"
                    })

            except Exception as e:
                logging.exception(f"Ошибка при обработке строки: {e}")
                rows.append({
                    "Артикул": row_dict.get('Артикул', ''),
                    "Название": row_dict.get('Название', ''),
                    "Количество (запрошено)": quantity_str,
                    "Количество (добавлено)": 0,
                    "Цена": "Не найдено",
                    "Доступно": "Не найдено",
                    "Сумма": 0,
                    "Статус": f"Ошибка: {str(e)[:50]}"
                })

        if not rows:
            await message.answer("⚠️ В файле не найдено ни одного артикула или названия из базы.", reply_markup=get_back_to_main_menu_keyboard())
            await state.clear()
            return

        df_result = pd.DataFrame(rows)
        total_rows = len(df_result)
        num_sheets = math.ceil(total_rows / MAX_ROWS_PER_FILE)
        logging.info(f"Всего строк для результата: {total_rows}, листов будет: {num_sheets}")

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            for i in range(num_sheets):
                start = i * MAX_ROWS_PER_FILE
                end = min(start + MAX_ROWS_PER_FILE, total_rows)
                part_df = df_result.iloc[start:end]
                sheet_name = f'Результаты_{i+1}'
                part_df.to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets[sheet_name]
                worksheet.set_column('A:A', 20)
                worksheet.set_column('B:B', 40)
                worksheet.set_column('C:D', 18)
                worksheet.set_column('E:G', 15)
                worksheet.set_column('H:H', 15)

        output.seek(0)
        filename = "Результаты_поиска_в_нескольких_листах.xlsx"
        doc = BufferedInputFile(output.read(), filename=filename)
        await bot.send_document(chat_id=user_id, document=doc, caption=f"Результаты поиска ({num_sheets} листов)", reply_markup=get_back_to_main_menu_keyboard())

        await message.answer(f"✅ Добавлено товаров в корзину: {total_added_quantity} на сумму {total_sum:.2f} ₽", reply_markup=get_back_to_main_menu_keyboard())
        await show_cart_func(message, state)
        await state.clear()

    except Exception as e:
        logging.exception("Ошибка при обработке файла")
        await message.answer(f"❌ Ошибка при обработке файла: {e}", reply_markup=get_back_to_main_menu_keyboard())
        await state.clear()

@dp.message(F.text == "🛒 Корзина")
async def show_cart_func(message: types.Message, state: FSMContext):
    """Показ корзины"""
    user_id = message.from_user.id
    await state.clear()
    if user_id not in user_carts or not user_carts[user_id]:
        await message.answer("🛒 Ваша корзина пуста.", reply_markup=get_main_menu_keyboard())
        return
    await message.answer("⏳ Формирую файл с вашей корзиной…")
    cart_items = list(user_carts[user_id].items())
    product_ids = [str(pid) for pid, _ in cart_items]
    rows = []
    for product_id, product_info in cart_items:
        pid_str = str(product_id)
        product = products_by_id.get(pid_str, {})
        category_name = product.get('category_name', 'Без категории')
        long_url = product.get('_URL_', '')  # Используем исходный URL
        rows.append({
            "Артикул": pid_str,
            "Название": product_info.get('name', ''),
            "Категория": category_name,
            "Ссылка": long_url,  # Используем исходный URL без сокращения
            "Количество": product_info.get('quantity', 0),
            "Цена за шт.": product_info.get('price', 0),
            "Сумма": product_info.get('price', 0) * product_info.get('quantity', 0)
        })
    df = pd.DataFrame(rows)
    total_rows = len(df)
    num_sheets = math.ceil(total_rows / MAX_ROWS_PER_FILE)
    logging.info(f"Всего товаров: {total_rows}, листов будет: {num_sheets}")
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        for i in range(num_sheets):
            start = i * MAX_ROWS_PER_FILE
            end = min(start + MAX_ROWS_PER_FILE, total_rows)
            part_df = df.iloc[start:end]
            if part_df.empty:
                logging.info(f"Лист {i+1} пустой, пропускаем")
                continue
            sheet_name = f"Корзина_{i+1}"
            part_df.to_excel(writer, index=False, sheet_name=sheet_name)
            worksheet = writer.sheets[sheet_name]
            worksheet.set_column('A:A', 20)
            worksheet.set_column('B:B', 40)
            worksheet.set_column('C:C', 25)
            worksheet.set_column('D:D', 50)
            worksheet.set_column('E:E', 15)
            worksheet.set_column('F:F', 15)
            worksheet.set_column('G:G', 15)
    output.seek(0)
    file_name = "Корзина_вся_частями.xlsx"
    file = BufferedInputFile(output.read(), filename=file_name)
    await bot.send_document(chat_id=user_id, document=file, caption="Ваша корзина (несколько листов)")
    total_sum = sum(row["Сумма"] for row in rows)
    await message.answer(f"🛒 Итого: {total_sum:.2f} ₽", reply_markup=get_cart_keyboard())

@dp.callback_query(F.data.startswith("add_"))
async def add_to_cart(callback: types.CallbackQuery, state: FSMContext):
    """Добавление товара в корзину"""
    parts = callback.data.split("_")
    if len(parts) != 3:
        await callback.answer("❌ Ошибка данных, попробуйте снова.")
        return
    product_id, quantity_available_str = parts[1], parts[2]
    try:
        quantity_available = int(quantity_available_str)
    except ValueError:
        await callback.answer("❌ Ошибка данных, попробуйте снова.")
        return
    product = next((p for p in products if str(p['_ID_']) == product_id), None)
    if product:
        price = parse_price(product.get('_PRICE_', '0'))
        product_data = {
            'product_id': product_id,
            'quantity_available': quantity_available,
            'price': price,
            'name': product.get('_NAME_', 'Без названия')
        }
        await state.update_data(**product_data)
        await callback.message.answer(f"✏️ Введите количество (макс. {quantity_available} шт.):", reply_markup=get_back_to_main_menu_keyboard())
        await state.set_state(OrderQuantity.waiting_for_quantity)
    else:
        await callback.answer("❌ Товар не найден.")

@dp.message(OrderQuantity.waiting_for_quantity)
async def process_quantity(message: types.Message, state: FSMContext):
    """Обработка введенного количества"""
    data = await state.get_data()
    product_id = data.get('product_id')
    quantity_available = data.get('quantity_available')
    price = data.get('price')
    name = data.get('name')
    if not all([product_id, quantity_available, price, name]):
        await message.answer("❌ Произошла ошибка, попробуйте добавить товар заново.", reply_markup=get_back_to_main_menu_keyboard())
        await state.clear()
        return
    try:
        quantity = int(message.text)
    except ValueError:
        await message.answer("⚠️ Пожалуйста, введите число.", reply_markup=get_back_to_main_menu_keyboard())
        return
    if quantity <= 0 or quantity > quantity_available:
        await message.answer(f"⚠️ Некорректное количество. Введите от 1 до {quantity_available}:", reply_markup=get_back_to_main_menu_keyboard())
        return
    user_id = message.from_user.id
    if user_id not in user_carts:
        user_carts[user_id] = {}
    user_carts[user_id][product_id] = {
        'quantity': quantity,
        'price': price,
        'name': name
    }
    await message.answer(
        f"✅ Добавлено {quantity} шт. в корзину!\n\n"
        "Введите следующий артикул для поиска или выберите действие ниже.",
        reply_markup=get_cart_keyboard()
    )
    await state.set_state(UserStates.waiting_for_article_request)

@dp.message(F.text == "🗑 Очистить корзину")
async def clear_cart_func(message: types.Message, state: FSMContext):
    """Очистка корзины"""
    user_id = message.from_user.id
    if user_id in user_carts:
        user_carts[user_id].clear()
    await message.answer("🛒 Корзина очищена.", reply_markup=get_main_menu_keyboard())
    await state.clear()

@dp.message(F.text == "✅ Оформить заказ")
async def checkout_func(message: types.Message, state: FSMContext):
    """Оформление заказа"""
    user_id = message.from_user.id
    if user_id not in user_carts or not user_carts[user_id]:
        await message.answer(
            "🛒 Ваша корзина пуста. Добавьте товары перед оформлением заказа.",
            reply_markup=get_main_menu_keyboard()
        )
        await state.clear()
        return
    await message.answer(
        "📞 Введите ваш номер телефона для связи:",
        reply_markup=get_main_menu_keyboard()
    )
    await state.set_state(OrderStates.waiting_for_contact)

@dp.message(OrderStates.waiting_for_contact)
async def process_contact(message: types.Message, state: FSMContext):
    """Обработка номера телефона"""
    contact_raw = message.text.strip()
    digits = re.sub(r'\D', '', contact_raw)
    if len(digits) == 11 and digits.startswith('8'):
        formatted = '+7' + digits[1:]
    elif len(digits) == 11 and digits.startswith('7'):
        formatted = '+' + digits
    elif len(digits) == 10:
        formatted = '+7' + digits
    else:
        await message.answer("❌ Введите корректный номер телефона в формате +7XXXXXXXXXX или 8XXXXXXXXXX:")
        return
    await state.update_data(contact=formatted)
    await message.answer("📍 Введите адрес доставки:")
    await state.set_state(OrderStates.waiting_for_address)

@dp.message(OrderStates.waiting_for_address)
async def process_address(message: types.Message, state: FSMContext):
    """Обработка адреса доставки"""
    address = message.text.strip()
    data = await state.get_data()
    contact = data.get("contact", "Не указан")
    user_id = message.from_user.id
    current_time = datetime.now().strftime("%d.%m.%Y %H:%M")
    cart_items = user_carts.get(user_id, {})
    if not cart_items:
        await message.answer("🛒 Ваша корзина пуста.", reply_markup=get_main_menu_keyboard())
        await state.clear()
        return
    total_sum = sum(item['price'] * item['quantity'] for item in cart_items.values())
    order_data = {
        'user_id': user_id,
        'username': message.from_user.username or "Без username",
        'contact': contact,
        'address': address,
        'total_sum': total_sum,
        'items': list(cart_items.values()),
        'order_time': current_time
    }
    excel_file = await generate_excel(order_data)
    await send_client_confirmation(message, order_data, excel_file)
    await notify_order(order_data, excel_file)
    user_carts[user_id].clear()
    await state.clear()

async def generate_excel(order_data: dict) -> bytes:
    """Генерация Excel-файла для заказа"""
    data = []
    for item in order_data['items']:
        data.append({
            "Название": item['name'],
            "Количество": item['quantity'],
            "Цена за шт.": item['price'],
            "Сумма": item['price'] * item['quantity']
        })
    df = pd.DataFrame(data)
    total_row = pd.DataFrame([{
        "Название": "Итого",
        "Количество": "",
        "Цена за шт.": "",
        "Сумма": order_data['total_sum']
    }])
    df = pd.concat([df, total_row], ignore_index=True)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Заказ', index=False)
        writer.close()
    output.seek(0)
    return output.read()

async def send_client_confirmation(message: types.Message, order_data: dict, excel_file: bytes):
    """Отправка подтверждения клиенту"""
    order_summary = (
        "✅ <b>Заказ оформлен!</b>\n\n"
        f"📞 <b>Контакт:</b> {hd.quote(order_data['contact'])}\n"
        f"🏠 <b>Адрес:</b> {hd.quote(order_data['address'])}\n\n"
        f"💰 <b>Итого:</b> {order_data['total_sum']:.2f} ₽\n\n"
        "📄 Подробности заказа в прикреплённом файле."
    )
    await message.answer(
        order_summary,
        parse_mode="HTML",
        reply_markup=get_main_menu_keyboard()
    )
    await bot.send_document(
        chat_id=message.chat.id,
        document=BufferedInputFile(excel_file, filename="Заказ.xlsx")
    )
    contact_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="📩 Связаться с менеджером",
            url=f"https://t.me/{ADMIN_USERNAME}"
        )]
    ])
    await message.answer(
        "📢 Чтобы завершить оформление заказа, напишите менеджеру.",
        reply_markup=contact_keyboard
    )

async def notify_order(order_data: dict, excel_file: bytes):
    """Уведомление о заказе"""
    text = (
        "🚨 <b>Новый заказ!</b>\n\n"
        f"👤 <b>Клиент:</b> {order_data['username']}\n"
        f"📞 <b>Контакт:</b> <code>{hd.quote(order_data['contact'])}</code>\n"
        f"🏠 <b>Адрес:</b> {hd.quote(order_data['address'])}\n\n"
        f"💰 <b>Сумма:</b> {order_data['total_sum']:.2f} ₽\n"
        f"🕒 <b>Время:</b> {hd.quote(order_data['order_time'])}"
    )
    for admin_id in admin_ids:
        try:
            await bot.send_document(
                chat_id=admin_id,
                document=BufferedInputFile(
                    excel_file,
                    filename=f"Заказ_{order_data['user_id']}.xlsx"
                ),
                caption=text,
                parse_mode="HTML"
            )
        except Exception as e:
            logging.error(f"Ошибка отправки админу {admin_id}: {e}")
    if ORDER_CHANNEL:
        try:
            await bot.send_document(
                chat_id=ORDER_CHANNEL,
                document=BufferedInputFile(
                    excel_file,
                    filename=f"Заказ_{order_data['user_id']}.xlsx"
                ),
                caption=text,
                parse_mode="HTML"
            )
        except Exception as e:
            logging.error(f"Ошибка отправки в канал: {e}")
            await bot.send_message(
                admin_ids[0],
                f"⚠️ Ошибка отправки в канал: {str(e)[:300]}"
            )

async def main():
    """Основная функция запуска бота"""
    logging.info("Старт бота")
    await db.connect()
    await bot.delete_webhook(drop_pending_updates=True)
    try:
        await dp.start_polling(bot, skip_updates=True)
    except asyncio.CancelledError:
        logging.info("Polling отменён")
    finally:
        logging.info("Закрываем бота и базу данных")
        await bot.session.close()
        await db.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nБот остановлен вручную")
