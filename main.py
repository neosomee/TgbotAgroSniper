import asyncio
import math
import logging
import re
import pandas as pd
import chardet
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from aiogram import Bot, Dispatcher, F, types, Router
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
import io
from aiogram.enums.parse_mode import ParseMode
import idna
from aiogram.types.input_file import BufferedInputFile
from db import Database
import logging
import sys
from datetime import datetime
from aiogram.utils.markdown import html_decoration as hd 
import unicodedata
import aiohttp
from aiohttp_retry import RetryClient, ExponentialRetry
from urllib.parse import urlparse, quote
from concurrent.futures import ThreadPoolExecutor

executor = ThreadPoolExecutor(max_workers=5)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

API_TOKEN = 
ADMIN_USERNAME = '@lprost'
ORDER_CHANNEL = 

logging.basicConfig(level=logging.INFO)

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

MAX_ROWS_PER_FILE = 1000

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
    waiting_for_multiple_articles_file = State()
    

class MultipleArticlesStates(StatesGroup):
    waiting_for_file = State()

class OrderStates(StatesGroup):
    waiting_for_contact = State()
    waiting_for_address = State()

class AdminStates(StatesGroup):
    waiting_for_broadcast_content = State()
    waiting_for_categories = State()
    waiting_for_products = State()

db = Database()


admin_ids = [5056594883, 6521061663]

categories = []
products = []
products_by_id = {}
categories_dict = {}

user_carts = {}

BASE_URL = "https://xn--80aaijtwglegf.xn--p1ai/"


async def clear_cart_func(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if user_id in user_carts:
        user_carts[user_id].clear()
    await message.answer("рџ›’ РљРѕСЂР·РёРЅР° РѕС‡РёС‰РµРЅР°.", reply_markup=get_main_menu_keyboard())
    await state.clear()

async def checkout_func(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if user_id not in user_carts or not user_carts[user_id]:
        await message.answer(
            "рџ›’ Р’Р°С€Р° РєРѕСЂР·РёРЅР° РїСѓСЃС‚Р°. Р”РѕР±Р°РІСЊС‚Рµ С‚РѕРІР°СЂС‹ РїРµСЂРµРґ РѕС„РѕСЂРјР»РµРЅРёРµРј Р·Р°РєР°Р·Р°.",
            reply_markup=get_main_menu_keyboard()
        )
        await state.clear()
        return
    await message.answer(
        "рџ“ћ Р’РІРµРґРёС‚Рµ РІР°С€ РЅРѕРјРµСЂ С‚РµР»РµС„РѕРЅР° РґР»СЏ СЃРІСЏР·Рё:",
        reply_markup=get_main_menu_keyboard()
    )
    await state.set_state(OrderStates.waiting_for_contact)

async def show_cart_func(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    await state.clear()
    # Р’С‹Р·РѕРІ С‚РІРѕРµР№ С„СѓРЅРєС†РёРё РїРѕРєР°Р·Р° РєРѕСЂР·РёРЅС‹
    await show_cart(message)


def remove_keyboard():
    return ReplyKeyboardMarkup(keyboard=[], resize_keyboard=True)

def get_cart_confirmation_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="рџ›’ РџРµСЂРµР№С‚Рё РІ РєРѕСЂР·РёРЅСѓ")],
            [KeyboardButton(text="рџЏ  РћСЃРЅРѕРІРЅРѕРµ РјРµРЅСЋ")]
        ],
        resize_keyboard=True
    )

def get_main_menu_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="рџ”Ќ Р—Р°РїСЂРѕСЃ РѕРґРЅРѕРіРѕ Р°СЂС‚РёРєСѓР»Р°"),
                KeyboardButton(text="рџ“Љ РџСЂРѕСЃС‡С‘С‚ Excel СЃ Р°СЂС‚РёРєСѓР»Р°РјРё"),
            ],
            [
                KeyboardButton(text="рџ›’ РљРѕСЂР·РёРЅР°"),
                KeyboardButton(text="рџ‘ЁвЂЌрџ’» РЎРІСЏР·СЊ СЃ РїРѕРґРґРµСЂР¶РєРѕР№")
            ]
        ],
        resize_keyboard=True,
        input_field_placeholder="Р’С‹Р±РµСЂРёС‚Рµ РґРµР№СЃС‚РІРёРµ"
    )

def get_back_to_main_menu_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="рџЏ  РћСЃРЅРѕРІРЅРѕРµ РјРµРЅСЋ")]
        ],
        resize_keyboard=True
    )

def get_product_keyboard(product_id, quantity_available):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="рџ›’ Р”РѕР±Р°РІРёС‚СЊ РІ РєРѕСЂР·РёРЅСѓ",
            callback_data=f"add_{product_id}_{quantity_available}"
        )]
    ])


def get_cart_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="рџ—‘ РћС‡РёСЃС‚РёС‚СЊ РєРѕСЂР·РёРЅСѓ"), KeyboardButton(text="вњ… РћС„РѕСЂРјРёС‚СЊ Р·Р°РєР°Р·")],
            [KeyboardButton(text="рџЏ  РћСЃРЅРѕРІРЅРѕРµ РјРµРЅСЋ")]
        ],
        resize_keyboard=True
    )
    return keyboard

def shorten_url_yandex(long_url: str) -> str | None:
    """РЎРёРЅС…СЂРѕРЅРЅР°СЏ С„СѓРЅРєС†РёСЏ СЃРѕРєСЂР°С‰РµРЅРёСЏ СЃСЃС‹Р»РєРё С‡РµСЂРµР· clck.ru"""
    try:
        response = requests.get(f'https://clck.ru/--?url={long_url}')
        if response.status_code == 200:
            return response.text
        else:
            logging.error(f"РћС€РёР±РєР° РїСЂРё СЃРѕРєСЂР°С‰РµРЅРёРё СЃСЃС‹Р»РєРё С‡РµСЂРµР· РЇРЅРґРµРєСЃ.РљР»РёРє: {response.status_code}")
            return None
    except Exception as e:
        logging.exception("РћС€РёР±РєР° РїСЂРё РѕР±СЂР°С‰РµРЅРёРё Рє РЇРЅРґРµРєСЃ.РљР»РёРє")
        return None

async def shorten_url_yandex_async(long_url: str) -> str | None:
    """РђСЃРёРЅС…СЂРѕРЅРЅР°СЏ РѕР±С‘СЂС‚РєР° СЃ Р»РёРјРёС‚РѕРј 0.2 СЃРµРєСѓРЅРґС‹ РјРµР¶РґСѓ РІС‹Р·РѕРІР°РјРё"""
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(executor, shorten_url_yandex, long_url)
    await asyncio.sleep(0.2)  # Р—Р°РґРµСЂР¶РєР° 200 РјСЃ
    return result
    


def split_message(text, max_length=4096):
    parts = []
    while len(text) > max_length:
        split_pos = text.rfind('\n', 0, max_length)
        if split_pos == -1:
            split_pos = max_length
        parts.append(text[:split_pos])
        text = text[split_pos:]
    parts.append(text)
    return parts


SIMILAR_CHARS_MAP = {
    'Рђ': 'A', 'Р’': 'B', 'Р•': 'E', 'Рљ': 'K', 'Рњ': 'M', 'Рќ': 'H',
    'Рћ': 'O', 'Р ': 'P', 'РЎ': 'C', 'Рў': 'T', 'РЈ': 'Y', 'РҐ': 'X',
    'Р°': 'A', 'РІ': 'B', 'Рµ': 'E', 'Рє': 'K', 'Рј': 'M', 'РЅ': 'H',
    'Рѕ': 'O', 'СЂ': 'P', 'СЃ': 'C', 'С‚': 'T', 'Сѓ': 'Y', 'С…': 'X',
}

def normalize_article(article) -> str:
    """
    РЈРЅРёРІРµСЂСЃР°Р»СЊРЅР°СЏ РЅРѕСЂРјР°Р»РёР·Р°С†РёСЏ Р°СЂС‚РёРєСѓР»Р°:
    - РџСЂРёРІРµРґРµРЅРёРµ Рє СЃС‚СЂРѕРєРµ
    - Unicode РЅРѕСЂРјР°Р»РёР·Р°С†РёСЏ
    - РџСЂРёРІРµРґРµРЅРёРµ Рє РІРµСЂС…РЅРµРјСѓ СЂРµРіРёСЃС‚СЂСѓ
    - Р—Р°РјРµРЅР° РїРѕС…РѕР¶РёС… СЂСѓСЃСЃРєРёС… Р±СѓРєРІ РЅР° Р»Р°С‚РёРЅСЃРєРёРµ
    - РЈРґР°Р»РµРЅРёРµ РІСЃРµС… СЃРёРјРІРѕР»РѕРІ РєСЂРѕРјРµ Р»Р°С‚РёРЅСЃРєРёС… Р±СѓРєРІ Рё С†РёС„СЂ
    """
    if not article:
        return ''
    article = str(article)
    article = unicodedata.normalize('NFKC', article)
    article = article.upper()
    article = ''.join(SIMILAR_CHARS_MAP.get(ch, ch) for ch in article)
    article = re.sub(r'[^A-Z0-9]', '', article)
    return article

def get_product_image_url(product: dict) -> str | None:
    img = product.get('_IMAGE_') or ''
    if img:
        img = img.strip()
        if img.startswith('http'):
            return img
        else:
            return urljoin(BASE_URL, img)
    # РџРѕРїС‹С‚РєР° РІР·СЏС‚СЊ РёР· _IMAGES_ РёР»Рё _PRODUCT_IMAGES_
    for field in ['_IMAGES_', '_PRODUCT_IMAGES_']:
        imgs = product.get(field)
        if imgs:
            first_img = imgs.split(';')[0].strip()
            if first_img:
                if first_img.startswith('http'):
                    return first_img
                else:
                    return urljoin(BASE_URL, first_img)
    return None

async def get_image_url_from_product_page(url: str) -> str | None:
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        # РџСЂРёРјРµСЂ СЃРµР»РµРєС‚РѕСЂР°, Р°РґР°РїС‚РёСЂСѓР№ РїРѕРґ СЃРІРѕР№ СЃР°Р№С‚
        img_tag = soup.select_one('.product-image img') or soup.select_one('.product-page img')
        if img_tag and img_tag.get('src'):
            img_url = img_tag['src']
            if not img_url.startswith('http'):
                img_url = urljoin(url, img_url)
            return img_url
    except Exception as e:
        logging.warning(f"РћС€РёР±РєР° РїСЂРё РїР°СЂСЃРёРЅРіРµ С„РѕС‚Рѕ СЃ СЃР°Р№С‚Р° {url}: {e}")
    return None

def find_product_by_article(article_query: str, products: list, use_cache=True):
    norm_query = normalize_article(article_query)
    if use_cache:
        if not hasattr(find_product_by_article, '_cache'):
            find_product_by_article._cache = {}
            for p in products:
                norm_sku = normalize_article(p.get('_SKU_', ''))
                norm_name = normalize_article(p.get('_NAME_', ''))
                # РљСЌС€РёСЂСѓРµРј РїРѕ РѕР±РѕРёРј РєР»СЋС‡Р°Рј
                find_product_by_article._cache[norm_sku] = p
                find_product_by_article._cache[norm_name] = p
        return find_product_by_article._cache.get(norm_query)
    else:
        return next(
            (p for p in products if normalize_article(p.get('_SKU_', '')) == norm_query or normalize_article(p.get('_NAME_', '')) == norm_query),
            None
        )



def clear_find_product_cache():
    """РћС‡РёСЃС‚РёС‚СЊ РєСЌС€ РїРѕРёСЃРєР°, РµСЃР»Рё РґР°РЅРЅС‹Рµ С‚РѕРІР°СЂРѕРІ РѕР±РЅРѕРІРёР»РёСЃСЊ."""
    if hasattr(find_product_by_article, '_cache'):
        del find_product_by_article._cache

def parse_price(price_str):
    try:
        price_clean = str(price_str).replace(' ', '').replace(',', '.')
        return float(price_clean)
    except:
        return 0.0

def normalize_sku(sku: str):
    return str(sku).replace('.', '').strip()

def format_product_info(product, short_url=None, sku=None, category=None) -> str:
    if sku is None:
        sku = product.get('_SKU_', '')
    if isinstance(sku, float) and str(sku).lower() == 'nan':
        sku = ''

    name = product.get('_NAME_', 'Р‘РµР· РЅР°Р·РІР°РЅРёСЏ')
    price = product.get('_PRICE_', 'Р¦РµРЅР° РЅРµ СѓРєР°Р·Р°РЅР°')
    quantity = product.get('_QUANTITY_', 0)

    try:
        price_str = f"{float(price):.2f} в‚Ѕ"
    except (ValueError, TypeError):
        price_str = str(price)

    text = (
        f"рџ› пёЏ *РќР°Р·РІР°РЅРёРµ:* {name}\n"
        f"рџ”– *РђСЂС‚РёРєСѓР»:* {sku}\n"
        f"рџ’° *Р¦РµРЅР°:* {price_str}\n"
        f"рџ“¦ *Р’ РЅР°Р»РёС‡РёРё:* {quantity} С€С‚.\n"
    )
    if category:
        text += f"рџ“‚ *РљР°С‚РµРіРѕСЂРёСЏ:* {category}\n"
    if short_url:
        text += f"рџ”— *РЎСЃС‹Р»РєР°:* {short_url}\n"

    return text



async def send_message_in_parts(message: types.Message, text: str, **kwargs):
    for part in split_message(text):
        await message.answer(part, **kwargs)


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user = message.from_user
    # Р”РѕР±Р°РІР»СЏРµРј РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РІ Р±Р°Р·Сѓ (РµСЃР»Рё РµРіРѕ С‚Р°Рј РЅРµС‚)
    await db.add_user(
        user_id=user.id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
    )
    original_url = "https://Р°РіСЂРѕСЃРЅР°Р№РїРµСЂ.СЂС„/image/catalog/logoagro3.png"
    punycode_domain = idna.encode("Р°РіСЂРѕСЃРЅР°Р№РїРµСЂ.СЂС„").decode()
    photo_url = original_url.replace("Р°РіСЂРѕСЃРЅР°Р№РїРµСЂ.СЂС„", punycode_domain)

    caption = (
        "рџ‘‹ РџСЂРёРІРµС‚! Р”РѕР±СЂРѕ РїРѕР¶Р°Р»РѕРІР°С‚СЊ РІ РЅР°С€ РђРіСЂРѕСЃРЅР°Р№РїРµСЂ Р±РѕС‚.\n"
        "РЎР°Р№С‚: РђРіСЂРѕСЃРЅР°Р№РїРµСЂ.СЂС„\n\n"
        "Р’РѕС‚ С‡С‚Рѕ С‚С‹ РјРѕР¶РµС€СЊ СЃРґРµР»Р°С‚СЊ:\n"
        "1пёЏвѓЈ *рџ”Ќ Р—Р°РїСЂРѕСЃ РѕРґРЅРѕРіРѕ Р°СЂС‚РёРєСѓР»Р°* - РІРІРµРґРё Р°СЂС‚РёРєСѓР», С‡С‚РѕР±С‹ РїРѕР»СѓС‡РёС‚СЊ РёРЅС„РѕСЂРјР°С†РёСЋ Рё С„РѕС‚Рѕ С‚РѕРІР°СЂР°.\n"
        "2пёЏвѓЈ *рџ“Љ РџСЂРѕСЃС‡С‘С‚ Excel СЃ Р°СЂС‚РёРєСѓР»Р°РјРё* - РѕС‚РїСЂР°РІСЊ Excel-С„Р°Р№Р» СЃ Р°СЂС‚РёРєСѓР»Р°РјРё Рё РєРѕР»РёС‡РµСЃС‚РІРѕРј, Рё СЏ СЃСЂР°Р·Сѓ РґРѕР±Р°РІР»СЋ С‚РѕРІР°СЂС‹ РІ РєРѕСЂР·РёРЅСѓ.\n"
        "3пёЏвѓЈ *рџ›’ РљРѕСЂР·РёРЅР°* - Р·РґРµСЃСЊ С‚С‹ РјРѕР¶РµС€СЊ РїРѕСЃРјРѕС‚СЂРµС‚СЊ РґРѕР±Р°РІР»РµРЅРЅС‹Рµ С‚РѕРІР°СЂС‹, РёР·РјРµРЅРёС‚СЊ РєРѕР»РёС‡РµСЃС‚РІРѕ РёР»Рё РѕС„РѕСЂРјРёС‚СЊ Р·Р°РєР°Р·.\n"
        "4пёЏвѓЈ *рџ‘ЁвЂЌрџ’» РЎРІСЏР·СЊ СЃ РїРѕРґРґРµСЂР¶РєРѕР№* - РєРѕРЅС‚Р°РєС‚С‹ РјРµРЅРµРґР¶РµСЂР°, РµСЃР»Рё РЅСѓР¶РЅР° РїРѕРјРѕС‰СЊ.\n\n"
        "рџ”№ РџРѕСЃР»Рµ РєР°Р¶РґРѕРіРѕ РґРµР№СЃС‚РІРёСЏ Сѓ С‚РµР±СЏ Р±СѓРґРµС‚ РєРЅРѕРїРєР° *рџЏ  РћСЃРЅРѕРІРЅРѕРµ РјРµРЅСЋ* РґР»СЏ Р±С‹СЃС‚СЂРѕРіРѕ РІРѕР·РІСЂР°С‚Р° СЃСЋРґР°.\n"
        "рџ”№ Р§С‚РѕР±С‹ РґРѕР±Р°РІРёС‚СЊ С‚РѕРІР°СЂ РІ РєРѕСЂР·РёРЅСѓ, РїРѕСЃР»Рµ Р·Р°РїСЂРѕСЃР° Р°СЂС‚РёРєСѓР»Р° РЅР°Р¶РјРё РЅР° РєРЅРѕРїРєСѓ \"рџ›’ Р”РѕР±Р°РІРёС‚СЊ РІ РєРѕСЂР·РёРЅСѓ\" Рё СѓРєР°Р¶Рё РєРѕР»РёС‡РµСЃС‚РІРѕ.\n"
        "рџ”№ Р”Р»СЏ РѕС„РѕСЂРјР»РµРЅРёСЏ Р·Р°РєР°Р·Р° РїРµСЂРµР№РґРё РІ РєРѕСЂР·РёРЅСѓ Рё СЃР»РµРґСѓР№ РёРЅСЃС‚СЂСѓРєС†РёСЏРј.\n\n"
        "Р•СЃР»Рё РІРѕР·РЅРёРєРЅСѓС‚ РІРѕРїСЂРѕСЃС‹ - РїРёС€Рё РІ СЂР°Р·РґРµР» СЃРІСЏР·Рё СЃ РїРѕРґРґРµСЂР¶РєРѕР№.\n\n"
        "Р–РµР»Р°РµРј РїСЂРёСЏС‚РЅС‹С… РїРѕРєСѓРїРѕРє! рџ›ЌпёЏ"
    )

    await message.answer_photo(
        photo=photo_url,
        caption=caption,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_main_menu_keyboard()
    )

@dp.message(F.text == "рџЏ  РћСЃРЅРѕРІРЅРѕРµ РјРµРЅСЋ")
async def back_to_main_menu(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "рџ‘‹ Р“Р»Р°РІРЅРѕРµ РјРµРЅСЋ. Р’С‹Р±РµСЂРёС‚Рµ РґРµР№СЃС‚РІРёРµ:",
        reply_markup=get_main_menu_keyboard()
    )

def get_support_inline_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="РќР°РїРёСЃР°С‚СЊ РјРµРЅРµРґР¶РµСЂСѓ",
                url="https://t.me/zucman61"  
            )
        ]
    ])

@dp.message(F.text == "рџ‘ЁвЂЌрџ’» РЎРІСЏР·СЊ СЃ РїРѕРґРґРµСЂР¶РєРѕР№")
async def contact_support(message: types.Message):
    text = (
        "рџ“ћ *Р”РёСЂРµРєС‚РѕСЂ РћРћРћ РђРіСЂРѕСЃРЅР°Р№РїРµСЂ:*\n Р®СЂРёР№ РњРѕСЂРѕР·\n"
        "рџ“§ *Р­Р»РµРєС‚СЂРѕРЅРЅР°СЏ РїРѕС‡С‚Р°:* agrosnaiper@yandex.ru\n"
        "рџ“± *РўРµР»РµС„РѕРЅ:* +7 (928) 279-05-29\n\n"
        "РЎР°Р№С‚: РђРіСЂРѕСЃРЅР°Р№РїРµСЂ.СЂС„"
    )
    await send_message_in_parts(
        message,
        text,
        parse_mode="Markdown",
        reply_markup=get_support_inline_keyboard()
    )

def get_admin_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="рџ“¦ Р—Р°РіСЂСѓР·РёС‚СЊ РїСЂРѕРґСѓРєС‚С‹")],
            [KeyboardButton(text="рџ“Љ РЎС‚Р°С‚РёСЃС‚РёРєР°")],
            [KeyboardButton(text="рџ“ў Р Р°СЃСЃС‹Р»РєР° СЃРѕРѕР±С‰РµРЅРёР№")],
            [KeyboardButton(text="рџЏ  Р’С‹С…РѕРґ РІ РѕСЃРЅРѕРІРЅРѕРµ РјРµРЅСЋ")]
        ],
        resize_keyboard=True,
        input_field_placeholder="Р’С‹Р±РµСЂРёС‚Рµ РґРµР№СЃС‚РІРёРµ"
    )

@dp.message(Command("admin"))
async def admin_panel(message: types.Message, state: FSMContext):
    if message.from_user.id in admin_ids:
        await state.set_state(None)  # РЎР±СЂР°СЃС‹РІР°РµРј РІСЃРµ СЃРѕСЃС‚РѕСЏРЅРёСЏ
        await message.answer(
            "рџ› пёЏ РђРґРјРёРЅ-РїР°РЅРµР»СЊ. Р§С‚Рѕ С…РѕС‚РёС‚Рµ СЃРґРµР»Р°С‚СЊ?", 
            reply_markup=get_admin_keyboard()
        )
    else:
        await message.answer("вќЊ РЈ РІР°СЃ РЅРµС‚ РїСЂР°РІ РґР»СЏ РґРѕСЃС‚СѓРїР° Рє Р°РґРјРёРЅ-РїР°РЅРµР»Рё.")

@dp.message(F.text == "рџЏ  Р’С‹С…РѕРґ РІ РѕСЃРЅРѕРІРЅРѕРµ РјРµРЅСЋ")
async def exit_admin_panel(message: types.Message, state: FSMContext):
    if message.from_user.id in admin_ids:
        await state.clear()
        await message.answer(
            "вњ… Р’С‹ РІС‹С€Р»Рё РёР· Р°РґРјРёРЅ-РїР°РЅРµР»Рё",
            reply_markup=get_main_menu_keyboard()
        )
    else:
        await message.answer("вќЊ РЈ РІР°СЃ РЅРµС‚ РїСЂР°РІ РґР»СЏ СЌС‚РѕРіРѕ РґРµР№СЃС‚РІРёСЏ.")

# РњРѕРґРёС„РёС†РёСЂРѕРІР°РЅРЅС‹Рµ С…СЌРЅРґР»РµСЂС‹ РґР»СЏ Р°РґРјРёРЅ-РґРµР№СЃС‚РІРёР№
@dp.message(F.text == "рџ“ў Р Р°СЃСЃС‹Р»РєР° СЃРѕРѕР±С‰РµРЅРёР№")
async def start_broadcast(message: types.Message, state: FSMContext):
    if message.from_user.id not in admin_ids:
        await message.answer("вќЊ РЈ РІР°СЃ РЅРµС‚ РїСЂР°РІ РґР»СЏ СЌС‚РѕРіРѕ РґРµР№СЃС‚РІРёСЏ.")
        return
    
    await message.answer(
        "вњ‰пёЏ РћС‚РїСЂР°РІСЊС‚Рµ СЃРѕРѕР±С‰РµРЅРёРµ РґР»СЏ СЂР°СЃСЃС‹Р»РєРё...",
        reply_markup=remove_keyboard()  
    )
    await state.set_state(AdminStates.waiting_for_broadcast_content)

@dp.message(AdminStates.waiting_for_broadcast_content)
async def process_broadcast_content(message: types.Message, state: FSMContext):
    if message.from_user.id not in admin_ids:
        await message.answer("вќЊ РЈ РІР°СЃ РЅРµС‚ РїСЂР°РІ РґР»СЏ СЌС‚РѕРіРѕ РґРµР№СЃС‚РІРёСЏ.")
        await state.clear()
        return

    await message.answer("вЏі РќР°С‡РёРЅР°СЋ СЂР°СЃСЃС‹Р»РєСѓ...")

    users = await db.get_all_users()
    success_count = 0
    fail_count = 0

    # РћРїСЂРµРґРµР»СЏРµРј С‚РёРї СЃРѕРѕР±С‰РµРЅРёСЏ Рё РїР°СЂР°РјРµС‚СЂС‹ РѕС‚РїСЂР°РІРєРё
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
        await message.answer("вќЊ РќРµРїРѕРґРґРµСЂР¶РёРІР°РµРјС‹Р№ С‚РёРї СЃРѕРѕР±С‰РµРЅРёСЏ. РџРѕР¶Р°Р»СѓР№СЃС‚Р°, РѕС‚РїСЂР°РІСЊС‚Рµ С‚РµРєСЃС‚, С„РѕС‚Рѕ РёР»Рё РІРёРґРµРѕ.")
        await state.clear()
        return

    # Р’СЃРїРѕРјРѕРіР°С‚РµР»СЊРЅР°СЏ С„СѓРЅРєС†РёСЏ РґР»СЏ РѕС‚РїСЂР°РІРєРё РѕРґРЅРѕРјСѓ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ СЃ РѕР±СЂР°Р±РѕС‚РєРѕР№ РѕС€РёР±РѕРє
    async def send_to_user(user_id):
        nonlocal success_count, fail_count
        try:
            await send_func(chat_id=user_id, **send_kwargs)
            success_count += 1
            await asyncio.sleep(0.05)  # РўР°Р№РјР°СѓС‚ РјРµР¶РґСѓ РѕС‚РїСЂР°РІРєР°РјРё
        except Exception as e:
            print(f"РћС€РёР±РєР° РїСЂРё РѕС‚РїСЂР°РІРєРµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ {user_id}: {e}")
            fail_count += 1

    # РЎРѕР·РґР°РµРј Р·Р°РґР°С‡Рё РґР»СЏ РїР°СЂР°Р»Р»РµР»СЊРЅРѕР№ РѕС‚РїСЂР°РІРєРё
    tasks = [send_to_user(user_id) for user_id, _ in users]

    # Р—Р°РїСѓСЃРєР°РµРј РІСЃРµ Р·Р°РґР°С‡Рё РїР°СЂР°Р»Р»РµР»СЊРЅРѕ
    await asyncio.gather(*tasks)

    await message.answer(
        f"вњ… Р Р°СЃСЃС‹Р»РєР° Р·Р°РІРµСЂС€РµРЅР°!\nРЈСЃРїРµС€РЅРѕ: {success_count}\nРќРµ СѓРґР°Р»РѕСЃСЊ: {fail_count}",
        reply_markup=get_admin_keyboard()
    )
    await state.set_state(None)



@dp.message(F.text == "рџ“¦ Р—Р°РіСЂСѓР·РёС‚СЊ РїСЂРѕРґСѓРєС‚С‹")
async def load_products(message: types.Message, state: FSMContext):
    if message.from_user.id in admin_ids:
        await message.answer("рџ“Ѓ РћС‚РїСЂР°РІСЊС‚Рµ CSV-С„Р°Р№Р» СЃ РїСЂРѕРґСѓРєС‚Р°РјРё.", reply_markup=get_back_to_main_menu_keyboard())
        await state.set_state(UploadStates.waiting_for_products)
    else:
        await message.answer("вќЊ РЈ РІР°СЃ РЅРµС‚ РїСЂР°РІ РґР»СЏ СЌС‚РѕРіРѕ РґРµР№СЃС‚РІРёСЏ.", reply_markup=get_back_to_main_menu_keyboard())

@dp.message(F.text == "рџ“Љ РЎС‚Р°С‚РёСЃС‚РёРєР°")
async def show_stats(message: types.Message):
    if message.from_user.id not in admin_ids:
        await message.answer("вќЊ РЈ РІР°СЃ РЅРµС‚ РїСЂР°РІ РґР»СЏ СЌС‚РѕРіРѕ РґРµР№СЃС‚РІРёСЏ.", reply_markup=get_back_to_main_menu_keyboard())
        return

    users = await db.get_all_users()
    users_count = len(users)

    await message.answer(
        f"рџ“€ РљРѕР»РёС‡РµСЃС‚РІРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№ РІ Р±РѕС‚Рµ: {users_count}\n"
        f"рџ“€ Р—Р°РіСЂСѓР¶РµРЅРѕ РїСЂРѕРґСѓРєС‚РѕРІ: {len(products)}",
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

        # РўРµРїРµСЂСЊ РїСЂРѕСЃС‚Рѕ СЃРѕС…СЂР°РЅСЏРµРј РЅР°Р·РІР°РЅРёРµ РєР°С‚РµРіРѕСЂРёРё РёР· РїРѕР»СЏ _CATEGORY_
        for product in products:
            product['category_name'] = product.get('_CATEGORY_', 'Р‘РµР· РєР°С‚РµРіРѕСЂРёРё')

        products_by_id = {str(p['_ID_']): p for p in products if '_ID_' in p}

        await message.answer(f"вњ… Р—Р°РіСЂСѓР¶РµРЅРѕ {len(products)} С‚РѕРІР°СЂРѕРІ.", reply_markup=get_admin_keyboard())
        await state.clear()

    except Exception as e:
        logging.exception("РћС€РёР±РєР° РїСЂРё РѕР±СЂР°Р±РѕС‚РєРµ С„Р°Р№Р»Р° С‚РѕРІР°СЂРѕРІ")
        await message.answer(f"вќЊ РћС€РёР±РєР° РїСЂРё РѕР±СЂР°Р±РѕС‚РєРµ С„Р°Р№Р»Р° С‚РѕРІР°СЂРѕРІ: {e}", reply_markup=get_admin_keyboard())
        await state.clear()


@dp.message(F.text == "рџ”Ќ Р—Р°РїСЂРѕСЃ РѕРґРЅРѕРіРѕ Р°СЂС‚РёРєСѓР»Р°")
async def start_single_article(message: types.Message, state: FSMContext):
    await message.answer("вњЏпёЏ Р’РІРµРґРёС‚Рµ Р°СЂС‚РёРєСѓР» РґР»СЏ РїРѕРёСЃРєР° РёРЅС„РѕСЂРјР°С†РёРё Рё С„РѕС‚Рѕ С‚РѕРІР°СЂР°:", reply_markup=get_back_to_main_menu_keyboard())
    await state.set_state(UserStates.waiting_for_article_request)

class UserStates(StatesGroup):
    waiting_for_article_request = State()
    article_requested_once = State()  # РЅРѕРІРѕРµ СЃРѕСЃС‚РѕСЏРЅРёРµ - РїРѕСЃР»Рµ РїРµСЂРІРѕРіРѕ Р·Р°РїСЂРѕСЃР°

@dp.message(UserStates.waiting_for_article_request)
async def handle_article_request(message: types.Message, state: FSMContext):
    print(f"handle_article_request РІС‹Р·РІР°РЅ РґР»СЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ {message.from_user.id}")
    text = message.text.strip()

    if text == "рџ—‘ РћС‡РёСЃС‚РёС‚СЊ РєРѕСЂР·РёРЅСѓ":
        await clear_cart(message)
        await state.clear()
        return

    if text == "вњ… РћС„РѕСЂРјРёС‚СЊ Р·Р°РєР°Р·":
        await checkout(message, state)
        return

    if text == "рџ›’ РљРѕСЂР·РёРЅР°":
        await show_cart(message)
        await state.clear()
        return

    if text == "рџЏ  РћСЃРЅРѕРІРЅРѕРµ РјРµРЅСЋ":
        await message.answer("рџЏ  Р“Р»Р°РІРЅРѕРµ РјРµРЅСЋ", reply_markup=get_main_menu_keyboard())
        await state.clear()
        return

    raw_query = message.text.strip()
    norm_query = normalize_article(raw_query)

    product = None
    for p in products:
        sku = p.get('_SKU_', '')
        name = p.get('_NAME_', '')
        norm_sku = normalize_article(sku)
        norm_name = normalize_article(name)

        if norm_sku == norm_query or norm_query in norm_name:
            product = p
            break

    if product:
        product_url = product.get('_URL_')
        photo_sent = False

        # РџРѕРїС‹С‚РєР° РѕС‚РїСЂР°РІРёС‚СЊ С„РѕС‚Рѕ
        if product_url:
            try:
                response = requests.get(product_url)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')

                img_tag = soup.select_one('.product-image img') or soup.select_one('.product-page img')

                if not img_tag:
                    img_tags = soup.find_all('img')
                    for tag in img_tags:
                        src = tag.get('src', '')
                        if norm_query in normalize_article(src) or 'product' in src.lower():
                            img_tag = tag
                            break

                if img_tag and img_tag.get('src'):
                    img_url = img_tag['src']
                    if not img_url.startswith('http'):
                        img_url = urljoin(product_url, img_url)

                    caption = f"рџ–ј Р¤РѕС‚Рѕ С‚РѕРІР°СЂР°:"
                    await message.answer_photo(photo=img_url, caption=caption)
                    photo_sent = True
                else:
                    await message.answer("вљ пёЏ Р¤РѕС‚Рѕ РґР»СЏ СЌС‚РѕРіРѕ С‚РѕРІР°СЂР° РЅРµ РЅР°Р№РґРµРЅРѕ РЅР° СЃС‚СЂР°РЅРёС†Рµ.")
            except Exception as e:
                await message.answer(f"вќЊ РћС€РёР±РєР° РїСЂРё Р·Р°РіСЂСѓР·РєРµ С„РѕС‚Рѕ: {str(e)[:50]}")
        else:
            await message.answer("вљ пёЏ URL С‚РѕРІР°СЂР° РЅРµ РЅР°Р№РґРµРЅ РІ РґР°РЅРЅС‹С….")

        # РџРѕР»СѓС‡Р°РµРј РєРѕСЂРѕС‚РєСѓСЋ СЃСЃС‹Р»РєСѓ С‡РµСЂРµР· aiohttp СЃРµСЃСЃРёСЋ
        short_url = None
        if product_url:
            short_url = await shorten_url_yandex_async(product_url)
            if not short_url or not short_url.startswith('http'):
                short_url = product_url  # fallback РЅР° РїРѕР»РЅСѓСЋ СЃСЃС‹Р»РєСѓ

        # РџРѕР»СѓС‡Р°РµРј РєР°С‚РµРіРѕСЂРёСЋ
        category_name = product.get('category_name', 'Р‘РµР· РєР°С‚РµРіРѕСЂРёРё')

        # Р¤РѕСЂРјРёСЂСѓРµРј С‚РµРєСЃС‚ СЃ РєР°С‚РµРіРѕСЂРёРµР№ Рё РєРѕСЂРѕС‚РєРѕР№ СЃСЃС‹Р»РєРѕР№
        text = format_product_info(product, short_url=short_url, category=category_name)

        quantity_available = int(product.get('_QUANTITY_', 0))
        product_id = product.get('_ID_')
        await send_message_in_parts(
            message,
            text,
            reply_markup=get_product_keyboard(product_id, quantity_available),
            parse_mode='Markdown'
        )
        await state.update_data(article_requested_once=True)
    else:
        await message.answer(
            f"вќЊ РўРѕРІР°СЂ СЃ Р°СЂС‚РёРєСѓР»РѕРј РёР»Рё РЅР°Р·РІР°РЅРёРµРј '{raw_query}' РЅРµ РЅР°Р№РґРµРЅ.",
            reply_markup=get_back_to_main_menu_keyboard()
        )
        await state.clear()




@dp.message(MultipleArticlesStates.waiting_for_file, F.document)
async def process_multiple_articles_file(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    await message.answer("вЏі РћР±СЂР°Р±Р°С‚С‹РІР°СЋ С„Р°Р№Р», РґРѕР±Р°РІР»СЏСЋ С‚РѕРІР°СЂС‹ РІ РєРѕСЂР·РёРЅСѓ...", reply_markup=get_back_to_main_menu_keyboard())

    try:
        file_id = message.document.file_id
        file = await bot.get_file(file_id)
        file_content = await bot.download_file(file.file_path)
        raw_data = file_content.read()

        df = pd.read_excel(io.BytesIO(raw_data), dtype=str)

        if df.shape[1] < 3:
            await message.answer("вќ— Р’ С„Р°Р№Р»Рµ РґРѕР»Р¶РЅРѕ Р±С‹С‚СЊ РјРёРЅРёРјСѓРј 3 СЃС‚РѕР»Р±С†Р°: РђСЂС‚РёРєСѓР», РќР°Р·РІР°РЅРёРµ, РљРѕР»РёС‡РµСЃС‚РІРѕ.", reply_markup=get_back_to_main_menu_keyboard())
            return

        rows = []
        total_sum = 0.0
        total_added_quantity = 0

        if user_id not in user_carts:
            user_carts[user_id] = {}

        for _, row in df.iterrows():
            try:
                sku = normalize_sku(str(row.iloc[0]))
                file_name = str(row.iloc[1]).strip() if not pd.isna(row.iloc[1]) else ''
                quantity_str = str(row.iloc[2]).strip()

                if not sku or not quantity_str.isdigit():
                    continue

                quantity = int(quantity_str)

                product = next((p for p in products if normalize_sku(p.get('_SKU_', '')) == sku), None)
                if product:
                    product_id = str(product.get('_ID_'))
                    price = parse_price(product.get('_PRICE_', '0'))
                    available = int(product.get('_QUANTITY_', 0))
                    name = file_name if file_name else product.get('_NAME_', 'Р‘РµР· РЅР°Р·РІР°РЅРёСЏ')

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
                        "РђСЂС‚РёРєСѓР»": sku,
                        "РќР°Р·РІР°РЅРёРµ": name,
                        "РљРѕР»РёС‡РµСЃС‚РІРѕ (Р·Р°РїСЂРѕС€РµРЅРѕ)": quantity,
                        "РљРѕР»РёС‡РµСЃС‚РІРѕ (РґРѕР±Р°РІР»РµРЅРѕ)": quantity_to_add,
                        "Р¦РµРЅР°": price,
                        "Р”РѕСЃС‚СѓРїРЅРѕ": available,
                        "РЎСѓРјРјР°": sum_price,
                        "РЎС‚Р°С‚СѓСЃ": "Р”РѕР±Р°РІР»РµРЅРѕ"
                    })
                else:
                    rows.append({
                        "РђСЂС‚РёРєСѓР»": sku,
                        "РќР°Р·РІР°РЅРёРµ": file_name,
                        "РљРѕР»РёС‡РµСЃС‚РІРѕ (Р·Р°РїСЂРѕС€РµРЅРѕ)": quantity,
                        "РљРѕР»РёС‡РµСЃС‚РІРѕ (РґРѕР±Р°РІР»РµРЅРѕ)": 0,
                        "Р¦РµРЅР°": "РќРµ РЅР°Р№РґРµРЅРѕ",
                        "Р”РѕСЃС‚СѓРїРЅРѕ": "РќРµ РЅР°Р№РґРµРЅРѕ",
                        "РЎСѓРјРјР°": 0,
                        "РЎС‚Р°С‚СѓСЃ": "РќРµ РЅР°Р№РґРµРЅ"
                    })

            except Exception:
                logging.exception("РћС€РёР±РєР° РїСЂРё РѕР±СЂР°Р±РѕС‚РєРµ СЃС‚СЂРѕРєРё")

        if not rows:
            await message.answer("вљ пёЏ Р’ С„Р°Р№Р»Рµ РЅРµ РЅР°Р№РґРµРЅРѕ РЅРё РѕРґРЅРѕРіРѕ Р°СЂС‚РёРєСѓР»Р° РёР· Р±Р°Р·С‹.", reply_markup=get_back_to_main_menu_keyboard())
            await state.clear()
            return

        # Р¤РѕСЂРјРёСЂСѓРµРј Excel СЃ СЂРµР·СѓР»СЊС‚Р°С‚Р°РјРё РѕР±СЂР°Р±РѕС‚РєРё С„Р°Р№Р»Р°
        df_result = pd.DataFrame(rows)
        total_rows = len(df_result)
        num_sheets = math.ceil(total_rows / MAX_ROWS_PER_FILE)
        logging.info(f"Р’СЃРµРіРѕ СЃС‚СЂРѕРє РґР»СЏ СЂРµР·СѓР»СЊС‚Р°С‚Р°: {total_rows}, Р»РёСЃС‚РѕРІ Р±СѓРґРµС‚: {num_sheets}")

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            for i in range(num_sheets):
                start = i * MAX_ROWS_PER_FILE
                end = min(start + MAX_ROWS_PER_FILE, total_rows)
                part_df = df_result.iloc[start:end]
                sheet_name = f'Р РµР·СѓР»СЊС‚Р°С‚С‹_{i+1}'
                part_df.to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets[sheet_name]
                worksheet.set_column('A:A', 20)
                worksheet.set_column('B:B', 40)
                worksheet.set_column('C:D', 18)
                worksheet.set_column('E:G', 15)
                worksheet.set_column('H:H', 15)

        output.seek(0)
        filename = "Р РµР·СѓР»СЊС‚Р°С‚С‹_РїРѕРёСЃРєР°_РІ_РЅРµСЃРєРѕР»СЊРєРёС…_Р»РёСЃС‚Р°С….xlsx"
        doc = BufferedInputFile(output.read(), filename=filename)
        await bot.send_document(chat_id=user_id, document=doc, caption=f"Р РµР·СѓР»СЊС‚Р°С‚С‹ РїРѕРёСЃРєР° ({num_sheets} Р»РёСЃС‚РѕРІ)", reply_markup=get_back_to_main_menu_keyboard())

        await message.answer(f"вњ… Р”РѕР±Р°РІР»РµРЅРѕ С‚РѕРІР°СЂРѕРІ РІ РєРѕСЂР·РёРЅСѓ: {total_added_quantity} РЅР° СЃСѓРјРјСѓ {total_sum:.2f} в‚Ѕ", reply_markup=get_back_to_main_menu_keyboard())
        await show_cart(message)  # РџРѕРєР°Р·С‹РІР°РµРј РєРѕСЂР·РёРЅСѓ СЃСЂР°Р·Сѓ РїРѕСЃР»Рµ Р·Р°РіСЂСѓР·РєРё С„Р°Р№Р»Р°
        await state.clear()

    except Exception as e:
        logging.exception("РћС€РёР±РєР° РїСЂРё РѕР±СЂР°Р±РѕС‚РєРµ С„Р°Р№Р»Р°")
        await message.answer(f"вќЊ РћС€РёР±РєР° РїСЂРё РѕР±СЂР°Р±РѕС‚РєРµ С„Р°Р№Р»Р°: {e}", reply_markup=get_back_to_main_menu_keyboard())
        await state.clear()

# РџРѕРєР°Р· РєРѕСЂР·РёРЅС‹ СЃ СЃРѕРєСЂР°С‰РµРЅРёРµРј СЃСЃС‹Р»РѕРє Рё С„РѕСЂРјРёСЂРѕРІР°РЅРёРµРј Excel
@dp.message(F.text == "рџ›’ РљРѕСЂР·РёРЅР°")
async def show_cart(message: types.Message):
    user_id = message.from_user.id

    if user_id not in user_carts or not user_carts[user_id]:
        await message.answer("рџ›’ Р’Р°С€Р° РєРѕСЂР·РёРЅР° РїСѓСЃС‚Р°.", reply_markup=get_main_menu_keyboard())
        return

    await message.answer("вЏі Р¤РѕСЂРјРёСЂСѓСЋ С„Р°Р№Р» СЃ РІР°С€РµР№ РєРѕСЂР·РёРЅРѕР№вЂ¦")

    cart_items = list(user_carts[user_id].items())
    product_ids = [str(pid) for pid, _ in cart_items]

    short_urls = []
    connector = aiohttp.TCPConnector(limit=5)  # Р»РёРјРёС‚ РѕРґРЅРѕРІСЂРµРјРµРЅРЅС‹С… СЃРѕРµРґРёРЅРµРЅРёР№
    async with aiohttp.ClientSession(connector=connector) as session:
        for pid in product_ids:
            product = products_by_id.get(pid, {})
            long_url = product.get('_URL_', '')
            short_url = await short_url(long_url)
            short_urls.append(short_url if short_url and short_url.startswith('http') else long_url)
            await asyncio.sleep(0.2)  # Р·Р°РґРµСЂР¶РєР° РјРµР¶РґСѓ Р·Р°РїСЂРѕСЃР°РјРё

    rows = []
    for (product_id, product_info), link in zip(cart_items, short_urls):
        pid_str = str(product_id)
        product = products_by_id.get(pid_str, {})
        category_name = product.get('category_name', 'Р‘РµР· РєР°С‚РµРіРѕСЂРёРё')

        logging.info(f"[DEBUG] РўРѕРІР°СЂ {pid_str}: category_name={category_name}")

        rows.append({
            "РђСЂС‚РёРєСѓР»": pid_str,
            "РќР°Р·РІР°РЅРёРµ": product_info.get('name', ''),
            "РљР°С‚РµРіРѕСЂРёСЏ": category_name,
            "РЎСЃС‹Р»РєР°": link,
            "РљРѕР»РёС‡РµСЃС‚РІРѕ": product_info.get('quantity', 0),
            "Р¦РµРЅР° Р·Р° С€С‚.": product_info.get('price', 0),
            "РЎСѓРјРјР°": product_info.get('price', 0) * product_info.get('quantity', 0)
        })

    df = pd.DataFrame(rows)
    total_rows = len(df)
    num_sheets = math.ceil(total_rows / MAX_ROWS_PER_FILE)
    logging.info(f"Р’СЃРµРіРѕ С‚РѕРІР°СЂРѕРІ: {total_rows}, Р»РёСЃС‚РѕРІ: {num_sheets}")

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        for i in range(num_sheets):
            start = i * MAX_ROWS_PER_FILE
            end = min(start + MAX_ROWS_PER_FILE, total_rows)
            part_df = df.iloc[start:end]
            if part_df.empty:
                logging.info(f"Р›РёСЃС‚ {i+1} РїСѓСЃС‚РѕР№, РїСЂРѕРїСѓСЃРєР°РµРј")
                continue
            sheet_name = f"РљРѕСЂР·РёРЅР°_{i+1}"
            part_df.to_excel(writer, index=False, sheet_name=sheet_name)
            worksheet = writer.sheets[sheet_name]
            worksheet.set_column('A:A', 20)  # РђСЂС‚РёРєСѓР»
            worksheet.set_column('B:B', 40)  # РќР°Р·РІР°РЅРёРµ
            worksheet.set_column('C:C', 25)  # РљР°С‚РµРіРѕСЂРёСЏ
            worksheet.set_column('D:D', 50)  # РЎСЃС‹Р»РєР°
            worksheet.set_column('E:E', 15)  # РљРѕР»РёС‡РµСЃС‚РІРѕ
            worksheet.set_column('F:F', 15)  # Р¦РµРЅР° Р·Р° С€С‚.
            worksheet.set_column('G:G', 15)  # РЎСѓРјРјР°

    output.seek(0)
    file_name = "РљРѕСЂР·РёРЅР°_РІСЃСЏ_С‡Р°СЃС‚СЏРјРё.xlsx"
    file = BufferedInputFile(output.read(), filename=file_name)

    await bot.send_document(chat_id=user_id, document=file, caption="Р’Р°С€Р° РєРѕСЂР·РёРЅР° (РЅРµСЃРєРѕР»СЊРєРѕ Р»РёСЃС‚РѕРІ)")

    total_sum = sum(row["РЎСѓРјРјР°"] for row in rows)
    await message.answer(f"рџ›’ РС‚РѕРіРѕ: {total_sum:.2f} в‚Ѕ", reply_markup=get_cart_keyboard())

  
@dp.callback_query(F.data.startswith("add_"))
async def add_to_cart(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    if len(parts) != 3:
        await callback.answer("вќЊ РћС€РёР±РєР° РґР°РЅРЅС‹С…, РїРѕРїСЂРѕР±СѓР№С‚Рµ СЃРЅРѕРІР°.")
        return
    product_id, quantity_available_str = parts[1], parts[2]
    try:
        quantity_available = int(quantity_available_str)
    except ValueError:
        await callback.answer("вќЊ РћС€РёР±РєР° РґР°РЅРЅС‹С…, РїРѕРїСЂРѕР±СѓР№С‚Рµ СЃРЅРѕРІР°.")
        return

    product = next((p for p in products if str(p['_ID_']) == product_id), None)
    if product:
        price = parse_price(product.get('_PRICE_', '0'))
        product_data = {
            'product_id': product_id,
            'quantity_available': quantity_available,
            'price': price,
            'name': product.get('_NAME_', 'Р‘РµР· РЅР°Р·РІР°РЅРёСЏ')
        }
        await state.update_data(**product_data)
        await callback.message.answer(f"вњЏпёЏ Р’РІРµРґРёС‚Рµ РєРѕР»РёС‡РµСЃС‚РІРѕ (РјР°РєСЃ. {quantity_available} С€С‚.):", reply_markup=get_back_to_main_menu_keyboard())
        await state.set_state(OrderQuantity.waiting_for_quantity)
    else:
        await callback.answer("вќЊ РўРѕРІР°СЂ РЅРµ РЅР°Р№РґРµРЅ.")

@dp.message(OrderQuantity.waiting_for_quantity)
async def process_quantity(message: types.Message, state: FSMContext):
    data = await state.get_data()
    product_id = data.get('product_id')
    quantity_available = data.get('quantity_available')
    price = data.get('price')
    name = data.get('name')

    if product_id is None or quantity_available is None or price is None or name is None:
        await message.answer("вќЊ РџСЂРѕРёР·РѕС€Р»Р° РѕС€РёР±РєР°, РїРѕРїСЂРѕР±СѓР№С‚Рµ РґРѕР±Р°РІРёС‚СЊ С‚РѕРІР°СЂ Р·Р°РЅРѕРІРѕ.", 
                           reply_markup=get_back_to_main_menu_keyboard())
        await state.clear()
        return

    try:
        quantity = int(message.text)
    except ValueError:
        await message.answer("вљ пёЏ РџРѕР¶Р°Р»СѓР№СЃС‚Р°, РІРІРµРґРёС‚Рµ С‡РёСЃР»Рѕ.", 
                           reply_markup=get_back_to_main_menu_keyboard())
        return

    if quantity <= 0 or quantity > quantity_available:
        await message.answer(f"вљ пёЏ РќРµРєРѕСЂСЂРµРєС‚РЅРѕРµ РєРѕР»РёС‡РµСЃС‚РІРѕ. Р’РІРµРґРёС‚Рµ РѕС‚ 1 РґРѕ {quantity_available}:",
                           reply_markup=get_back_to_main_menu_keyboard())
        return

    user_id = message.from_user.id
    if user_id not in user_carts:
        user_carts[user_id] = {}

    user_carts[user_id][product_id] = {
        'quantity': quantity,
        'price': price,
        'name': name
    }

    # РџРѕСЃР»Рµ РґРѕР±Р°РІР»РµРЅРёСЏ С‚РѕРІР°СЂР° РїСЂРµРґР»Р°РіР°РµРј РІРІРµСЃС‚Рё СЃР»РµРґСѓСЋС‰РёР№ Р°СЂС‚РёРєСѓР» РёР»Рё РїРµСЂРµР№С‚Рё РІ РєРѕСЂР·РёРЅСѓ/РјРµРЅСЋ
    await message.answer(
        f"вњ… Р”РѕР±Р°РІР»РµРЅРѕ {quantity} С€С‚. РІ РєРѕСЂР·РёРЅСѓ!\n\n"
        "Р’РІРµРґРёС‚Рµ СЃР»РµРґСѓСЋС‰РёР№ Р°СЂС‚РёРєСѓР» РґР»СЏ РїРѕРёСЃРєР° РёР»Рё РІС‹Р±РµСЂРёС‚Рµ РґРµР№СЃС‚РІРёРµ РЅРёР¶Рµ.",
        reply_markup=get_cart_keyboard()  # РљР»Р°РІРёР°С‚СѓСЂР° СЃ РєРЅРѕРїРєР°РјРё
    )

    # Р’РѕР·РІСЂР°С‰Р°РµРј РІ СЃРѕСЃС‚РѕСЏРЅРёРµ РѕР¶РёРґР°РЅРёСЏ Р°СЂС‚РёРєСѓР»Р°
    await state.set_state(UserStates.waiting_for_article_request)


@dp.message(F.text == "рџ›’ РџРµСЂРµР№С‚Рё РІ РєРѕСЂР·РёРЅСѓ")
async def handle_cart_button(message: types.Message):
    user_id = message.from_user.id
    if user_id not in user_carts or not user_carts[user_id]:
        await message.answer("рџ›’ Р’Р°С€Р° РєРѕСЂР·РёРЅР° РїСѓСЃС‚Р°", 
                           reply_markup=get_main_menu_keyboard())
        return
    
    # Р—РґРµСЃСЊ РґРѕР»Р¶РЅР° Р±С‹С‚СЊ РІР°С€Р° Р»РѕРіРёРєР° РѕС‚РѕР±СЂР°Р¶РµРЅРёСЏ РєРѕСЂР·РёРЅС‹
    cart_text = "рџ›’ Р’Р°С€Р° РєРѕСЂР·РёРЅР°:\n\n"
    total = 0
    
    for product_id, item in user_carts[user_id].items():
        product_total = item['quantity'] * item['price']
        cart_text += f"в–Є {item['name']}\n"
        cart_text += f"РљРѕР»РёС‡РµСЃС‚РІРѕ: {item['quantity']} Г— {item['price']} в‚Ѕ = {product_total} в‚Ѕ\n\n"
        total += product_total
    
    cart_text += f"РС‚РѕРіРѕ: {total} в‚Ѕ"
    
    await message.answer(
        cart_text,
        reply_markup=get_main_menu_keyboard()  # РР»Рё РґСЂСѓРіР°СЏ РєР»Р°РІРёР°С‚СѓСЂР° РґР»СЏ РєРѕСЂР·РёРЅС‹
    )


@dp.message(F.text == "рџ“Љ РџСЂРѕСЃС‡С‘С‚ Excel СЃ Р°СЂС‚РёРєСѓР»Р°РјРё")
async def start_multiple_articles(message: types.Message, state: FSMContext):
    await message.answer(
        "рџ“¤ РћС‚РїСЂР°РІСЊС‚Рµ Excel-С„Р°Р№Р» СЃ Р°СЂС‚РёРєСѓР»Р°РјРё, РЅР°Р·РІР°РЅРёРµРј (РјРѕР¶РЅРѕ РѕСЃС‚Р°РІРёС‚СЊ РїСѓСЃС‚С‹Рј) Рё РєРѕР»РёС‡РµСЃС‚РІРѕРј.\n\n"
        "Р¤РѕСЂРјР°С‚: РІ РїРµСЂРІРѕРј СЃС‚РѕР»Р±С†Рµ Р°СЂС‚РёРєСѓР», РІРѕ РІС‚РѕСЂРѕРј - РЅР°Р·РІР°РЅРёРµ, РІ С‚СЂРµС‚СЊРµРј - РєРѕР»РёС‡РµСЃС‚РІРѕ.",
        reply_markup=get_back_to_main_menu_keyboard()
    )
    await state.set_state(MultipleArticlesStates.waiting_for_file)


@dp.message(UserStates.waiting_for_article_request)
async def handle_article_request(message: types.Message, state: FSMContext):
    print(f"handle_article_request РІС‹Р·РІР°РЅ РґР»СЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ {message.from_user.id}")
    text = message.text.strip()

    if text == "рџ—‘ РћС‡РёСЃС‚РёС‚СЊ РєРѕСЂР·РёРЅСѓ":
        await clear_cart(message)
        await state.clear()
        return

    if text == "вњ… РћС„РѕСЂРјРёС‚СЊ Р·Р°РєР°Р·":
        await checkout(message, state)
        return

    if text == "рџ›’ РљРѕСЂР·РёРЅР°":
        await show_cart(message)
        await state.clear()
        return

    if text == "рџЏ  РћСЃРЅРѕРІРЅРѕРµ РјРµРЅСЋ":
        await message.answer("рџЏ  Р“Р»Р°РІРЅРѕРµ РјРµРЅСЋ", reply_markup=get_main_menu_keyboard())
        await state.clear()
        return

    raw_query = message.text.strip()
    norm_query = normalize_article(raw_query)

    product = None
    for p in products:
        sku = p.get('_SKU_', '')
        name = p.get('_NAME_', '')
        norm_sku = normalize_article(sku)
        norm_name = normalize_article(name)

        if norm_sku == norm_query or norm_query in norm_name:
            product = p
            break

    if product:
        product_url = product.get('_URL_')
        photo_sent = False

        # РџРѕРїС‹С‚РєР° РѕС‚РїСЂР°РІРёС‚СЊ С„РѕС‚Рѕ
        if product_url:
            try:
                response = requests.get(product_url)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')

                img_tag = soup.select_one('.product-image img') or soup.select_one('.product-page img')

                if not img_tag:
                    img_tags = soup.find_all('img')
                    for tag in img_tags:
                        src = tag.get('src', '')
                        if norm_query in normalize_article(src) or 'product' in src.lower():
                            img_tag = tag
                            break

                if img_tag and img_tag.get('src'):
                    img_url = img_tag['src']
                    if not img_url.startswith('http'):
                        img_url = urljoin(product_url, img_url)

                    caption = f"рџ–ј Р¤РѕС‚Рѕ С‚РѕРІР°СЂР°:"
                    await message.answer_photo(photo=img_url, caption=caption)
                    photo_sent = True
                else:
                    await message.answer("вљ пёЏ Р¤РѕС‚Рѕ РґР»СЏ СЌС‚РѕРіРѕ С‚РѕРІР°СЂР° РЅРµ РЅР°Р№РґРµРЅРѕ РЅР° СЃС‚СЂР°РЅРёС†Рµ.")
            except Exception as e:
                await message.answer(f"вќЊ РћС€РёР±РєР° РїСЂРё Р·Р°РіСЂСѓР·РєРµ С„РѕС‚Рѕ: {str(e)[:50]}")
        else:
            await message.answer("вљ пёЏ URL С‚РѕРІР°СЂР° РЅРµ РЅР°Р№РґРµРЅ РІ РґР°РЅРЅС‹С….")

        # РџРѕР»СѓС‡Р°РµРј РєРѕСЂРѕС‚РєСѓСЋ СЃСЃС‹Р»РєСѓ С‡РµСЂРµР· СЃРёРЅС…СЂРѕРЅРЅСѓСЋ С„СѓРЅРєС†РёСЋ СЃ Р»РёРјРёС‚РѕРј
        short_url = None
        if product_url:
            short_url = await shorten_url_yandex_async(product_url)
            if not short_url or not short_url.startswith('http'):
                short_url = product_url  # fallback РЅР° РїРѕР»РЅСѓСЋ СЃСЃС‹Р»РєСѓ

        # РџРѕР»СѓС‡Р°РµРј РєР°С‚РµРіРѕСЂРёСЋ
        category_name = product.get('category_name', 'Р‘РµР· РєР°С‚РµРіРѕСЂРёРё')

        # Р¤РѕСЂРјРёСЂСѓРµРј С‚РµРєСЃС‚ СЃ РєР°С‚РµРіРѕСЂРёРµР№ Рё РєРѕСЂРѕС‚РєРѕР№ СЃСЃС‹Р»РєРѕР№
        text = format_product_info(product, short_url=short_url, category=category_name)

        quantity_available = int(product.get('_QUANTITY_', 0))
        product_id = product.get('_ID_')
        await send_message_in_parts(
            message,
            text,
            reply_markup=get_product_keyboard(product_id, quantity_available),
            parse_mode='Markdown'
        )
        await state.update_data(article_requested_once=True)
    else:
        await message.answer(
            f"вќЊ РўРѕРІР°СЂ СЃ Р°СЂС‚РёРєСѓР»РѕРј РёР»Рё РЅР°Р·РІР°РЅРёРµРј '{raw_query}' РЅРµ РЅР°Р№РґРµРЅ.",
            reply_markup=get_back_to_main_menu_keyboard()
        )
        await state.clear()


@dp.message(F.text == "рџ›’ РљРѕСЂР·РёРЅР°")
async def show_cart(message: types.Message):
    user_id = message.from_user.id

    if user_id not in user_carts or not user_carts[user_id]:
        await message.answer("рџ›’ Р’Р°С€Р° РєРѕСЂР·РёРЅР° РїСѓСЃС‚Р°.", reply_markup=get_main_menu_keyboard())
        return

    await message.answer("вЏі Р¤РѕСЂРјРёСЂСѓСЋ С„Р°Р№Р» СЃ РІР°С€РµР№ РєРѕСЂР·РёРЅРѕР№вЂ¦")

    cart_items = list(user_carts[user_id].items())
    product_ids = [str(pid) for pid, _ in cart_items]

    async with aiohttp.ClientSession() as session:
        short_urls = []
        for pid in product_ids:
            product = products_by_id.get(pid, {})
            long_url = product.get('_URL_', '')
            short_url = await shorten_url_yandex_async(long_url)
            short_urls.append(short_url)
            await asyncio.sleep(0.2)  # Р·Р°РґРµСЂР¶РєР° 200 РјСЃ РјРµР¶РґСѓ Р·Р°РїСЂРѕСЃР°РјРё

    rows = []
    for (product_id, product_info), short_url in zip(cart_items, short_urls):
        pid_str = str(product_id)
        product = products_by_id.get(pid_str, {})
        url = product.get('_URL_', '')
        link = short_url if short_url and short_url.startswith('http') else url

        category_name = product.get('category_name', 'Р‘РµР· РєР°С‚РµРіРѕСЂРёРё')

        rows.append({
            "РђСЂС‚РёРєСѓР»": pid_str,
            "РќР°Р·РІР°РЅРёРµ": product_info.get('name', ''),
            "РљР°С‚РµРіРѕСЂРёСЏ": category_name,
            "РЎСЃС‹Р»РєР°": link,
            "РљРѕР»РёС‡РµСЃС‚РІРѕ": product_info.get('quantity', 0),
            "Р¦РµРЅР° Р·Р° С€С‚.": product_info.get('price', 0),
            "РЎСѓРјРјР°": product_info.get('price', 0) * product_info.get('quantity', 0)
        })

    df = pd.DataFrame(rows)
    total_rows = len(df)
    num_sheets = math.ceil(total_rows / MAX_ROWS_PER_FILE)
    logging.info(f"Р’СЃРµРіРѕ С‚РѕРІР°СЂРѕРІ: {total_rows}, Р»РёСЃС‚РѕРІ Р±СѓРґРµС‚: {num_sheets}")

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        for i in range(num_sheets):
            start = i * MAX_ROWS_PER_FILE
            end = min(start + MAX_ROWS_PER_FILE, total_rows)
            part_df = df.iloc[start:end]
            if part_df.empty:
                logging.info(f"Р›РёСЃС‚ {i+1} РїСѓСЃС‚РѕР№, РїСЂРѕРїСѓСЃРєР°РµРј")
                continue
            sheet_name = f"РљРѕСЂР·РёРЅР°_{i+1}"
            part_df.to_excel(writer, index=False, sheet_name=sheet_name)
            worksheet = writer.sheets[sheet_name]
            worksheet.set_column('A:A', 20)  # РђСЂС‚РёРєСѓР»
            worksheet.set_column('B:B', 40)  # РќР°Р·РІР°РЅРёРµ
            worksheet.set_column('C:C', 25)  # РљР°С‚РµРіРѕСЂРёСЏ
            worksheet.set_column('D:D', 50)  # РЎСЃС‹Р»РєР°
            worksheet.set_column('E:E', 15)  # РљРѕР»РёС‡РµСЃС‚РІРѕ
            worksheet.set_column('F:F', 15)  # Р¦РµРЅР° Р·Р° С€С‚.
            worksheet.set_column('G:G', 15)  # РЎСѓРјРјР°

    output.seek(0)
    file_name = "РљРѕСЂР·РёРЅР°_РІСЃСЏ_С‡Р°СЃС‚СЏРјРё.xlsx"
    file = BufferedInputFile(output.read(), filename=file_name)

    await bot.send_document(chat_id=user_id, document=file, caption="Р’Р°С€Р° РєРѕСЂР·РёРЅР° (РЅРµСЃРєРѕР»СЊРєРѕ Р»РёСЃС‚РѕРІ)")

    total_sum = sum(row["РЎСѓРјРјР°"] for row in rows)
    await message.answer(f"рџ›’ РС‚РѕРіРѕ: {total_sum:.2f} в‚Ѕ", reply_markup=get_cart_keyboard())




# РћР±СЂР°Р±РѕС‚С‡РёРє РєРЅРѕРїРєРё "РћС‡РёСЃС‚РёС‚СЊ РєРѕСЂР·РёРЅСѓ"
@dp.message(F.text == "рџ—‘ РћС‡РёСЃС‚РёС‚СЊ РєРѕСЂР·РёРЅСѓ")
async def clear_cart(message: types.Message):
    user_id = message.from_user.id
    if user_id in user_carts:
        user_carts[user_id].clear()
    await message.answer("рџ›’ РљРѕСЂР·РёРЅР° РѕС‡РёС‰РµРЅР°.", reply_markup=get_main_menu_keyboard())

# РћР±СЂР°Р±РѕС‚С‡РёРє РєРЅРѕРїРєРё "РћС„РѕСЂРјРёС‚СЊ Р·Р°РєР°Р·"
@dp.message(F.text == "вњ… РћС„РѕСЂРјРёС‚СЊ Р·Р°РєР°Р·")
async def checkout(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if user_id not in user_carts or not user_carts[user_id]:
        await message.answer(
            "рџ›’ Р’Р°С€Р° РєРѕСЂР·РёРЅР° РїСѓСЃС‚Р°. Р”РѕР±Р°РІСЊС‚Рµ С‚РѕРІР°СЂС‹ РїРµСЂРµРґ РѕС„РѕСЂРјР»РµРЅРёРµРј Р·Р°РєР°Р·Р°.",
            reply_markup=get_main_menu_keyboard()
        )
        return
    await message.answer(
        "рџ“ћ Р’РІРµРґРёС‚Рµ РІР°С€ РЅРѕРјРµСЂ С‚РµР»РµС„РѕРЅР° РґР»СЏ СЃРІСЏР·Рё:",
        reply_markup=get_main_menu_keyboard()
    )
    await state.set_state(OrderStates.waiting_for_contact)

# РћР±СЂР°Р±РѕС‚С‡РёРє С‚РµР»РµС„РѕРЅР°
@dp.message(OrderStates.waiting_for_contact)
async def process_contact(message: types.Message, state: FSMContext):
    contact_raw = message.text.strip()

    # РЈР±РёСЂР°РµРј РІСЃРµ РЅРµС†РёС„СЂРѕРІС‹Рµ СЃРёРјРІРѕР»С‹
    digits = re.sub(r'\D', '', contact_raw)

    # РџСЂРѕРІРµСЂСЏРµРј РґР»РёРЅСѓ Рё С„РѕСЂРјР°С‚ РЅРѕРјРµСЂР°
    if len(digits) == 11 and digits.startswith('8'):
        # Р—Р°РјРµРЅСЏРµРј 8 РЅР° +7
        formatted = '+7' + digits[1:]
    elif len(digits) == 11 and digits.startswith('7'):
        formatted = '+' + digits
    elif len(digits) == 10:
        # Р•СЃР»Рё 10 С†РёС„СЂ, СЃС‡РёС‚Р°РµРј, С‡С‚Рѕ Р±РµР· РєРѕРґР° СЃС‚СЂР°РЅС‹, РґРѕР±Р°РІР»СЏРµРј +7
        formatted = '+7' + digits
    else:
        await message.answer("вќЊ Р’РІРµРґРёС‚Рµ РєРѕСЂСЂРµРєС‚РЅС‹Р№ РЅРѕРјРµСЂ С‚РµР»РµС„РѕРЅР° РІ С„РѕСЂРјР°С‚Рµ +7XXXXXXXXXX РёР»Рё 8XXXXXXXXXX:")
        return

    await state.update_data(contact=formatted)
    await message.answer("рџ“Ќ Р’РІРµРґРёС‚Рµ Р°РґСЂРµСЃ РґРѕСЃС‚Р°РІРєРё:")
    await state.set_state(OrderStates.waiting_for_address)


# РћСЃРЅРѕРІРЅРѕР№ РѕР±СЂР°Р±РѕС‚С‡РёРє Р·Р°РєР°Р·Р°
@dp.message(OrderStates.waiting_for_address)
async def process_address(message: types.Message, state: FSMContext):
    # РџРѕР»СѓС‡Р°РµРј РґР°РЅРЅС‹Рµ
    address = message.text.strip()
    data = await state.get_data()
    contact = data.get("contact", "РќРµ СѓРєР°Р·Р°РЅ")
    user_id = message.from_user.id
    current_time = datetime.now().strftime("%d.%m.%Y %H:%M")

    
    # РџСЂРѕРІРµСЂСЏРµРј РєРѕСЂР·РёРЅСѓ
    cart_items = user_carts.get(user_id, {})
    if not cart_items:
        await message.answer("рџ›’ Р’Р°С€Р° РєРѕСЂР·РёРЅР° РїСѓСЃС‚Р°.", reply_markup=get_main_menu_keyboard())
        await state.clear()
        return
    
    # Р¤РѕСЂРјРёСЂСѓРµРј РґР°РЅРЅС‹Рµ Р·Р°РєР°Р·Р°
    total_sum = sum(item['price'] * item['quantity'] for item in cart_items.values())
    order_data = {
        'user_id': user_id,
        'username': message.from_user.username or "Р‘РµР· username",
        'contact': contact,
        'address': address,
        'total_sum': total_sum,
        'items': list(cart_items.values()),
        'order_time': current_time
    }
    
    # Р“РµРЅРµСЂРёСЂСѓРµРј Excel-С„Р°Р№Р»
    excel_file = await generate_excel(order_data)
    
    # РћС‚РїСЂР°РІР»СЏРµРј РєР»РёРµРЅС‚Сѓ
    await send_client_confirmation(message, order_data, excel_file)
    
    # РћС‚РїСЂР°РІР»СЏРµРј СѓРІРµРґРѕРјР»РµРЅРёСЏ
    await notify_order(order_data, excel_file)
    
    # РћС‡РёС‰Р°РµРј РґР°РЅРЅС‹Рµ
    user_carts[user_id].clear()
    await state.clear()

# Р“РµРЅРµСЂР°С†РёСЏ Excel-С„Р°Р№Р»Р°
async def generate_excel(order_data: dict) -> bytes:
    data = []
    for item in order_data['items']:
        data.append({
            "РќР°Р·РІР°РЅРёРµ": item['name'],
            "РљРѕР»РёС‡РµСЃС‚РІРѕ": item['quantity'],
            "Р¦РµРЅР° Р·Р° С€С‚.": item['price'],
            "РЎСѓРјРјР°": item['price'] * item['quantity']
        })
    
    df = pd.DataFrame(data)
    total_row = pd.DataFrame([{
        "РќР°Р·РІР°РЅРёРµ": "РС‚РѕРіРѕ",
        "РљРѕР»РёС‡РµСЃС‚РІРѕ": "",
        "Р¦РµРЅР° Р·Р° С€С‚.": "",
        "РЎСѓРјРјР°": order_data['total_sum']
    }])
    df = pd.concat([df, total_row], ignore_index=True)
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Р—Р°РєР°Р·', index=False)
        writer.close()
    output.seek(0)
    return output.read()

# РћС‚РїСЂР°РІРєР° РєР»РёРµРЅС‚Сѓ
async def send_client_confirmation(message: types.Message, order_data: dict, excel_file: bytes):
    order_summary = (
        "вњ… <b>Р—Р°РєР°Р· РѕС„РѕСЂРјР»РµРЅ!</b>\n\n"
        f"рџ“ћ <b>РљРѕРЅС‚Р°РєС‚:</b> {hd.quote(order_data['contact'])}\n"
        f"рџЏ  <b>РђРґСЂРµСЃ:</b> {hd.quote(order_data['address'])}\n\n"
        f"рџ’° <b>РС‚РѕРіРѕ:</b> {order_data['total_sum']:.2f} в‚Ѕ\n\n"
        "рџ“„ РџРѕРґСЂРѕР±РЅРѕСЃС‚Рё Р·Р°РєР°Р·Р° РІ РїСЂРёРєСЂРµРїР»С‘РЅРЅРѕРј С„Р°Р№Р»Рµ."
    )
    
    await message.answer(
        order_summary,
        parse_mode="HTML",
        reply_markup=get_main_menu_keyboard()
    )
    
    await bot.send_document(
        chat_id=message.chat.id,
        document=types.BufferedInputFile(excel_file, filename="Р—Р°РєР°Р·.xlsx")
    )
    
    # РљРЅРѕРїРєР° СЃРІСЏР·Рё СЃ РјРµРЅРµРґР¶РµСЂРѕРј
    contact_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="рџ“© РЎРІСЏР·Р°С‚СЊСЃСЏ СЃ РјРµРЅРµРґР¶РµСЂРѕРј",
            url=f"https://t.me/{ADMIN_USERNAME}"
        )]
    ])
    
    await message.answer(
        "рџ“ў Р§С‚РѕР±С‹ Р·Р°РІРµСЂС€РёС‚СЊ РѕС„РѕСЂРјР»РµРЅРёРµ Р·Р°РєР°Р·Р°, РЅР°РїРёС€РёС‚Рµ РјРµРЅРµРґР¶РµСЂСѓ.",
        reply_markup=contact_keyboard
    )

# РћС‚РїСЂР°РІРєР° СѓРІРµРґРѕРјР»РµРЅРёР№
async def notify_order(order_data: dict, excel_file: bytes):
    # РўРµРєСЃС‚ РґР»СЏ Р°РґРјРёРЅРѕРІ/РєР°РЅР°Р»Р°
    text = (
        "рџљЁ <b>РќРѕРІС‹Р№ Р·Р°РєР°Р·!</b>\n\n"
        f"рџ‘¤ <b>РљР»РёРµРЅС‚:</b> {order_data['username']}\n"
        f"рџ“ћ <b>РљРѕРЅС‚Р°РєС‚:</b> <code>{hd.quote(order_data['contact'])}</code>\n"
        f"рџЏ  <b>РђРґСЂРµСЃ:</b> {hd.quote(order_data['address'])}\n\n"
        f"рџ’° <b>РЎСѓРјРјР°:</b> {order_data['total_sum']:.2f} в‚Ѕ\n"
        f"рџ•’ <b>Р’СЂРµРјСЏ:</b> {hd.quote(order_data['order_time'])}"
    )
    
    # РћС‚РїСЂР°РІРєР° Р°РґРјРёРЅР°Рј
    for admin_id in admin_ids:
        try:
           await bot.send_document(
            chat_id=admin_id,
            document=types.BufferedInputFile(
                excel_file,
                filename=f"Р—Р°РєР°Р·_{order_data['user_id']}.xlsx"
            ),
            caption=text,  
            parse_mode="HTML"
        )
        except Exception as e:
            logging.error(f"РћС€РёР±РєР° РѕС‚РїСЂР°РІРєРё Р°РґРјРёРЅСѓ {admin_id}: {e}")
    
    # РћС‚РїСЂР°РІРєР° РІ РєР°РЅР°Р»
    if ORDER_CHANNEL:
        try:
            await bot.send_document(
            chat_id=ORDER_CHANNEL,
            document=types.BufferedInputFile(
                excel_file,
                filename=f"Р—Р°РєР°Р·_{order_data['user_id']}.xlsx"
            ),
            caption=text,  # РўРµРєСЃС‚ С‚РµРїРµСЂСЊ РІ РїРѕРґРїРёСЃРё Рє С„Р°Р№Р»Сѓ
            parse_mode="HTML"
        )
        except Exception as e:
            logging.error(f"РћС€РёР±РєР° РѕС‚РїСЂР°РІРєРё РІ РєР°РЅР°Р»: {e}")
        # Р”РѕРїРѕР»РЅРёС‚РµР»СЊРЅРѕ СѓРІРµРґРѕРјР»СЏРµРј Р°РґРјРёРЅР° РѕР± РѕС€РёР±РєРµ
            await bot.send_message(
            admin_ids[0],
            f"вљ пёЏ РћС€РёР±РєР° РѕС‚РїСЂР°РІРєРё РІ РєР°РЅР°Р»: {str(e)[:300]}"
        )



async def main():
    logging.info("РЎС‚Р°СЂС‚ Р±РѕС‚Р°")
    await db.connect()  # РџРѕРґРєР»СЋС‡Р°РµРјСЃСЏ Рє Р±Р°Р·Рµ
    await bot.delete_webhook(drop_pending_updates=True)
    try:
        await dp.start_polling(bot, skip_updates=True)
    except asyncio.CancelledError:
        logging.info("Polling РѕС‚РјРµРЅС‘РЅ")
    finally:
        logging.info("Р—Р°РєСЂС‹РІР°РµРј Р±РѕС‚Р° Рё Р±Р°Р·Сѓ РґР°РЅРЅС‹С…")
        await bot.session.close()
        await db.close()  # Р—Р°РєСЂС‹РІР°РµРј Р±Р°Р·Сѓ

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nР‘РѕС‚ РѕСЃС‚Р°РЅРѕРІР»РµРЅ РІСЂСѓС‡РЅСѓСЋ")
