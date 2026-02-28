import asyncio
import os
import sys
import logging
import subprocess
import psutil
import sqlite3
import hashlib
import json
import zipfile
import venv
import shutil
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Set, Tuple, Callable, Awaitable, Any
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiohttp import web
from pathlib import Path
from dotenv import load_dotenv
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
# Load environment variables
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# Configuration
TOKEN = os.getenv('BOT_TOKEN')
OWNER_ID_STR = os.getenv('OWNER_ID')
ADMIN_ID_STR = os.getenv('ADMIN_ID')
YOUR_USERNAME = os.getenv('YOUR_USERNAME')
UPDATE_CHANNEL = os.getenv('UPDATE_CHANNEL', '-1002902012204')
try:
    CHANNEL_ID = int(UPDATE_CHANNEL)
except ValueError:
    logger.error("UPDATE_CHANNEL must be a valid integer chat ID!")
    CHANNEL_ID = -100
# Validate required environment variables
if not TOKEN:
    logger.error("BOT_TOKEN not found in environment variables!")
    raise ValueError("BOT_TOKEN is required. Please set it in .env file or environment variables.")
if not OWNER_ID_STR or not ADMIN_ID_STR:
    logger.error("OWNER_ID or ADMIN_ID not found in environment variables!")
    raise ValueError("OWNER_ID and ADMIN_ID are required. Please set them in .env file.")
try:
    OWNER_ID = int(OWNER_ID_STR)
    ADMIN_ID = int(ADMIN_ID_STR)
except ValueError:
    logger.error("OWNER_ID or ADMIN_ID must be valid integers!")
    raise
YOUR_USERNAME = YOUR_USERNAME or '@jahidul_98'
# Paths
BASE_DIR = Path(__file__).parent.absolute()
UPLOAD_BOTS_DIR = BASE_DIR / 'upload_bots'
IROTECH_DIR = BASE_DIR / 'inf'
DATABASE_PATH = IROTECH_DIR / 'bot_data.db'
LOGS_DIR = BASE_DIR / 'user_logs'
# Create directories if they don't exist
UPLOAD_BOTS_DIR.mkdir(exist_ok=True)
IROTECH_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)
# Constants
FREE_USER_LIMIT = 1
SUBSCRIBED_USER_LIMIT = float('inf')
ADMIN_LIMIT = float('inf')
OWNER_LIMIT = float('inf')
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 MB
FREE_USER_RAM_LIMIT = 512 * 1024 * 1024  # 512 MB
PREMIUM_USER_RAM_LIMIT = 1 * 1024 * 1024 * 1024  # 1 GB
PREMIUM_PRICE = 15
MEMBER_CHECK_FAILED = "❌ <b>You must join our channel to use this bot!</b>\n\n"
# Initialize bot
bot = Bot(token=TOKEN)
dp = Dispatcher(storage=MemoryStorage())
# Global data structures
bot_scripts: Dict[str, Dict] = {}
user_subscriptions: Dict[int, Dict] = {}
user_projects: Dict[int, Dict[str, Dict]] = {}
user_favorites: Dict[int, List[str]] = {}
banned_users: Set[int] = set()
active_users: Set[int] = set()
admin_ids: Set[int] = {ADMIN_ID, OWNER_ID}
bot_locked: bool = False
bot_stats: Dict[str, int] = {'total_uploads': 0, 'total_downloads': 0, 'total_runs': 0, 'total_projects': 0}
# States for project creation
class ProjectStates(StatesGroup):
    waiting_for_project_name = State()
    waiting_for_project_file = State()
    waiting_for_edit_command = State()
    waiting_for_pip_install = State()
# Database functions
def init_db():
    """Initialize the database with required tables"""
    logger.info(f"Initializing database at: {DATABASE_PATH}")
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS subscriptions
                     (user_id INTEGER PRIMARY KEY, expiry TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS projects
                     (user_id INTEGER, project_name TEXT, created_at TEXT,
                      last_updated TEXT, file_count INTEGER, run_command TEXT,
                      PRIMARY KEY (user_id, project_name))''')
        c.execute('''CREATE TABLE IF NOT EXISTS project_files
                     (user_id INTEGER, project_name TEXT, file_name TEXT,
                      file_type TEXT, upload_date TEXT,
                      PRIMARY KEY (user_id, project_name, file_name))''')
        c.execute('''CREATE TABLE IF NOT EXISTS project_logs
                     (user_id INTEGER, project_name TEXT, log_content TEXT,
                      log_date TEXT, PRIMARY KEY (user_id, project_name, log_date))''')
        c.execute('''CREATE TABLE IF NOT EXISTS active_users
                     (user_id INTEGER PRIMARY KEY, join_date TEXT, last_active TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS admins
                     (user_id INTEGER PRIMARY KEY)''')
        c.execute('''CREATE TABLE IF NOT EXISTS banned_users
                     (user_id INTEGER PRIMARY KEY, banned_date TEXT, reason TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS favorites
                     (user_id INTEGER, project_name TEXT, PRIMARY KEY (user_id, project_name))''')
        c.execute('''CREATE TABLE IF NOT EXISTS bot_stats
                     (stat_name TEXT PRIMARY KEY, stat_value INTEGER)''')
        c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (OWNER_ID,))
        if ADMIN_ID != OWNER_ID:
            c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (ADMIN_ID,))
        for stat in ['total_uploads', 'total_downloads', 'total_runs', 'total_projects']:
            c.execute('INSERT OR IGNORE INTO bot_stats (stat_name, stat_value) VALUES (?, 0)', (stat,))
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"Database initialization error: {e}", exc_info=True)
def load_data():
    """Load data from database into memory"""
    logger.info("Loading data from database...")
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('SELECT user_id, expiry FROM subscriptions')
        for user_id, expiry in c.fetchall():
            try:
                user_subscriptions[user_id] = {'expiry': datetime.fromisoformat(expiry)}
            except ValueError:
                logger.warning(f"Invalid expiry date for user {user_id}")
        c.execute('SELECT user_id, project_name, created_at, last_updated, file_count, run_command FROM projects')
        for user_id, project_name, created_at, last_updated, file_count, run_command in c.fetchall():
            if user_id not in user_projects:
                user_projects[user_id] = {}
            user_projects[user_id][project_name] = {
                'created_at': created_at,
                'last_updated': last_updated,
                'file_count': file_count,
                'run_command': run_command or 'python3 main.py',
                'files': []
            }
        c.execute('SELECT user_id, project_name, file_name, file_type FROM project_files')
        for user_id, project_name, file_name, file_type in c.fetchall():
            if user_id in user_projects and project_name in user_projects[user_id]:
                user_projects[user_id][project_name]['files'].append((file_name, file_type))
        c.execute('SELECT user_id FROM active_users')
        active_users.update(user_id for (user_id,) in c.fetchall())
        c.execute('SELECT user_id FROM admins')
        admin_ids.update(user_id for (user_id,) in c.fetchall())
        c.execute('SELECT user_id FROM banned_users')
        banned_users.update(user_id for (user_id,) in c.fetchall())
        c.execute('SELECT user_id, project_name FROM favorites')
        for user_id, project_name in c.fetchall():
            if user_id not in user_favorites:
                user_favorites[user_id] = []
            user_favorites[user_id].append(project_name)
        c.execute('SELECT stat_name, stat_value FROM bot_stats')
        for stat_name, stat_value in c.fetchall():
            bot_stats[stat_name] = stat_value
        conn.close()
        logger.info(f"Data loaded: {len(active_users)} users, {len(banned_users)} banned, {len(admin_ids)} admins.")
    except Exception as e:
        logger.error(f"Error loading data: {e}", exc_info=True)
# Helper functions
def get_user_project_limit(user_id: int) -> float:
    """Get the project limit for a user"""
    if user_id == OWNER_ID:
        return OWNER_LIMIT
    if user_id in admin_ids:
        return ADMIN_LIMIT
    if user_id in user_subscriptions and user_subscriptions[user_id]['expiry'] > datetime.now():
        return SUBSCRIBED_USER_LIMIT
    return float(FREE_USER_LIMIT)
def get_user_ram_limit(user_id: int) -> float:
    """Get the RAM limit for a user"""
    if user_id == OWNER_ID:
        return float('inf')
    if user_id in admin_ids:
        return float('inf')
    if user_id in user_subscriptions and user_subscriptions[user_id]['expiry'] > datetime.now():
        return PREMIUM_USER_RAM_LIMIT
    return float(FREE_USER_RAM_LIMIT)
async def get_channel_invite_link():
    """Generate a fresh invite link for the channel"""
    try:
        try:
            link = await bot.export_chat_invite_link(CHANNEL_ID)
            return link
        except Exception as e:
            logger.warning(f"Failed to create new invite link: {e}")
            return f"https://t.me/c/{str(CHANNEL_ID)[4:]}"
    except Exception as e:
        logger.error(f"Error generating channel link: {e}")
        return f"https://t.me/c/{str(CHANNEL_ID)[4:]}"
# Keyboard functions
async def get_main_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Get the main keyboard for a user based on their permissions"""
    invite_link = await get_channel_invite_link()
    keyboard = [
        [InlineKeyboardButton(text="📢 Join Our Channel", url=invite_link)],
        [InlineKeyboardButton(text="🆕 New Project", callback_data="new_project"),
         InlineKeyboardButton(text="📁 My Projects", callback_data="my_projects")],
        [InlineKeyboardButton(text="⭐ Favorites", callback_data="my_favorites"),
         InlineKeyboardButton(text="🔍 Search Projects", callback_data="search_projects")],
        [InlineKeyboardButton(text="📊 My Stats", callback_data="statistics"),
         InlineKeyboardButton(text="⚡ Bot Speed", callback_data="bot_speed")],
    ]
    if user_id in admin_ids:
        keyboard.append([
            InlineKeyboardButton(text="👨‍💼 Admin Panel", callback_data="admin_panel"),
            InlineKeyboardButton(text="💬 Contact", url=f"https://t.me/{YOUR_USERNAME.replace('@', '')}")
        ])
    else:
        keyboard.append([
            InlineKeyboardButton(text="💎 Get Premium", callback_data="get_premium"),
            InlineKeyboardButton(text="💬 Contact Owner", url=f"https://t.me/{YOUR_USERNAME.replace('@', '')}")
        ])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)
def get_admin_panel_keyboard() -> InlineKeyboardMarkup:
    """Fixed admin panel keyboard with proper structure"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👥 User Stats", callback_data="admin_total_users"),
            InlineKeyboardButton(text="📁 Projects Stats", callback_data="admin_total_projects")
        ],
        [
            InlineKeyboardButton(text="🚀 Running Scripts", callback_data="admin_running_scripts"),
            InlineKeyboardButton(text="💎 Premium Users", callback_data="admin_premium_users")
        ],
        [
            InlineKeyboardButton(text="➕ Add Admin", callback_data="admin_add_admin"),
            InlineKeyboardButton(text="➖ Remove Admin", callback_data="admin_remove_admin")
        ],
        [
            InlineKeyboardButton(text="🚫 Ban User", callback_data="admin_ban_user"),
            InlineKeyboardButton(text="✅ Unban User", callback_data="admin_unban_user")
        ],
        [
            InlineKeyboardButton(text="📊 Bot Analytics", callback_data="admin_analytics"),
            InlineKeyboardButton(text="⚙️ System Info", callback_data="admin_system_status")
        ],
        [
            InlineKeyboardButton(text="🔒 Lock/Unlock", callback_data="lock_bot"),
            InlineKeyboardButton(text="📢 Broadcast", callback_data="broadcast")
        ],
        [
            InlineKeyboardButton(text="🗑️ Clean Projects", callback_data="admin_clean_projects"),
            InlineKeyboardButton(text="💾 Backup DB", callback_data="admin_backup_db")
        ],
        [
            InlineKeyboardButton(text="📝 View Logs", callback_data="admin_view_logs"),
            InlineKeyboardButton(text="🔄 Restart Bot", callback_data="admin_restart_bot")
        ],
        [InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
    ])
def get_project_keyboard(user_id: int, project_name: str) -> InlineKeyboardMarkup:
    """Get streamlined keyboard for a specific project."""
    project = user_projects[user_id][project_name]
    files = project['files']
    buttons = []
    # Check if a script is running for this project
    is_running = any(
        script_key.startswith(f"{user_id}_{project_name}_")
        for script_key in bot_scripts
    )
    # Use 'main_run' as a consistent key for the main process
    main_script_key = f"{user_id}_{project_name}_main_run"
    # 1. Main Run/Stop/Restart Button
    if is_running:
        buttons.append([
            InlineKeyboardButton(text="🛑 Stop Project", callback_data=f"stop_script:{main_script_key}"),
            InlineKeyboardButton(text="🔄 Restart Project", callback_data=f"restart_script:{main_script_key}")
        ])
    else:
        buttons.append([
            InlineKeyboardButton(text="▶️ Run Project", callback_data=f"run_script:{project_name}:main_file")
        ])
    # 2. Main Action Buttons
    buttons.extend([
        [InlineKeyboardButton(text="👨‍💻 Pip Install", callback_data=f"pip_install:{project_name}"),
         InlineKeyboardButton(text="📝 Edit Command", callback_data=f"edit_command:{project_name}")],
        [InlineKeyboardButton(text="📤 Upload File", callback_data=f"upload_file:{project_name}"),
         InlineKeyboardButton(text="➕ New File", callback_data=f"new_file:{project_name}")]
    ])
    # 3. File List/Actions
    zip_files = [f for f in files if f[1] == 'zip']
    other_files = [f for f in files if f[1] != 'zip']
    file_management_row = []
    if zip_files:
        file_management_row.append(InlineKeyboardButton(text=f"📦 Manage Zips ({len(zip_files)})", callback_data=f"manage_zips:{project_name}"))
    if other_files:
        file_management_row.append(InlineKeyboardButton(text=f"📂 Manage Files ({len(other_files)})", callback_data=f"manage_files:{project_name}"))
    if file_management_row:
        buttons.append(file_management_row)
    # 4. Project-level actions
    buttons.extend([
        [InlineKeyboardButton(text="📥 Download Logs", callback_data=f"download_logs:{project_name}"),
         InlineKeyboardButton(text="⭐ Favorite", callback_data=f"toggle_fav:{project_name}")],
        [InlineKeyboardButton(text="🗑️ Delete Project", callback_data=f"delete_project:{project_name}")],
        [InlineKeyboardButton(text="🏠 My Projects", callback_data="my_projects")]
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)
# Initialize database and load data
init_db()
load_data()
# Middleware for channel check
class ChannelCheckMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[types.TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: types.TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        user_id = event.from_user.id if event.from_user else None
        if user_id is None or user_id in banned_users:
            return await handler(event, data)
        if user_id == OWNER_ID:
            return await handler(event, data)
        if isinstance(event, types.Message) and event.text and event.text.startswith('/start'):
            return await handler(event, data)
        try:
            member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
            if member.status in ['member', 'administrator', 'creator']:
                return await handler(event, data)
            else:
                invite_link = await get_channel_invite_link()
                keyboard = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="📢 Join Our Channel", url=invite_link)],
                ])
                if isinstance(event, types.Message):
                    await event.answer(MEMBER_CHECK_FAILED + f"Please join to continue using the bot.", reply_markup=keyboard, parse_mode="HTML")
                elif isinstance(event, types.CallbackQuery):
                    try:
                        # CRITICAL FIX 1: Answer the callback query even if failing the check
                        await event.answer(MEMBER_CHECK_FAILED.split('\n')[0].replace('❌', '⚠️').strip(), show_alert=True)
                        # CRITICAL FIX 2: Wrap edit_text in try/except TelegramBadRequest
                        await event.message.edit_text(MEMBER_CHECK_FAILED + f"Please join to continue using the bot.", reply_markup=keyboard, parse_mode="HTML")
                    except (TelegramBadRequest, TelegramForbiddenError) as e:
                        if "message is not modified" in str(e):
                            logger.warning("Channel check failed to edit message: message not modified.")
                            pass  # Ignore "message is not modified" error
                        else:
                            await bot.send_message(user_id, MEMBER_CHECK_FAILED + f"Please join to continue using the bot.", reply_markup=keyboard, parse_mode="HTML")
                    
                return
        except Exception as e:
            logger.error(f"Channel membership check failed for {user_id}: {e}")
            if user_id in admin_ids:
                return await handler(event, data)
            if isinstance(event, types.Message):
                await event.answer("❌ **Error during channel check!** Please ensure the bot is an administrator in the channel or contact the owner.")
            elif isinstance(event, types.CallbackQuery):
                # NOTE: If we get here, the outer middleware should catch the error.
                # Attempting to answer here may cause the second traceback.
                # However, since the issue is in the inner handler, we rely on the primary fix.
                raise e # Re-raise the exception to be handled by the outer error middleware (if present)
            return
dp.message.middleware(ChannelCheckMiddleware())
dp.callback_query.middleware(ChannelCheckMiddleware())
# Command handlers
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Handle the /start command"""
    user_id = message.from_user.id
    if user_id in banned_users:
        await message.answer("🚫 <b>You are banned from using this bot!</b>\n\nContact admin for more info.", parse_mode="HTML")
        return
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        if member.status not in ['member', 'administrator', 'creator']:
            invite_link = await get_channel_invite_link()
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📢 Join Our Channel", url=invite_link)],
            ])
            await message.answer("❌ <b>Access Denied!</b>\n\nYou must join our channel to use this bot. Click the button below to join.", reply_markup=keyboard, parse_mode="HTML")
            return
    except Exception as e:
        logger.error(f"Failed initial channel check for {user_id}: {e}")
        if user_id not in admin_ids:
            await message.answer("❌ **Error during channel verification!** Please ensure the bot is an administrator in the channel or contact the owner.", parse_mode="HTML")
            return
    active_users.add(user_id)
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        now = datetime.now().isoformat()
        c.execute('INSERT OR REPLACE INTO active_users (user_id, join_date, last_active) VALUES (?, ?, ?)',
                  (user_id, now, now))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error saving active user: {e}")
    welcome_text = f"""
👋 <b>Welcome to the Python Project Hoster!</b>
<b>🎯 Key Features:</b>
🚀 Deploy Instantly: Upload your code as a .zip or .py file.
🤖 Full Control: Start, stop, restart, and view logs for all your projects.
👨‍💻 Pip Install: Install any Python package directly from Telegram (available to all users)
<b>📦 Project Tiers:</b>
🆓 Free Tier: You get 1 project slot with 512MB RAM to start.
💎 Premium Tier: Need more power? Purchase premium for {PREMIUM_PRICE} ⭐ to get unlimited projects with 1024MB Ram.
<b>💡 Get started:</b>
Click "🆕 New Project" to create your first project!
"""
    await message.answer(welcome_text, reply_markup=await get_main_keyboard(user_id), parse_mode="HTML")
# Project management handlers
@dp.callback_query(F.data == "new_project")
async def callback_new_project(callback: types.CallbackQuery, state: FSMContext):
    """Handle new project creation"""
    user_id = callback.from_user.id
    current_projects = len(user_projects.get(user_id, {}))
    limit = get_user_project_limit(user_id)
    
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    if current_projects >= limit:
        await callback.message.answer(
            f"❌ You've reached your project limit ({current_projects}/{int(limit) if limit != float('inf') else '∞'})!\n"
            f"💎 Upgrade to premium for unlimited projects!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💎 Get Premium", callback_data="get_premium")]])
        )
        return
        
    await callback.message.edit_text(
        "✍️ <b>Please enter a name for your new project</b> (e.g., my-awesome-bot).\n\n"
        "Send <code>/cancel</code> to abort.",
        parse_mode="HTML"
    )
    await state.set_state(ProjectStates.waiting_for_project_name)
    
@dp.message(ProjectStates.waiting_for_project_name)
async def process_project_name(message: types.Message, state: FSMContext):
    """Process the project name"""
    user_id = message.from_user.id
    project_name = message.text.strip()
    if project_name.lower() == "/cancel":
        await message.answer("❌ Project creation cancelled.")
        await state.clear()
        return
    if not project_name or len(project_name) > 50:
        await message.answer(
            "❌ Invalid project name!\n\n"
            "Please enter a name between 1-50 characters.\n"
            "Send <code>/cancel</code> to abort.",
            parse_mode="HTML"
        )
        return
    if user_id in user_projects and project_name in user_projects[user_id]:
        await message.answer(
            f"❌ You already have a project named '{project_name}'.\n\n"
            "Please choose a different name or use <code>/cancel</code> to abort.",
            parse_mode="HTML"
        )
        return
    await state.update_data(project_name=project_name)
    await message.answer(
        f"✅ Project name set to: <code>{project_name}</code>\n\n"
        "📤 <b>Please upload the project's .py file or a .zip archive.</b>\n"
        f"Max file size: {MAX_FILE_SIZE // (1024 * 1024)} MB.\n\n"
        "Send <code>/cancel</code> to abort.",
        parse_mode="HTML"
    )
    await state.set_state(ProjectStates.waiting_for_project_file)
    
@dp.message(ProjectStates.waiting_for_project_file, F.document)
async def process_project_file(message: types.Message, state: FSMContext):
    """Process the project file upload"""
    user_id = message.from_user.id
    document = message.document
    if document.file_size > MAX_FILE_SIZE:
        await message.answer(
            f"❌ File too large! Max size is {MAX_FILE_SIZE // (1024 * 1024)} MB.\n\n"
            "Please upload a smaller file or use <code>/cancel</code> to abort.",
            parse_mode="HTML"
        )
        return
    user_data = await state.get_data()
    project_name = user_data.get('project_name')
    if not project_name:
        await message.answer("❌ Error: Project name not found. Please start over.")
        await state.clear()
        return
    project_dir = UPLOAD_BOTS_DIR / str(user_id) / project_name
    project_dir.mkdir(parents=True, exist_ok=True)
    file_name = document.file_name
    file_path = project_dir / file_name
    download_msg = await message.answer(f"📥 Downloading {file_name}...")
    try:
        await bot.download(document, destination=file_path)
        file_ext = os.path.splitext(file_name)[1].lower()
        # Determine run command based on initial file
        run_command = 'python3 main.py'
        if file_ext == '.py':
            run_command = f'python3 {file_name}'
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        now = datetime.now().isoformat()
        c.execute('''INSERT INTO projects
                     (user_id, project_name, created_at, last_updated, file_count, run_command)
                     VALUES (?, ?, ?, ?, ?, ?)''',
                  (user_id, project_name, now, now, 1, run_command))
        c.execute('''INSERT INTO project_files
                     (user_id, project_name, file_name, file_type, upload_date)
                     VALUES (?, ?, ?, ?, ?)''',
                  (user_id, project_name, file_name, file_ext[1:], now))
        c.execute('UPDATE bot_stats SET stat_value = stat_value + 1 WHERE stat_name = ?', ('total_uploads',))
        c.execute('UPDATE bot_stats SET stat_value = stat_value + 1 WHERE stat_name = ?', ('total_projects',))
        conn.commit()
        conn.close()
        if user_id not in user_projects:
            user_projects[user_id] = {}
        user_projects[user_id][project_name] = {
            'created_at': now,
            'last_updated': now,
            'file_count': 1,
            'run_command': run_command,
            'files': [(file_name, file_ext[1:])]
        }
        await bot.delete_message(chat_id=message.chat.id, message_id=download_msg.message_id)
        if file_ext == '.zip':
            has_requirements = False
            try:
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    if 'requirements.txt' in zip_ref.namelist():
                        has_requirements = True
            except zipfile.BadZipFile:
                pass
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📦 Extract ZIP", callback_data=f"extract_zip:{project_name}:{file_name}")],
                [InlineKeyboardButton(text="📁 View Project", callback_data=f"view_project:{project_name}")],
                [InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
            ])
            success_text = f"""
╔═══════════════════════╗
    ✅ <b>PROJECT CREATED!</b> ✅
╚═══════════════════════╝
📁 <b>Project:</b> <code>{project_name}</code>
📄 <b>File:</b> {file_name}
💾 <b>Size:</b> {document.file_size / (1024 * 1024):.2f} MB
📅 <b>Created:</b> {datetime.now().strftime('%Y-%m-%d %H:%M')}
💡 Run Command: <code>{run_command}</code>
"""
            if has_requirements:
                success_text += "\n💡 Found requirements.txt - extract the ZIP to install dependencies automatically"
            await message.answer(success_text, reply_markup=keyboard, parse_mode="HTML")
        else:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="▶️ Run Project", callback_data=f"run_script:{project_name}:main_file")],
                [InlineKeyboardButton(text="📁 View Project", callback_data=f"view_project:{project_name}")],
                [InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
            ])
            await message.answer(
                f"""╔═══════════════════════╗
    ✅ <b>PROJECT CREATED!</b> ✅
╚═══════════════════════╝
📁 <b>Project:</b> <code>{project_name}</code>
📄 <b>File:</b> {file_name}
💾 <b>Size:</b> {document.file_size / (1024 * 1024):.2f} MB
📅 <b>Created:</b> {datetime.now().strftime('%Y-%m-%d %H:%M')}
💡 Run Command: <code>{run_command}</code>
💡 You can now run your script or upload more files to this project!""",
                reply_markup=keyboard,
                parse_mode="HTML"
            )
        await state.clear()
    except Exception as e:
        logger.error(f"Error creating project: {e}")
        await message.answer(f"❌ Error creating project: {str(e)}")
        await state.clear()
        
@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message, state: FSMContext):
    """Handle cancel command"""
    current_state = await state.get_state()
    if current_state:
        await message.answer("❌ Operation cancelled.")
        await state.clear()
    else:
        await message.answer("❌ No operation to cancel.")
        
@dp.callback_query(F.data == "my_projects")
async def callback_my_projects(callback: types.CallbackQuery):
    """Handle my projects callback"""
    user_id = callback.from_user.id
    
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    projects = user_projects.get(user_id, {})
    if not projects:
        text = """
╔═══════════════════════╗
    📁 <b>MY PROJECTS</b> 📁
╚═══════════════════════╝
📭 <b>No projects found!</b>
Create your first project by clicking
"🆕 New Project" in the main menu! 🚀
"""
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🆕 New Project", callback_data="new_project")],
            [InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
        ])
    else:
        text = f"""
╔═══════════════════════╗
    📁 <b>MY PROJECTS ({len(projects)})</b> 📁
╚═══════════════════════╝
"""
        buttons = []
        for project_name, project_data in projects.items():
            is_favorite = project_name in user_favorites.get(user_id, [])
            star = "⭐" if is_favorite else "☆"
            text += f"📁 {star} <code>{project_name}</code>\n"
            text += f"    📄 Files: {project_data['file_count']}\n"
            text += f"    📅 Created: {datetime.fromisoformat(project_data['created_at']).strftime('%Y-%m-%d')}\n"
            text += f"    💻 Command: <code>{project_data['run_command']}</code>\n\n"
            buttons.append([
                InlineKeyboardButton(text=f"📂 Open {project_name[:15]}", callback_data=f"view_project:{project_name}"),
                InlineKeyboardButton(text=f"{star}", callback_data=f"toggle_fav:{project_name}")
            ])
        buttons.extend([
            [InlineKeyboardButton(text="🆕 New Project", callback_data="new_project")],
            [InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
        ])
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    
@dp.callback_query(F.data.startswith("view_project:"))
async def callback_view_project(callback: types.CallbackQuery):
    """Handle view project callback"""
    user_id = callback.from_user.id
    project_name = callback.data.split(":", 1)[1]
    
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    if user_id not in user_projects or project_name not in user_projects[user_id]:
        await callback.message.answer("❌ Project not found!", reply_markup=await get_main_keyboard(user_id))
        return
        
    project = user_projects[user_id][project_name]
    files = project['files']
    file_list_str = "\n".join([f" • <code>{f[0]}</code> ({f[1].upper()})" for f in files])
    text = f"""
╔═══════════════════════╗
    📂 <b>PROJECT: {project_name}</b>
╚═══════════════════════╝
📅 <b>Created:</b> {datetime.fromisoformat(project['created_at']).strftime('%Y-%m-%d %H:%M')}
📅 <b>Last Updated:</b> {datetime.fromisoformat(project['last_updated']).strftime('%Y-%m-%d %H:%M')}
📄 <b>Files:</b> {project['file_count']}
💻 <b>Run Command:</b> <code>{project['run_command']}</code>
<b>📋 FILES:</b>
"""
    if not files:
        text += "No files in this project yet.\n"
    else:
        text += file_list_str + "\n"
    try:
        await callback.message.edit_text(
            text,
            reply_markup=get_project_keyboard(user_id, project_name),
            parse_mode="HTML"
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
             logger.warning("View project failed: message not modified.")
             pass # Ignore benign error
        else:
            raise e
    
@dp.callback_query(F.data.startswith("upload_file:"))
async def callback_upload_file(callback: types.CallbackQuery, state: FSMContext):
    """Handle upload file to project callback"""
    user_id = callback.from_user.id
    project_name = callback.data.split(":", 1)[1]
    
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    if user_id not in user_projects or project_name not in user_projects[user_id]:
        await callback.message.answer("❌ Project not found!")
        return
        
    await callback.message.edit_text(
        f"""📤 <b>UPLOAD FILE TO {project_name}</b>
Supported formats:
👨‍💻 Python (.py)
🟨 JavaScript (.js)
📦 ZIP Archives (.zip)
Max file size: {MAX_FILE_SIZE // (1024 * 1024)} MB
Send your file now or /cancel to abort.""",
        parse_mode="HTML"
    )
    
@dp.message(F.document)
async def handle_document(message: types.Message):
    """Handle document uploads"""
    user_id = message.from_user.id
    document = message.document
    file_name = document.file_name
    file_ext = os.path.splitext(file_name)[1].lower()
    # Try to find the project name from the reply
    project_name = None
    if message.reply_to_message and "UPLOAD FILE TO" in message.reply_to_message.text:
        try:
            # Safely extract project name from the reply message
            project_name = message.reply_to_message.text.split("UPLOAD FILE TO ")[1].split("\n")[0].strip()
        except IndexError:
            pass

    if not project_name:
        await message.answer("❌ Please use the upload button in a project to add files, or reply to the upload prompt.")
        return
        
    if user_id not in user_projects or project_name not in user_projects[user_id]:
        await message.answer("❌ Project not found. Please try again.")
        return
        
    if document.file_size > MAX_FILE_SIZE:
        await message.answer(
            f"❌ File too large! Max size is {MAX_FILE_SIZE // (1024 * 1024)} MB.",
            parse_mode="HTML"
        )
        return
        
    if file_ext not in ['.py', '.js', '.zip']:
        await message.answer("❌ Only .py, .js, and .zip files are supported!")
        return
        
    project_dir = UPLOAD_BOTS_DIR / str(user_id) / project_name
    project_dir.mkdir(parents=True, exist_ok=True)
    file_path = project_dir / file_name
    download_msg = await message.answer(f"📥 Downloading {file_name}...")
    
    try:
        await bot.download(document, destination=file_path)
        file_ext = os.path.splitext(file_name)[1].lower()
        
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        now = datetime.now().isoformat()
        
        # Check if file already exists in project_files
        c.execute('''SELECT 1 FROM project_files
                     WHERE user_id = ? AND project_name = ? AND file_name = ?''',
                  (user_id, project_name, file_name))
        is_new_file = not c.fetchone()
        
        if is_new_file:
            c.execute('''UPDATE projects
                         SET file_count = file_count + 1, last_updated = ?
                         WHERE user_id = ? AND project_name = ?''',
                      (now, user_id, project_name))
            c.execute('''INSERT INTO project_files
                         (user_id, project_name, file_name, file_type, upload_date)
                         VALUES (?, ?, ?, ?, ?)''',
                      (user_id, project_name, file_name, file_ext[1:], now))
            c.execute('UPDATE bot_stats SET stat_value = stat_value + 1 WHERE stat_name = ?', ('total_uploads',))
            conn.commit()
            
            if file_name not in [f[0] for f in user_projects[user_id][project_name]['files']]:
                user_projects[user_id][project_name]['files'].append((file_name, file_ext[1:]))
                user_projects[user_id][project_name]['file_count'] += 1
                user_projects[user_id][project_name]['last_updated'] = now
        else:
             # File exists, update timestamp only
             c.execute('''UPDATE projects SET last_updated = ? WHERE user_id = ? AND project_name = ?''', (now, user_id, project_name))
             conn.commit()
             # Log that the file was overwritten
             logger.info(f"File {file_name} overwritten in project {project_name}")
             
        conn.close()
        
        if file_ext == '.zip':
            has_requirements = False
            try:
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    if 'requirements.txt' in zip_ref.namelist():
                        has_requirements = True
            except zipfile.BadZipFile:
                pass
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📦 Extract ZIP", callback_data=f"extract_zip:{project_name}:{file_name}")],
                [InlineKeyboardButton(text="📁 View Project", callback_data=f"view_project:{project_name}")],
                [InlineKeyboardButton(text="🏠 My Projects", callback_data="my_projects")]
            ])
            success_text = f"""
╔═══════════════════════╗
    ✅ <b>FILE UPLOADED!</b> ✅
╚═══════════════════════╝
📁 <b>Project:</b> <code>{project_name}</code>
📄 <b>File:</b> {file_name}
💾 <b>Size:</b> {document.file_size / (1024 * 1024):.2f} MB
📅 <b>Uploaded:</b> {datetime.now().strftime('%Y-%m-%d %H:%M')}
"""
            if has_requirements:
                success_text += "\n\n💡 Found requirements.txt - extract the ZIP to install dependencies automatically"
            await message.answer(success_text, reply_markup=keyboard, parse_mode="HTML")
        else:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="▶️ Run Project", callback_data=f"run_script:{project_name}:main_file")],
                [InlineKeyboardButton(text="📁 View Project", callback_data=f"view_project:{project_name}")],
                [InlineKeyboardButton(text="🏠 My Projects", callback_data="my_projects")]
            ])
            await message.answer(
                f"""╔═══════════════════════╗
    ✅ <b>FILE UPLOADED!</b> ✅
╚═══════════════════════╝
📁 <b>Project:</b> <code>{project_name}</code>
📄 <b>File:</b> {file_name}
💾 <b>Size:</b> {document.file_size / (1024 * 1024):.2f} MB
📅 <b>Uploaded:</b> {datetime.now().strftime('%Y-%m-%d %H:%M')}
💡 You can now run your script or upload more files!""",
                reply_markup=keyboard,
                parse_mode="HTML"
            )
    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        await message.answer(f"❌ Error uploading file: {str(e)}")
        
@dp.callback_query(F.data.startswith("extract_zip:"))
async def callback_extract_zip(callback: types.CallbackQuery):
    """Handle ZIP file extraction"""
    user_id = callback.from_user.id
    _, project_name, file_name = callback.data.split(":")
    
    # CRITICAL FIX: Answer the callback immediately to prevent timeout
    await callback.answer(f"⏳ Starting extraction of {file_name}...", show_alert=False) 

    if user_id not in user_projects or project_name not in user_projects[user_id]:
        # Cannot use callback.answer/message.edit_text reliably if project not found, send new message
        await bot.send_message(user_id, "❌ Project not found!")
        return
        
    project_dir = UPLOAD_BOTS_DIR / str(user_id) / project_name
    zip_path = project_dir / file_name
    
    if not zip_path.exists():
        await bot.send_message(user_id, "❌ ZIP file not found!")
        return
        
    if not zipfile.is_zipfile(zip_path):
        await bot.send_message(user_id, "❌ Invalid ZIP file!")
        return

    status_text = f"""
╔═══════════════════════╗
    📦 <b>EXTRACTING ZIP</b> 📦
╚═══════════════════════╝
📁 <b>Project:</b> <code>{project_name}</code>
📄 <b>File:</b> <code>{file_name}</code>
⏳ <b>Status:</b> Extracting...
Please wait...
"""
    status_msg = await callback.message.edit_text(status_text, parse_mode="HTML")
    
    try:
        # Extract the ZIP in a separate thread
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: zipfile.ZipFile(zip_path, 'r').extractall(project_dir))
        
        all_files = []
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                all_files = zip_ref.namelist()
        except zipfile.BadZipFile:
            pass
            
        has_requirements = "requirements.txt" in all_files
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        now = datetime.now().isoformat()
        extracted_count = 0
        
        for extracted_file in all_files:
            if extracted_file.endswith('/') or extracted_file.startswith('__MACOSX'):
                continue
                
            file_path_obj = Path(extracted_file)
            file_ext = file_path_obj.suffix.lower()
            just_name = file_path_obj.name
            
            # Only register Python/JS files as project files for now
            if file_ext in ['.py', '.js']:
                c.execute('''SELECT 1 FROM project_files
                             WHERE user_id = ? AND project_name = ? AND file_name = ?''',
                          (user_id, project_name, just_name))
                if not c.fetchone():
                    c.execute('''INSERT INTO project_files
                                     (user_id, project_name, file_name, file_type, upload_date)
                                     VALUES (?, ?, ?, ?, ?)''',
                              (user_id, project_name, just_name, file_ext[1:], now))
                    extracted_count += 1
                    if just_name not in [f[0] for f in user_projects[user_id][project_name]['files']]:
                        user_projects[user_id][project_name]['files'].append((just_name, file_ext[1:]))
                        
        c.execute('''UPDATE projects
                     SET file_count = file_count + ?, last_updated = ?
                     WHERE user_id = ? AND project_name = ?''',
                  (extracted_count, now, user_id, project_name))
                  
        if zip_path.exists():
            zip_path.unlink()
            c.execute('''DELETE FROM project_files
                         WHERE user_id = ? AND project_name = ? AND file_name = ? AND file_type = 'zip' ''',
                      (user_id, project_name, file_name))
                      
            # Update in-memory cache
            if user_id in user_projects and project_name in user_projects[user_id]:
                user_projects[user_id][project_name]['files'] = [
                    f for f in user_projects[user_id][project_name]['files'] if f[0] != file_name
                ]
                # Adjust total file count (subtract 1 for the zip, add for extracted scripts)
                user_projects[user_id][project_name]['file_count'] -= 1
                user_projects[user_id][project_name]['file_count'] += extracted_count
                
        conn.commit()
        conn.close()
        
        log_content = f"Extracted ZIP file {file_name} at {datetime.now()}\n"
        log_content += f"Extracted {len(all_files)} files, registered {extracted_count} script files\n"
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('''INSERT INTO project_logs
                     (user_id, project_name, log_content, log_date)
                     VALUES (?, ?, ?, ?)''',
                  (user_id, project_name, log_content, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        
        success_text = f"""
╔═══════════════════════╗
    ✅ <b>EXTRACTION SUCCESS!</b> ✅
╚═══════════════════════╝
📁 <b>Project:</b> <code>{project_name}</code>
📄 <b>ZIP File:</b> <code>{file_name}</code>
📊 <b>Total Extracted:</b> {len(all_files)} files
✅ <b>Registered Scripts:</b> {extracted_count} files
🗑️ <b>ZIP Deleted:</b> Automatically
✨ Extraction completed successfully!
"""
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📁 View Project", callback_data=f"view_project:{project_name}")],
            [InlineKeyboardButton(text="🏠 My Projects", callback_data="my_projects")]
        ])
        
        await status_msg.edit_text(success_text, reply_markup=keyboard, parse_mode="HTML")

        if has_requirements:
            await callback.message.answer("\n\n🔧 Found requirements.txt - installing dependencies...")
            # Note: install_dependencies is async and non-blocking
            await install_dependencies(user_id, callback.message, project_name)

    except zipfile.BadZipFile:
        await status_msg.edit_text("❌ Corrupted ZIP file! Please delete and re-upload.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📁 View Project", callback_data=f"view_project:{project_name}")]]))
    except Exception as e:
        logger.error(f"Error extracting ZIP: {e}")
        error_log = f"ZIP extraction error at {datetime.now()}\n"
        error_log += f"Error: {str(e)}\n"
        error_log += f"Project: {project_name}\n"
        error_log += f"ZIP file: {file_name}\n"
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('''INSERT INTO project_logs
                     (user_id, project_name, log_content, log_date)
                     VALUES (?, ?, ?, ?)''',
                  (user_id, project_name, error_log, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        await status_msg.edit_text(f"❌ Extraction failed: {str(e)}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📁 View Project", callback_data=f"view_project:{project_name}")]]))

async def install_dependencies(user_id: int, message: types.Message, project_name: str, file_name: Optional[str] = None) -> bool:
    """Install dependencies from requirements.txt in project directory"""
    project_dir = UPLOAD_BOTS_DIR / str(user_id) / project_name
    req_file = project_dir / "requirements.txt"
    if not req_file.exists():
        await message.answer("❌ requirements.txt not found in your project!")
        return False
    try:
        venv_path = project_dir / "venv"
        status_msg = None
        if not venv_path.exists():
            status_msg = await message.answer(
                "🔧 <b>Creating virtual environment...</b>\n\n"
                "▓░░░░░░░░░ 10%",
                parse_mode="HTML"
            )
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, lambda: venv.create(venv_path, with_pip=True))
            if status_msg:
                await status_msg.edit_text("🔧 <b>Virtual environment created!</b>\n\n"
                                             f"📦 Installing dependencies...\n\n"
                                             "▓▓▓▓░░░░░░ 40%", parse_mode="HTML")
        else:
            status_msg = await message.answer(
                "📦 <b>Installing dependencies...</b>\n\n"
                "▓▓▓▓░░░░░░ 40%\n\n"
                f"📄 Reading: <code>requirements.txt</code>",
                parse_mode="HTML"
            )
        pip_path = venv_path / "bin" / "pip"
        if not pip_path.exists():
            pip_path = venv_path / "Scripts" / "pip.exe"
        if not pip_path.exists():
            raise FileNotFoundError(f"pip executable not found in venv path: {venv_path}")
        process = await asyncio.create_subprocess_exec(
            str(pip_path), "install", "-r", str(req_file),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(project_dir)
        )
        stdout, stderr = await process.communicate()
        if process.returncode == 0:
            success_text = f"""
╔═══════════════════════╗
    ✅ <b>DEPENDENCIES INSTALLED</b> ✅
╚═══════════════════════╝
📋 <b>Success!</b>
All dependencies from requirements.txt
have been installed in a virtual environment.
💡 You can now run your scripts
"""
            keyboard_buttons = []
            if file_name:
                keyboard_buttons.append([
                    InlineKeyboardButton(text="▶️ Run Project", callback_data=f"run_script:{project_name}:main_file")
                ])
            keyboard_buttons.extend([
                [InlineKeyboardButton(text="📁 View Project", callback_data=f"view_project:{project_name}")],
                [InlineKeyboardButton(text="📥 Download Logs", callback_data=f"download_logs:{project_name}"),
                 InlineKeyboardButton(text="🏠 My Projects", callback_data="my_projects")]
            ])
            keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
            if status_msg:
                await bot.delete_message(chat_id=message.chat.id, message_id=status_msg.message_id)
            await message.answer(success_text, reply_markup=keyboard, parse_mode="HTML")
            log_content = f"Dependencies installed successfully at {datetime.now()}\n"
            log_content += f"Virtual environment created at: {venv_path}\n"
            log_content += f"Requirements file: {req_file}\n"
            conn = sqlite3.connect(DATABASE_PATH)
            c = conn.cursor()
            c.execute('''INSERT INTO project_logs
                         (user_id, project_name, log_content, log_date)
                         VALUES (?, ?, ?, ?)''',
                      (user_id, project_name, log_content, datetime.now().isoformat()))
            conn.commit()
            conn.close()
            return True
        else:
            error_msg = stderr.decode().strip() or stdout.decode().strip()
            error_log = f"Dependency installation failed at {datetime.now()}\n"
            error_log += f"Error: {error_msg[:2000]}\n"
            error_log += f"Command: pip install -r {req_file}\n"
            error_log += f"Working directory: {project_dir}\n"
            conn = sqlite3.connect(DATABASE_PATH)
            c = conn.cursor()
            c.execute('''INSERT INTO project_logs
                         (user_id, project_name, log_content, log_date)
                         VALUES (?, ?, ?, ?)''',
                      (user_id, project_name, error_log, datetime.now().isoformat()))
            conn.commit()
            conn.close()
            if status_msg:
                await bot.delete_message(chat_id=message.chat.id, message_id=status_msg.message_id)
            await message.answer(
                f"❌ <b>Installation failed!</b>\n\n"
                f"Error: <code>{error_msg[:1000]}</code>\n\n"
                f"💡 The error has been logged. You can view it in the project logs.\n"
                f"Try installing manually or check the requirements.txt file",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="📥 Download Logs", callback_data=f"download_logs:{project_name}")],
                    [InlineKeyboardButton(text="📁 View Project", callback_data=f"view_project:{project_name}")],
                    [InlineKeyboardButton(text="🏠 My Projects", callback_data="my_projects")]
                ]),
                parse_mode="HTML"
            )
            return False
    except Exception as e:
        logger.error(f"Error installing dependencies: {e}")
        error_log = f"Dependency installation error at {datetime.now()}\n"
        error_log += f"Error: {str(e)}\n"
        error_log += f"Project: {project_name}\n"
        error_log += f"Requirements file: {req_file}\n"
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('''INSERT INTO project_logs
                     (user_id, project_name, log_content, log_date)
                     VALUES (?, ?, ?, ?)''',
                  (user_id, project_name, error_log, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        if status_msg:
            await bot.delete_message(chat_id=message.chat.id, message_id=status_msg.message_id)
        await message.answer(f"❌ Error installing dependencies: {str(e)}")
        return False
        
@dp.callback_query(F.data.startswith("run_script:"))
async def callback_run_script(callback: types.CallbackQuery):
    """Handle running the project's main script using the run_command"""
    user_id = callback.from_user.id
    
    # CRITICAL FIX: Answer the callback immediately to prevent timeout
    # Use a non-alert message for faster response
    await callback.answer("🚀 Attempting to start project...", show_alert=False)
    
    try:
        _, project_name, _ = callback.data.split(":", 2)
    except ValueError:
        await callback.message.answer("❌ Invalid run command data!")
        return
        
    if user_id not in user_projects or project_name not in user_projects[user_id]:
        await callback.message.answer("❌ Project not found!")
        return
        
    project_data = user_projects[user_id][project_name]
    run_command = project_data['run_command'].strip()
    
    # Check if any script for this project is already running
    is_running = any(k.startswith(f"{user_id}_{project_name}_") for k in bot_scripts)
    if is_running:
        # Avoid edit_reply_markup here as it's redundant if the state is already 'running'
        await bot.send_message(user_id, "⚠️ Project is already running!")
        return
        
    if not run_command:
        await callback.message.answer("❌ No run command set for this project!")
        return
        
    try:
        main_file_name = run_command.split()[-1]
        file_ext = os.path.splitext(main_file_name)[1].lower()
    except IndexError:
        main_file_name = "main_script"
        
    script_key = f"{user_id}_{project_name}_main_run"
    project_dir = UPLOAD_BOTS_DIR / str(user_id) / project_name
    ram_limit = get_user_ram_limit(user_id)
    
    try:
        logs_dir = LOGS_DIR / str(user_id) / project_name
        logs_dir.mkdir(parents=True, exist_ok=True)
        log_file_path = logs_dir / f"{main_file_name.split('.')[0]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        log_file = open(log_file_path, 'w')
        
        cmd_parts = run_command.split()
        if not cmd_parts:
            raise ValueError("Run command is empty or invalid.")
            
        executable = cmd_parts[0]
        arguments = cmd_parts[1:]
        
        if executable.lower() == 'python3' or executable.lower() == 'python':
            python_path = sys.executable
            venv_path = project_dir / "venv"
            if venv_path.exists():
                venv_python = venv_path / "bin" / "python"
                if not venv_python.exists():
                    venv_python = venv_path / "Scripts" / "python.exe"
                if venv_python.exists():
                    python_path = str(venv_python)
            executable = python_path
            
        process = subprocess.Popen(
            [executable] + arguments,
            cwd=str(project_dir),
            stdout=log_file,
            stderr=log_file,
            preexec_fn=lambda: os.setpgrp()
        )
        
        bot_scripts[script_key] = {
            'process': process,
            'project_name': project_name,
            'file_name': main_file_name,
            'script_owner_id': user_id,
            'start_time': datetime.now(),
            'project_dir': str(project_dir),
            'type': file_ext[1:] if file_ext else 'unknown',
            'log_file': log_file,
            'log_path': str(log_file_path),
            'ram_limit': ram_limit
        }
        
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        run_log = f"Project run command started at {datetime.now()}\n"
        run_log += f"Process ID: {process.pid}\n"
        run_log += f"Command: {run_command}\n"
        run_log += f"Working directory: {project_dir}\n"
        run_log += f"RAM Limit: {ram_limit / (1024*1024)} MB\n"
        c.execute('''INSERT INTO project_logs
                     (user_id, project_name, log_content, log_date)
                     VALUES (?, ?, ?, ?)''',
                  (user_id, project_name, run_log, datetime.now().isoformat()))
        c.execute('UPDATE bot_stats SET stat_value = stat_value + 1 WHERE stat_name = ?', ('total_runs',))
        conn.commit()
        conn.close()
        
        bot_stats['total_runs'] = bot_stats.get('total_runs', 0) + 1
        
        await callback.message.answer(f"✅ Project **`{project_name}`** started! (PID: {process.pid})", parse_mode='Markdown')
        asyncio.create_task(monitor_script_ram(script_key))
        
        # Update button state from Run to Stop/Restart
        try:
            await callback.message.edit_reply_markup(reply_markup=get_project_keyboard(user_id, project_name))
        except TelegramBadRequest as e:
            if "message is not modified" in str(e):
                 logger.warning("Run script failed to edit message: message not modified.")
                 pass # Ignore benign error
            else:
                 raise e
        
    except Exception as e:
        logger.error(f"Error running script: {e}")
        error_log = f"Project execution error at {datetime.now()}\n"
        error_log += f"Error: {str(e)}\n"
        error_log += f"Project: {project_name}\n"
        error_log += f"Command: {run_command}\n"
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('''INSERT INTO project_logs
                     (user_id, project_name, log_content, log_date)
                     VALUES (?, ?, ?, ?)''',
                  (user_id, project_name, error_log, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        await callback.message.answer(f"❌ Error starting script: {str(e)}")
        try:
             await callback.message.edit_reply_markup(reply_markup=get_project_keyboard(user_id, project_name))
        except TelegramBadRequest as e:
            if "message is not modified" in str(e):
                 logger.warning("Run script (error path) failed to edit message: message not modified.")
                 pass # Ignore benign error
            else:
                 raise e

async def monitor_script_ram(script_key: str):
    """Monitor script RAM usage and stop if exceeding limit"""
    while script_key in bot_scripts:
        script_info = bot_scripts[script_key]
        try:
            process = script_info['process']
            parent = psutil.Process(process.pid)
            mem_info = parent.memory_info()
            ram_usage = mem_info.rss
            if ram_usage > script_info['ram_limit']:
                for child in parent.children(recursive=True):
                    child.terminate()
                parent.terminate()
                if 'log_file' in script_info and not script_info['log_file'].closed:
                    script_info['log_file'].write(
                        f"\n\n🛑 Script stopped due to RAM limit exceeded\n"
                        f"RAM Usage: {ram_usage / (1024*1024):.2f} MB\n"
                        f"RAM Limit: {script_info['ram_limit'] / (1024*1024)} MB\n"
                        f"Stopped at: {datetime.now()}\n"
                    )
                    script_info['log_file'].close()
                stop_log = f"Script {script_info['file_name']} stopped at {datetime.now()} due to RAM limit exceeded\n"
                stop_log += f"RAM Usage: {ram_usage / (1024*1024):.2f} MB\n"
                stop_log += f"RAM Limit: {script_info['ram_limit'] / (1024*1024)} MB\n"
                stop_log += f"Process ID: {process.pid}\n"
                conn = sqlite3.connect(DATABASE_PATH)
                c = conn.cursor()
                c.execute('''INSERT INTO project_logs
                             (user_id, project_name, log_content, log_date)
                             VALUES (?, ?, ?, ?)''',
                          (script_info['script_owner_id'], script_info['project_name'], stop_log, datetime.now().isoformat()))
                conn.commit()
                conn.close()
                await bot.send_message(
                    script_info['script_owner_id'],
                    f"🛑 **Project `{script_info['project_name']}` stopped!**\n\n"
                    f"Reason: RAM limit exceeded ({ram_usage / (1024*1024):.2f} MB used / {script_info['ram_limit'] / (1024*1024)} MB limit).",
                    parse_mode='Markdown'
                )
                del bot_scripts[script_key]
                break
            await asyncio.sleep(5)
        except (psutil.NoSuchProcess, ProcessLookupError):
            if 'log_file' in script_info and not script_info['log_file'].closed:
                script_info['log_file'].close()
            if script_key in bot_scripts:
                # Log graceful termination if process ended without being stopped by us
                if process.returncode is not None:
                     termination_log = f"Script {script_info['file_name']} terminated on its own at {datetime.now()}\n"
                     termination_log += f"Exit Code: {process.returncode}\n"
                     conn = sqlite3.connect(DATABASE_PATH)
                     c = conn.cursor()
                     c.execute('''INSERT INTO project_logs
                                 (user_id, project_name, log_content, log_date)
                                 VALUES (?, ?, ?, ?)''',
                                (script_info['script_owner_id'], script_info['project_name'], termination_log, datetime.now().isoformat()))
                     conn.commit()
                     conn.close()
                del bot_scripts[script_key]
            break
        except Exception as e:
            logger.error(f"Error monitoring script RAM: {e}")
            break
            
@dp.callback_query(F.data.startswith("stop_script:"))
async def callback_stop_script(callback: types.CallbackQuery):
    """Handle stopping a running script"""
    script_key = callback.data.split(":", 1)[1]
    
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer("🛑 Stopping project...", show_alert=False)
    
    if script_key not in bot_scripts:
        await callback.message.answer("❌ Project not found or already stopped!")
        return
        
    try:
        script_info = bot_scripts[script_key]
        process = script_info['process']
        log_file = script_info.get('log_file')
        file_name = script_info['file_name']
        project_name = script_info['project_name']
        user_id = script_info['script_owner_id']
        
        # Close log file gracefully before killing
        if log_file and not log_file.closed:
            log_file.close()
            
        parent = psutil.Process(process.pid)
        children = parent.children(recursive=True)
        for child in children:
            child.terminate()
        parent.terminate()
        
        stop_log = f"Script {file_name} stopped manually by user {user_id} at {datetime.now()}\n"
        stop_log += f"Process ID: {process.pid}\n"
        stop_log += f"Runtime: {(datetime.now() - script_info['start_time']).total_seconds():.2f} seconds\n"
        
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('''INSERT INTO project_logs
                     (user_id, project_name, log_content, log_date)
                     VALUES (?, ?, ?, ?)''',
                  (user_id, project_name, stop_log, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        
        del bot_scripts[script_key]
        
        # Update button state from Stop/Restart to Run
        try:
            await callback.message.edit_reply_markup(reply_markup=get_project_keyboard(user_id, project_name))
        except TelegramBadRequest as e:
            if "message is not modified" in str(e):
                 logger.warning("Stop script failed to edit message: message not modified.")
                 pass # Ignore benign error
            else:
                 raise e
                 
        await callback.message.answer(f"✅ Project **`{project_name}`** stopped successfully!", parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error stopping script: {e}")
        await callback.message.answer(f"❌ Error stopping script: {str(e)}")
        try:
             await callback.message.edit_reply_markup(reply_markup=get_project_keyboard(user_id, project_name))
        except TelegramBadRequest as e:
            if "message is not modified" in str(e):
                 logger.warning("Stop script (error path) failed to edit message: message not modified.")
                 pass # Ignore benign error
            else:
                 raise e
        
@dp.callback_query(F.data.startswith("restart_script:"))
async def callback_restart_script(callback: types.CallbackQuery):
    """Handle restarting the project's main script"""
    
    # CRITICAL FIX: Answer the callback immediately to prevent timeout
    # This must be the first thing done in a long-running callback handler.
    await callback.answer("🔄 Restarting project...", show_alert=False)
    
    script_key = callback.data.split(":", 1)[1]
    
    if script_key not in bot_scripts:
        # Since the callback is answered, send a new message instead of answering with an alert.
        await callback.message.answer("❌ Project not found or not running!")
        return
        
    # 1. Stop the current process
    try:
        script_info = bot_scripts[script_key]
        old_process = script_info['process']
        old_file_name = script_info['file_name']
        project_name = script_info['project_name']
        user_id = script_info['script_owner_id']
        
        parent = psutil.Process(old_process.pid)
        for child in parent.children(recursive=True):
            child.terminate()
        parent.terminate()
        
        if 'log_file' in script_info and not script_info['log_file'].closed:
            script_info['log_file'].close()
            
        stop_log = f"Script {old_file_name} stopped for restart at {datetime.now()}\n"
        stop_log += f"Process ID: {old_process.pid}\n"
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('''INSERT INTO project_logs
                     (user_id, project_name, log_content, log_date)
                     VALUES (?, ?, ?, ?)''',
                  (user_id, project_name, stop_log, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        
        del bot_scripts[script_key]
        
    except Exception as e:
        logger.error(f"Error stopping script for restart: {e}")
        # Use message.answer() since the callback has already been answered
        await callback.message.answer(f"❌ Error stopping script for restart: {str(e)}")
        return
        
    # 2. Start a new process
    project_dir = UPLOAD_BOTS_DIR / str(user_id) / project_name
    ram_limit = get_user_ram_limit(user_id)
    run_command = user_projects[user_id][project_name]['run_command'].strip()
    
    if not run_command:
        await callback.message.answer("❌ No run command set for this project to restart!")
        return
        
    try:
        main_file_name = run_command.split()[-1]
        file_ext = os.path.splitext(main_file_name)[1].lower()
    except IndexError:
        main_file_name = "main_script"
        
    logs_dir = LOGS_DIR / str(user_id) / project_name
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file_path = logs_dir / f"{main_file_name.split('.')[0]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_file = open(log_file_path, 'w')
    
    try:
        cmd_parts = run_command.split()
        executable = cmd_parts[0]
        arguments = cmd_parts[1:]
        
        if executable.lower() == 'python3' or executable.lower() == 'python':
            python_path = sys.executable
            venv_path = project_dir / "venv"
            if venv_path.exists():
                venv_python = venv_path / "bin" / "python"
                if not venv_python.exists():
                    venv_python = venv_path / "Scripts" / "python.exe"
                if venv_python.exists():
                    python_path = str(venv_python)
            executable = python_path
            
        new_process = subprocess.Popen(
            [executable] + arguments,
            cwd=str(project_dir),
            stdout=log_file,
            stderr=log_file,
            preexec_fn=lambda: os.setpgrp()
        )
        
        new_script_key = f"{user_id}_{project_name}_main_run"
        bot_scripts[new_script_key] = {
            'process': new_process,
            'project_name': project_name,
            'file_name': main_file_name,
            'script_owner_id': user_id,
            'start_time': datetime.now(),
            'project_dir': str(project_dir),
            'type': file_ext[1:] if file_ext else 'unknown',
            'log_file': log_file,
            'log_path': str(log_file_path),
            'ram_limit': ram_limit
        }
        
        restart_log = f"Project run command restarted at {datetime.now()}\n"
        restart_log += f"New Process ID: {new_process.pid}\n"
        restart_log += f"Command: {run_command}\n"
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('''INSERT INTO project_logs
                     (user_id, project_name, log_content, log_date)
                     VALUES (?, ?, ?, ?)''',
                  (user_id, project_name, restart_log, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        
        await callback.message.answer(f"🔄 Project **`{project_name}`** restarted successfully!", parse_mode='Markdown')
        asyncio.create_task(monitor_script_ram(new_script_key))
        
        # Update button state (should already be correct but ensures it)
        try:
            await callback.message.edit_reply_markup(reply_markup=get_project_keyboard(user_id, project_name))
        except TelegramBadRequest as e:
            if "message is not modified" in str(e):
                 logger.warning("Restart script failed to edit message: message not modified.")
                 pass # Ignore benign error
            else:
                 raise e
        
    except Exception as e:
        logger.error(f"Error restarting script: {e}")
        await callback.message.answer(f"❌ Error restarting script: {str(e)}")
        
@dp.callback_query(F.data.startswith("view_logs:"))
async def callback_view_logs(callback: types.CallbackQuery):
    """Handle view logs callback"""
    user_id = callback.from_user.id
    project_name = callback.data.split(":", 1)[1]
    
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    if user_id not in user_projects or project_name not in user_projects[user_id]:
        await callback.message.answer("❌ Project not found!")
        return
        
    logs_dir = LOGS_DIR / str(user_id) / project_name
    if not logs_dir.exists() or not any(logs_dir.iterdir()):
        await callback.message.answer("❌ No log files found!")
        return
        
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Download All Logs", callback_data=f"download_logs:{project_name}")],
        [InlineKeyboardButton(text="📁 View Project", callback_data=f"view_project:{project_name}")],
        [InlineKeyboardButton(text="🏠 My Projects", callback_data="my_projects")]
    ])
    try:
        await callback.message.edit_text(
            f"📝 <b>Logs for {project_name}</b>\n\n"
            "Click below to download all log files:",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
             logger.warning("View logs failed: message not modified.")
             pass # Ignore benign error
        else:
            raise e
    
@dp.callback_query(F.data.startswith("download_logs:"))
async def callback_download_logs(callback: types.CallbackQuery):
    """Handle download logs callback"""
    user_id = callback.from_user.id
    project_name = callback.data.split(":", 1)[1]
    
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer("⏳ Compiling and uploading logs. Please wait...", show_alert=False)
    
    if user_id not in user_projects or project_name not in user_projects[user_id]:
        await callback.message.answer("❌ Project not found!")
        return
        
    try:
        logs_dir = LOGS_DIR / str(user_id) / project_name
        if not logs_dir.exists() or not any(logs_dir.glob('*.log')):
            await callback.message.answer("❌ No log files found!")
            return
            
        zip_path = LOGS_DIR / f"{user_id}_{project_name}_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
        
        # This is a blocking operation, run in executor
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: create_zip_archive(logs_dir, zip_path))
        
        await callback.message.answer_document(
            FSInputFile(zip_path),
            caption=f"📝 <b>Log Files for {project_name}</b>\n\n"
                      f"Downloaded at: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            parse_mode="HTML"
        )
        
        # Clean up the temp zip file
        zip_path.unlink()
        
    except Exception as e:
        logger.error(f"Error downloading logs: {e}")
        await callback.message.answer(f"❌ Error downloading logs: {str(e)}")

# Helper for creating zip archive to avoid blocking the main thread
def create_zip_archive(source_dir, output_path):
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for log_file in source_dir.glob('*.log'):
            zipf.write(log_file, arcname=log_file.name)
            
@dp.callback_query(F.data.startswith("toggle_fav:"))
async def callback_toggle_favorite(callback: types.CallbackQuery):
    """Handle toggling favorites"""
    user_id = callback.from_user.id
    project_name = callback.data.split(":", 1)[1]
    
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    if user_id not in user_projects or project_name not in user_projects[user_id]:
        await callback.message.answer("❌ Project not found!")
        return
        
    if user_id not in user_favorites:
        user_favorites[user_id] = []
        
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        if project_name in user_favorites[user_id]:
            user_favorites[user_id].remove(project_name)
            c.execute('DELETE FROM favorites WHERE user_id = ? AND project_name = ?',
                      (user_id, project_name))
            
            # Use non-alert answer for status update (was already done by initial answer)
            await bot.send_message(user_id, f"❌ **`{project_name}`** removed from favorites!", parse_mode='Markdown')

        else:
            user_favorites[user_id].append(project_name)
            c.execute('INSERT OR IGNORE INTO favorites (user_id, project_name) VALUES (?, ?)',
                      (user_id, project_name))
                      
            # Use non-alert answer for status update (was already done by initial answer)
            await bot.send_message(user_id, f"⭐ **`{project_name}`** added to favorites!", parse_mode='Markdown')
            
        conn.commit()
        conn.close()
        
        # Try to update the message based on where the callback came from
        if 'my_projects' in callback.message.text:
             await callback_my_projects(callback)
        elif 'PROJECT' in callback.message.text:
             await callback_view_project(callback)
        else:
             # Just edit the reply markup if we can't figure out where we are
             try:
                 await callback.message.edit_reply_markup(reply_markup=get_project_keyboard(user_id, project_name))
             except TelegramBadRequest as e:
                 if "message is not modified" in str(e):
                      logger.warning("Toggle fav failed to edit message: message not modified.")
                      pass # Ignore benign error
                 else:
                      raise e

    except Exception as e:
        logger.error(f"Error toggling favorite: {e}")
        await callback.message.answer(f"❌ Error: {str(e)}")
        
@dp.callback_query(F.data.startswith("delete_file:"))
async def callback_delete_file(callback: types.CallbackQuery):
    """Handle file deletion"""
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer("⏳ Deleting file...", show_alert=False)
    
    user_id = callback.from_user.id
    _, project_name, file_name = callback.data.split(":")
    
    if user_id not in user_projects or project_name not in user_projects[user_id]:
        await callback.message.answer("❌ Project not found!")
        return
        
    project_dir = UPLOAD_BOTS_DIR / str(user_id) / project_name
    file_path = project_dir / file_name
    return_callback = f"view_project:{project_name}"
    
    if file_name.lower().endswith('.zip'):
        return_callback = f"manage_zips:{project_name}"
    elif file_name.lower().endswith(('.py', '.js')):
        return_callback = f"manage_files:{project_name}"
        
    try:
        if file_path.exists():
            if file_path.is_dir():
                shutil.rmtree(file_path)
            else:
                file_path.unlink()
                
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('''DELETE FROM project_files
                     WHERE user_id = ? AND project_name = ? AND file_name = ?''',
                  (user_id, project_name, file_name))
                  
        c.execute('''UPDATE projects
                     SET file_count = file_count - 1, last_updated = ?
                     WHERE user_id = ? AND project_name = ?''',
                  (datetime.now().isoformat(), user_id, project_name))
        conn.commit()
        conn.close()
        
        # Update in-memory cache
        if user_id in user_projects and project_name in user_projects[user_id]:
            user_projects[user_id][project_name]['files'] = [
                f for f in user_projects[user_id][project_name]['files'] if f[0] != file_name
            ]
            user_projects[user_id][project_name]['file_count'] -= 1
            user_projects[user_id][project_name]['last_updated'] = datetime.now().isoformat()
            
        delete_log = f"File {file_name} deleted at {datetime.now()}\n"
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('''INSERT INTO project_logs
                     (user_id, project_name, log_content, log_date)
                     VALUES (?, ?, ?, ?)''',
                  (user_id, project_name, delete_log, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        
        await bot.send_message(user_id, f"✅ **`{file_name}`** deleted successfully!", parse_mode='Markdown')

        # Re-trigger the appropriate view function
        # This re-triggering might cause issues, simplified here to directly call view
        if return_callback.startswith("manage_files:"):
            # You need a separate function for managing files to re-implement
            await callback_view_project(callback) 
        elif return_callback.startswith("manage_zips:"):
             # You need a separate function for managing zips to re-implement
            await callback_view_project(callback) 
        else:
            await callback_view_project(callback)
            
    except Exception as e:
        logger.error(f"Error deleting file: {e}")
        await callback.message.answer(f"❌ Error deleting file: {str(e)}")

@dp.callback_query(F.data.startswith("delete_project:"))
async def callback_delete_project(callback: types.CallbackQuery):
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer("⏳ Deleting project...", show_alert=False)
    
    user_id = callback.from_user.id
    project_name = callback.data.split(":", 1)[1]
    
    if user_id not in user_projects or project_name not in user_projects[user_id]:
        await callback.message.answer("❌ Project not found!")
        return
        
    try:
        # Stop any running scripts for this project first
        keys_to_delete = [k for k in bot_scripts if k.startswith(f"{user_id}_{project_name}_")]
        for key in keys_to_delete:
            if key in bot_scripts:
                script_info = bot_scripts[key]
                try:
                    process = script_info['process']
                    parent = psutil.Process(process.pid)
                    for child in parent.children(recursive=True):
                        child.terminate()
                    parent.terminate()
                    if 'log_file' in script_info and not script_info['log_file'].closed:
                        script_info['log_file'].close()
                except Exception as e:
                    logger.warning(f"Failed to kill process {key} during project delete: {e}")
                finally:
                    del bot_scripts[key]
                    
        project_dir = UPLOAD_BOTS_DIR / str(user_id) / project_name
        if project_dir.exists():
            shutil.rmtree(project_dir)
            
        logs_dir = LOGS_DIR / str(user_id) / project_name
        if logs_dir.exists():
            shutil.rmtree(logs_dir)
            
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('DELETE FROM projects WHERE user_id = ? AND project_name = ?',
                  (user_id, project_name))
        c.execute('DELETE FROM project_files WHERE user_id = ? AND project_name = ?',
                  (user_id, project_name))
        c.execute('DELETE FROM project_logs WHERE user_id = ? AND project_name = ?',
                  (user_id, project_name))
        c.execute('DELETE FROM favorites WHERE user_id = ? AND project_name = ?',
                  (user_id, project_name))
        conn.commit()
        conn.close()
        
        if user_id in user_projects and project_name in user_projects[user_id]:
            del user_projects[user_id][project_name]
            
        if user_id in user_favorites and project_name in user_favorites[user_id]:
            user_favorites[user_id].remove(project_name)
            
        await callback.message.answer(f"✅ Project **`{project_name}`** deleted successfully!", parse_mode='Markdown')
        await callback_my_projects(callback)
        
    except Exception as e:
        logger.error(f"Error deleting project: {e}")
        await callback.message.answer(f"❌ Error deleting project: {str(e)}")
        
@dp.callback_query(F.data == "my_favorites")
async def callback_my_favorites(callback: types.CallbackQuery):
    """Handle my favorites callback"""
    user_id = callback.from_user.id
    
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    favorites = user_favorites.get(user_id, [])
    if not favorites:
        text = """
╔═══════════════════════╗
    ⭐ <b>FAVORITE PROJECTS</b> ⭐
╚═══════════════════════╝
📭 <b>No favorite projects yet!</b>
Add projects to favorites for quick access! 🚀
"""
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📁 My Projects", callback_data="my_projects")],
            [InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
        ])
    else:
        text = f"""
╔═══════════════════════╗
    ⭐ <b>FAVORITE PROJECTS ({len(favorites)})</b> ⭐
╚═══════════════════════╝
"""
        buttons = []
        for project_name in favorites:
            if user_id in user_projects and project_name in user_projects[user_id]:
                project = user_projects[user_id][project_name]
                text += f"📁 ⭐ <code>{project_name}</code>\n"
                text += f"    📄 Files: {project['file_count']}\n"
                text += f"    📅 Created: {datetime.fromisoformat(project['created_at']).strftime('%Y-%m-%d')}\n"
                text += f"    💻 Command: <code>{project['run_command']}</code>\n\n"
                buttons.append([
                    InlineKeyboardButton(text=f"📂 Open {project_name[:15]}", callback_data=f"view_project:{project_name}"),
                    InlineKeyboardButton(text=f"❌", callback_data=f"toggle_fav:{project_name}")
                ])
        buttons.extend([
            [InlineKeyboardButton(text="📁 My Projects", callback_data="my_projects")],
            [InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
        ])
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    
@dp.callback_query(F.data == "search_projects")
async def callback_search_projects(callback: types.CallbackQuery):
    """Handle search projects callback"""
    user_id = callback.from_user.id
    
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    projects = user_projects.get(user_id, {})
    text = f"""
╔═══════════════════════╗
    🔍 <b>SEARCH PROJECTS</b> 🔍
╚═══════════════════════╝
📊 <b>Total Projects:</b> {len(projects)}
<b>💡 To search:</b>
Use <code>/searchproject project_name</code>
"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📁 My Projects", callback_data="my_projects")],
        [InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
    ])
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")

@dp.callback_query(F.data == "bot_speed")
async def callback_bot_speed(callback: types.CallbackQuery):
    start_time = datetime.now()
    
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer("⚡ Testing...")
    
    end_time = datetime.now()
    speed = (end_time - start_time).total_seconds() * 1000
    if speed < 100:
        status = "🟢 Excellent"
        emoji = "🚀"
    elif speed < 300:
        status = "🟡 Good"
        emoji = "⚡"
    else:
        status = "🔴 Slow"
        emoji = "🐌"
    text = f"""
╔═══════════════════════╗
    ⚡ <b>SPEED TEST</b> ⚡
╚═══════════════════════╝
{emoji} <b>Response Time:</b> {speed:.2f}ms
📊 <b>Status:</b> {status}
🖥️ <b>Server Info:</b>
• CPU: {psutil.cpu_percent()}%
• Memory: {psutil.virtual_memory().percent}%
• Uptime: Online ✅
✨ Bot is running smoothly!
"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Test Again", callback_data="bot_speed")],
        [InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
    ])
    try:
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
             logger.warning("Bot speed failed to edit message: message not modified.")
             pass # Ignore benign error
        else:
            raise e

@dp.callback_query(F.data == "statistics")
async def callback_statistics(callback: types.CallbackQuery):
    """Handle the statistics callback with RAM limits"""
    user_id = callback.from_user.id
    
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    project_count = len(user_projects.get(user_id, {}))
    fav_count = len(user_favorites.get(user_id, []))
    limit = get_user_project_limit(user_id)
    ram_limit = get_user_ram_limit(user_id)
    is_premium = user_id in user_subscriptions and user_subscriptions[user_id]['expiry'] > datetime.now()
    total_files = sum(
        project['file_count']
        for project in user_projects.get(user_id, {}).values()
    )
    running_scripts = sum(
        1 for k in bot_scripts.keys()
        if k.startswith(f"{user_id}_")
    )
    text = f"""
╔═══════════════════════╗
    📊 <b>YOUR STATISTICS</b> 📊
╚═══════════════════════╝
👤 <b>User:</b> {callback.from_user.full_name}
🆔 <b>ID:</b> <code>{user_id}</code>
💎 <b>Account:</b> {'Premium ✨' if is_premium else 'Free 🆓'}
<b>📦 PROJECT STATISTICS:</b>
📁 Projects: {project_count}/{'∞' if limit == float('inf') else int(limit)}
⭐ Favorites: {fav_count}
📄 Total Files: {total_files}
🚀 Running Scripts: {running_scripts}
💾 RAM Limit: {ram_limit / (1024*1024)} MB
<b>📈 USAGE STATISTICS:</b>
📤 Uploads: {bot_stats.get('total_uploads', 0)}
▶️ Script Runs: {bot_stats.get('total_runs', 0)}
📥 Log Downloads: {bot_stats.get('total_downloads', 0)}
{'✅ Bot Status: Active' if not bot_locked else '🔒 Bot: Maintenance'}
<b>🎯 YOUR TIER:</b>
{'💎 Premium Tier: 1GB RAM, Unlimited projects' if is_premium else '🆓 Free Tier: 512MB RAM, 1 project'}
"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💎 Get Premium" if not is_premium else "✨ My Account",
                              callback_data="get_premium" if not is_premium else "statistics")],
        [InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
    ])
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")

@dp.callback_query(F.data == "get_premium")
async def callback_get_premium(callback: types.CallbackQuery):
    """Handle get premium callback with full premium plan info"""
    user_id = callback.from_user.id
    
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    if user_id in user_subscriptions and user_subscriptions[user_id]['expiry'] > datetime.now():
        await callback.message.answer("✅ You already have premium!", reply_markup=await get_main_keyboard(user_id))
        return
        
    premium_text = f"""
╔═══════════════════════╗
    💎 <b>PREMIUM PLAN</b> 💎
╚═══════════════════════╝
✨ <b>UPGRADE TO PREMIUM FOR {PREMIUM_PRICE}⭐:</b>
📦 Unlimited projects
💾 1GB RAM per project
💬 Priority support
⭐ Premium badge
🎯 Exclusive features
<b>💰 PRICING:</b>
{PREMIUM_PRICE} Telegram Stars for 1 month
<b>💬 HOW TO UPGRADE:</b>
Click the button below to purchase premium with Telegram Stars:
"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💳 Buy Premium ({PREMIUM_PRICE}⭐)", callback_data="buy_premium")],
        [InlineKeyboardButton(text="💬 Contact Owner", url=f"https://t.me/{YOUR_USERNAME.replace('@', '')}")],
        [InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
    ])
    await callback.message.edit_text(premium_text, reply_markup=keyboard, parse_mode="HTML")

@dp.callback_query(F.data == "buy_premium")
async def callback_buy_premium(callback: types.CallbackQuery):
    """Handle buy premium callback"""
    user_id = callback.from_user.id
    
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    if user_id in user_subscriptions and user_subscriptions[user_id]['expiry'] > datetime.now():
        await callback.message.answer("✅ You already have premium!", reply_markup=await get_main_keyboard(user_id))
        return
        
    try:
        await callback.message.answer_invoice(
            title="Premium Subscription (1 Month)",
            description=f"Upgrade to premium for {PREMIUM_PRICE} Telegram Stars. Get unlimited projects and 1GB RAM ",
            payload=f"premium_{user_id}",
            provider_token="",
            currency="XTR",
            prices=[types.LabeledPrice(label="Premium Subscription (1 Month)", amount=PREMIUM_PRICE * 1)],
            need_name=False,
            need_phone_number=False,
            need_email=False,
            need_shipping_address=False,
            is_flexible=False
        )
    except Exception as e:
        logger.error(f"Error creating premium invoice: {e}")
        await callback.message.answer(f"❌ Error creating invoice: {str(e)}")

@dp.pre_checkout_query()
async def precheckout_query(pre_checkout_q: types.PreCheckoutQuery):
    """Handle pre-checkout query"""
    await bot.answer_pre_checkout_query(pre_checkout_q.id, ok=True)
    
@dp.message(F.successful_payment)
async def successful_payment(message: types.Message):
    """Handle successful payment"""
    if message.successful_payment.invoice_payload.startswith("premium_"):
        user_id = int(message.successful_payment.invoice_payload.split("_")[1])
        expiry = datetime.now() + timedelta(days=30)
        user_subscriptions[user_id] = {'expiry': expiry}
        
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('INSERT OR REPLACE INTO subscriptions (user_id, expiry) VALUES (?, ?)',
                  (user_id, expiry.isoformat()))
        conn.commit()
        conn.close()
        
        await message.answer(
            f"""✅ <b>Payment Successful!</b>
💎 You now have premium access until {expiry.strftime('%Y-%m-%d')}.
<b>🎉 Premium Benefits:</b>
📦 Unlimited projects
💾 1GB RAM per project
📊 Advanced analytics
💬 Priority support
⭐ Premium badge
Enjoy your premium experience!""",
            parse_mode="HTML"
        )
# Admin panel handlers
@dp.callback_query(F.data == "admin_panel")
async def callback_admin_panel(callback: types.CallbackQuery):
    """Handle the admin panel callback"""
    user_id = callback.from_user.id
    
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    if user_id not in admin_ids:
        await callback.message.answer("❌ Admin access required!")
        return
        
    text = """
╔═══════════════════════╗
    👑 <b>ADMIN PANEL</b> 👑
╚═══════════════════════╝
<b>🎛️ ADMIN CONTROL CENTER</b>
Manage users, projects, system settings
and monitor bot performance.
<b>📊 20+ Admin Features Available!</b>
Select an option below to continue...
"""
    await callback.message.edit_text(text, reply_markup=get_admin_panel_keyboard(), parse_mode="HTML")
    
@dp.callback_query(F.data == "admin_total_users")
async def callback_admin_total_users(callback: types.CallbackQuery):
    """Handle admin total users callback"""
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
        
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    try:
        free_users = sum(1 for uid in active_users if uid not in user_subscriptions and uid not in admin_ids)
        premium_users = sum(1 for uid in user_subscriptions if user_subscriptions[uid]['expiry'] > datetime.now())
        admin_count = len(admin_ids)
        banned_count = len(banned_users)
        
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('''SELECT user_id, join_date FROM active_users
                     ORDER BY join_date DESC LIMIT 10''')
        recent_users = c.fetchall()
        conn.close()
        
        text = f"""
╔═══════════════════════╗
    👥 <b>USER STATISTICS</b> 👥
╚═══════════════════════╝
📊 <b>Total Users:</b> {len(active_users)}
👑 <b>Admins:</b> {admin_count}
💎 <b>Premium:</b> {premium_users}
🆓 <b>Free:</b> {free_users}
🚫 <b>Banned:</b> {banned_count}
<b>📅 Recent Users (10):</b>
"""
        for user_id, join_date in recent_users:
            join_date = datetime.fromisoformat(join_date)
            user_type = "Admin" if user_id in admin_ids else "Premium" if user_id in user_subscriptions and user_subscriptions[user_id]['expiry'] > datetime.now() else "Free"
            text += f"• <code>{user_id}</code> - {join_date.strftime('%Y-%m-%d')} ({user_type})\n"
            
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📁 Project Stats", callback_data="admin_total_projects")],
            [InlineKeyboardButton(text="🔍 List User Projects", callback_data="admin_list_user_projects")],
            [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
        ])
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Error in admin_total_users: {e}")
        await callback.message.answer(f"❌ Error: {str(e)}")
        
@dp.callback_query(F.data == "admin_list_user_projects")
async def callback_admin_list_user_projects(callback: types.CallbackQuery):
    """Handle listing all user projects for admin"""
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
        
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    text = """
╔═══════════════════════╗
    👥 <b>LIST USER PROJECTS</b> 👥
╚═══════════════════════╝
<b>📋 Instructions:</b>
Send a command in this format:
<code>/userprojects USER_ID</code>
Example:
<code>/userprojects 123456789</code>
This will show all projects for user with ID 123456789
"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 User Stats", callback_data="admin_total_users")],
        [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
    ])
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    
@dp.message(Command("userprojects"))
async def cmd_user_projects(message: types.Message):
    """Handle /userprojects command"""
    if message.from_user.id not in admin_ids:
        await message.answer("❌ Admin only!")
        return
        
    try:
        args = message.text.split()
        if len(args) != 2:
            await message.answer("Usage: /userprojects USER_ID")
            return
            
        target_user_id = int(args[1])
        if target_user_id not in user_projects or not user_projects[target_user_id]:
            await message.answer(f"❌ User **`{target_user_id}`** has no projects or doesn't exist!", parse_mode='Markdown')
            return
            
        projects = user_projects[target_user_id]
        text = f"""
╔═══════════════════════╗
    📁 <b>PROJECTS FOR USER {target_user_id}</b> 📁
╚═══════════════════════╝
<b>📊 Total Projects:</b> {len(projects)}
"""
        for project_name, project_data in projects.items():
            text += f"\n📁 <code>{project_name}</code>\n"
            text += f"    📄 Files: {project_data['file_count']}\n"
            text += f"    📅 Created: {datetime.fromisoformat(project_data['created_at']).strftime('%Y-%m-%d %H:%M')}\n"
            text += f"    💻 Command: <code>{project_data['run_command']}</code>\n"
            running_scripts = [k for k in bot_scripts.keys() if k.startswith(f"{target_user_id}_{project_name}_")]
            if running_scripts:
                text += f"    🚀 Running Scripts: {len(running_scripts)}\n"
                for script_key in running_scripts:
                    script_info = bot_scripts[script_key]
                    runtime = (datetime.now() - script_info['start_time']).total_seconds()
                    text += f"      • {script_info['file_name']} (PID: {script_info['process'].pid}, {int(runtime)}s)\n"
                    
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🛑 Stop All User Scripts", callback_data=f"admin_stop_user_scripts:{target_user_id}"),
                InlineKeyboardButton(text="🗑️ Delete All Projects", callback_data=f"admin_delete_user_projects:{target_user_id}")
            ],
            [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
        ])
        await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
        
    except ValueError:
        await message.answer("❌ Invalid USER_ID!")
    except Exception as e:
        logger.error(f"Error in userprojects command: {e}")
        await message.answer(f"❌ Error: {str(e)}")
        
@dp.callback_query(F.data.startswith("admin_stop_user_scripts:"))
async def callback_admin_stop_user_scripts(callback: types.CallbackQuery):
    """Handle stopping all scripts for a user"""
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
        
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer("⏳ Stopping all user scripts...", show_alert=False)
    
    try:
        user_id = int(callback.data.split(":")[1])
        stopped_count = 0
        user_scripts = [k for k in bot_scripts.keys() if k.startswith(f"{user_id}_")]
        
        if not user_scripts:
            await bot.send_message(callback.from_user.id, f"❌ User **`{user_id}`** has no running scripts!", parse_mode='Markdown')
            return
            
        for script_key in user_scripts:
            script_info = bot_scripts[script_key]
            try:
                process = script_info['process']
                parent = psutil.Process(process.pid)
                for child in parent.children(recursive=True):
                    child.terminate()
                parent.terminate()
                
                if 'log_file' in script_info and not script_info['log_file'].closed:
                    script_info['log_file'].write(
                        f"\n\n🛑 Script stopped by admin at {datetime.now()}\n"
                        f"Stopped by: {callback.from_user.id}\n"
                    )
                    script_info['log_file'].close()
                    
                stop_log = f"Script {script_info['file_name']} stopped by admin {callback.from_user.id} at {datetime.now()}\n"
                stop_log += f"Process ID: {process.pid}\n"
                stop_log += f"Project: {script_info['project_name']}\n"
                conn = sqlite3.connect(DATABASE_PATH)
                c = conn.cursor()
                c.execute('''INSERT INTO project_logs
                             (user_id, project_name, log_content, log_date)
                             VALUES (?, ?, ?, ?)''',
                          (user_id, script_info['project_name'], stop_log, datetime.now().isoformat()))
                conn.commit()
                conn.close()
                
                stopped_count += 1
                del bot_scripts[script_key]
                
            except Exception as e:
                logger.error(f"Error stopping script {script_key}: {e}")
                continue
                
        await bot.send_message(callback.from_user.id, f"✅ Stopped **`{stopped_count}`** scripts for user **`{user_id}`**!", parse_mode='Markdown')
        
        # Manually trigger re-display of userprojects stats
        msg = callback.message
        msg.text = f"/userprojects {user_id}"
        await cmd_user_projects(msg)
        
    except Exception as e:
        logger.error(f"Error stopping user scripts: {e}")
        await bot.send_message(callback.from_user.id, f"❌ Error stopping user scripts: {str(e)}")
        
@dp.callback_query(F.data.startswith("admin_delete_user_projects:"))
async def callback_admin_delete_user_projects(callback: types.CallbackQuery):
    """Handle deleting all projects for a user"""
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
        
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer("⏳ Deleting all user projects...", show_alert=False)
    
    try:
        user_id = int(callback.data.split(":")[1])
        if user_id not in user_projects or not user_projects[user_id]:
            await bot.send_message(callback.from_user.id, f"❌ User **`{user_id}`** has no projects!", parse_mode='Markdown')
            return
            
        user_scripts = [k for k in bot_scripts.keys() if k.startswith(f"{user_id}_")]
        for script_key in user_scripts:
            script_info = bot_scripts[script_key]
            try:
                process = script_info['process']
                parent = psutil.Process(process.pid)
                for child in parent.children(recursive=True):
                    child.terminate()
                parent.terminate()
                if 'log_file' in script_info and not script_info['log_file'].closed:
                    script_info['log_file'].close()
                del bot_scripts[script_key]
            except:
                pass
                
        deleted_count = 0
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        
        for project_name in list(user_projects[user_id].keys()):
            try:
                project_dir = UPLOAD_BOTS_DIR / str(user_id) / project_name
                if project_dir.exists():
                    shutil.rmtree(project_dir)
                    
                logs_dir = LOGS_DIR / str(user_id) / project_name
                if logs_dir.exists():
                    shutil.rmtree(logs_dir)
                    
                c.execute('DELETE FROM projects WHERE user_id = ? AND project_name = ?',
                          (user_id, project_name))
                c.execute('DELETE FROM project_files WHERE user_id = ? AND project_name = ?',
                          (user_id, project_name))
                c.execute('DELETE FROM project_logs WHERE user_id = ? AND project_name = ?',
                          (user_id, project_name))
                c.execute('DELETE FROM favorites WHERE user_id = ? AND project_name = ?',
                          (user_id, project_name))
                          
                deleted_count += 1
                if project_name in user_projects[user_id]:
                    del user_projects[user_id][project_name]
                if user_id in user_favorites and project_name in user_favorites[user_id]:
                    user_favorites[user_id].remove(project_name)
                    
            except Exception as e:
                logger.error(f"Error deleting project {project_name}: {e}")
                continue
                
        conn.commit()
        conn.close()
        
        await bot.send_message(callback.from_user.id, f"✅ Deleted **`{deleted_count}`** projects for user **`{user_id}`**!", parse_mode='Markdown')
        await callback_admin_total_users(callback)
        
    except Exception as e:
        logger.error(f"Error deleting user projects: {e}")
        await bot.send_message(callback.from_user.id, f"❌ Error deleting user projects: {str(e)}")
        
@dp.callback_query(F.data == "admin_total_projects")
async def callback_admin_total_projects(callback: types.CallbackQuery):
    """Handle admin total projects callback"""
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
        
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    try:
        total_projects = sum(len(projects) for projects in user_projects.values())
        py_files = 0
        js_files = 0
        zip_files = 0
        
        for projects in user_projects.values():
            for project in projects.values():
                for file_name, file_type in project['files']:
                    if file_type == 'py':
                        py_files += 1
                    elif file_type == 'js':
                        js_files += 1
                    elif file_type == 'zip':
                        zip_files += 1
                        
        all_projects = []
        for user_id, projects in user_projects.items():
            for project_name, project_data in projects.items():
                all_projects.append((user_id, project_name, project_data['file_count']))
                
        top_projects = sorted(all_projects, key=lambda x: x[2], reverse=True)[:5]
        text = f"""
╔═══════════════════════╗
    📁 <b>PROJECT STATISTICS</b> 📁
╚═══════════════════════╝
📊 <b>Total Projects:</b> {total_projects}
📄 <b>Total Files:</b> {py_files + js_files + zip_files}
<b>📦 File Types:</b>
👨‍💻 Python: {py_files}
🟨 JavaScript: {js_files}
📦 ZIP: {zip_files}
<b>🏆 Top Projects (by file count):</b>
"""
        for user_id, project_name, file_count in top_projects:
            is_premium = user_id in user_subscriptions and user_subscriptions[user_id]['expiry'] > datetime.now()
            user_type = "Admin" if user_id in admin_ids else "Premium" if is_premium else "Free"
            text += f"• <code>{project_name}</code> - {file_count} files (User: {user_id}, {user_type})\n"
            
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👥 User Stats", callback_data="admin_total_users")],
            [InlineKeyboardButton(text="🚀 Running Scripts", callback_data="admin_running_scripts")],
            [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
        ])
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Error in admin_total_projects: {e}")
        await callback.message.answer(f"❌ Error: {str(e)}")
        
@dp.callback_query(F.data == "admin_running_scripts")
async def callback_admin_running_scripts(callback: types.CallbackQuery):
    """Handle admin running scripts callback"""
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
        
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    try:
        if not bot_scripts:
            text = """
╔═══════════════════════╗
    🚀 <b>RUNNING SCRIPTS</b> 🚀
╚═══════════════════════╝
💤 No scripts running currently
"""
            buttons = []
        else:
            text = f"""
╔═══════════════════════╗
    🚀 <b>RUNNING SCRIPTS ({len(bot_scripts)})</b> 🚀
╚═══════════════════════╝
"""
            buttons = []
            for script_key, info in bot_scripts.items():
                runtime = (datetime.now() - info['start_time']).total_seconds()
                is_premium = info['script_owner_id'] in user_subscriptions and user_subscriptions[info['script_owner_id']]['expiry'] > datetime.now()
                user_type = "Admin" if info['script_owner_id'] in admin_ids else "Premium" if is_premium else "Free"
                text += f"🔸 <code>{info['file_name']}</code>\n"
                text += f"    📁 Project: {info['project_name']}\n"
                text += f"    👤 User: {info['script_owner_id']} ({user_type})\n"
                text += f"    PID: {info['process'].pid}\n"
                text += f"    Runtime: {int(runtime)}s\n\n"
                buttons.append([InlineKeyboardButton(
                    text=f"🛑 Stop {info['project_name'][:15]}",
                    callback_data=f"stop_script:{script_key}"
                )])
                
        buttons.append([InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")])
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Error in admin_running_scripts: {e}")
        await callback.message.answer(f"❌ Error: {str(e)}")
        
@dp.callback_query(F.data == "admin_premium_users")
async def callback_admin_premium_users(callback: types.CallbackQuery):
    """Handle admin premium users callback"""
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
        
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    try:
        premium_users = [(u, data) for u, data in user_subscriptions.items() if data['expiry'] > datetime.now()]
        expired_users = [(u, data) for u, data in user_subscriptions.items() if data['expiry'] <= datetime.now()]
        
        text = f"""
╔═══════════════════════╗
    💎 <b>PREMIUM USERS ({len(premium_users)})</b> 💎
╚═══════════════════════╝
<b>💎 Active Premium Users:</b>
"""
        for user_id, data in premium_users:
            expiry_date = data['expiry'].strftime('%Y-%m-%d')
            days_left = (data['expiry'] - datetime.now()).days
            project_count = len(user_projects.get(user_id, {}))
            text += f"• <code>{user_id}</code> - Expires: {expiry_date} ({days_left} days)\n"
            text += f"  Projects: {project_count}\n"
            
        text += f"\n<b>⏳ Expired Premium Users ({len(expired_users)}):</b>\n"
        for user_id, data in expired_users:
            expiry_date = data['expiry'].strftime('%Y-%m-%d')
            text += f"• <code>{user_id}</code> - Expired: {expiry_date}\n"
            
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Add Premium", callback_data="admin_add_premium")],
            [InlineKeyboardButton(text="👥 User Stats", callback_data="admin_total_users")],
            [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
        ])
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Error in admin_premium_users: {e}")
        await callback.message.answer(f"❌ Error: {str(e)}")
        
@dp.callback_query(F.data == "admin_add_premium")
async def callback_admin_add_premium(callback: types.CallbackQuery):
    """Handle admin add premium callback"""
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
        
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    text = """
╔═══════════════════════╗
    💎 <b>ADD PREMIUM USER</b> 💎
╚═══════════════════════╝
<b>📋 Command:</b>
<code>/addpremium USER_ID DAYS</code>
<b>💡 Example Pricing:</b>
• 30 days: /addpremium 123456789 30
• 90 days: /addpremium 123456789 90
• 1 year: /addpremium 123456789 365
<b>🎁 Premium Benefits:</b>
• Unlimited project limit (vs 1 for free)
• Priority processing
• Advanced analytics
• Faster response time
• Premium support
• Exclusive features
• Virtual environment support
• Auto dependency installation
<b>📊 Current Premium Users:</b>
""" + str(len([u for u in user_subscriptions if user_subscriptions[u]['expiry'] > datetime.now()]))
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💎 View Premium Users", callback_data="admin_premium_users")],
        [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
    ])
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")

@dp.callback_query(F.data == "admin_analytics")
async def callback_admin_analytics(callback: types.CallbackQuery):
    """Fixed admin analytics callback"""
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
        
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        
        c.execute('SELECT COUNT(*) FROM active_users')
        total_users = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM active_users WHERE join_date >= ?',
                  ((datetime.now() - timedelta(days=7)).isoformat(),))
        new_users_week = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM active_users WHERE join_date >= ?',
                  ((datetime.now() - timedelta(days=1)).isoformat(),))
        new_users_day = c.fetchone()[0]
        
        c.execute('SELECT COUNT(*) FROM projects')
        total_projects = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM projects WHERE created_at >= ?',
                  ((datetime.now() - timedelta(days=7)).isoformat(),))
        new_projects_week = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM projects WHERE created_at >= ?',
                  ((datetime.now() - timedelta(days=1)).isoformat(),))
        new_projects_day = c.fetchone()[0]
        
        c.execute('SELECT COUNT(*) FROM project_files')
        total_files = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM project_files WHERE upload_date >= ?',
                  ((datetime.now() - timedelta(days=7)).isoformat(),))
        new_files_week = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM project_files WHERE upload_date >= ?',
                  ((datetime.now() - timedelta(days=1)).isoformat(),))
        new_files_day = c.fetchone()[0]
        
        c.execute('SELECT COUNT(*) FROM project_logs WHERE log_content LIKE ?', ('%started at%',))
        total_runs = c.fetchone()[0]
        c.execute('''SELECT COUNT(*) FROM project_logs
                     WHERE log_content LIKE ? AND log_date >= ?''',
                  ('%started at%', (datetime.now() - timedelta(days=7)).isoformat()))
        runs_week = c.fetchone()[0]
        c.execute('''SELECT COUNT(*) FROM project_logs
                     WHERE log_content LIKE ? AND log_date >= ?''',
                  ('%started at%', (datetime.now() - timedelta(days=1)).isoformat()))
        runs_day = c.fetchone()[0]
        
        conn.close()
        
        text = f"""
╔═══════════════════════╗
    📊 <b>BOT ANALYTICS DASHBOARD</b> 📊
╚═══════════════════════╝
<b>👥 USER STATISTICS:</b>
📊 Total Users: {total_users}
🆕 New Users (Week): {new_users_week}
🆕 New Users (Day): {new_users_day}
<b>📁 PROJECT STATISTICS:</b>
📊 Total Projects: {total_projects}
🆕 New Projects (Week): {new_projects_week}
🆕 New Projects (Day): {new_projects_day}
<b>📄 FILE STATISTICS:</b>
📊 Total Files: {total_files}
🆕 New Files (Week): {new_files_week}
🆕 New Files (Day): {new_files_day}
<b>▶️ SCRIPT EXECUTION:</b>
📊 Total Runs: {total_runs}
🆕 Runs (Week): {runs_week}
🆕 Runs (Day): {runs_day}
<b>🛡️ SYSTEM STATUS:</b>
🚀 Running Scripts: {len(bot_scripts)}
🔒 Bot Status: {'Locked' if bot_locked else 'Active'}
📅 Current Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
<b>📈 GROWTH TRENDS:</b>
👥 User growth: {'↑' if new_users_week > 0 else '→'} {new_users_week} this week
📁 Project growth: {'↑' if new_projects_week > 0 else '→'} {new_projects_week} this week
📄 File uploads: {'↑' if new_files_week > 0 else '→'} {new_files_week} this week
▶️ Script runs: {'↑' if runs_week > 0 else '→'} {runs_week} this week
"""
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👥 User Stats", callback_data="admin_total_users"),
             InlineKeyboardButton(text="📁 Project Stats", callback_data="admin_total_projects")],
            [InlineKeyboardButton(text="🚀 Running Scripts", callback_data="admin_running_scripts")],
            [InlineKeyboardButton(text="⚙️ System Info", callback_data="admin_system_status")],
            [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
        ])
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Error in admin analytics: {e}")
        await callback.message.answer(f"❌ Error: {str(e)}")
        
@dp.callback_query(F.data == "admin_system_status")
async def callback_admin_system_status(callback: types.CallbackQuery):
    """Handle admin system status callback"""
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
        
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    try:
        cpu = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        text = f"""
╔═══════════════════════╗
    ⚙️ <b>SYSTEM STATUS</b> ⚙️
╚═══════════════════════╝
<b>💻 CPU USAGE:</b>
• Current: {cpu}%
• {'🟢 Normal' if cpu < 70 else '🟡 High' if cpu < 90 else '🔴 Critical'}
<b>🧠 MEMORY USAGE:</b>
• System Used: {memory.percent}%
• System Free: {memory.available / (1024**3):.1f} GB
• System Total: {memory.total / (1024**3):.1f} GB
• Bot Process: {mem_info.rss / (1024 * 1024):.1f} MB
<b>💾 DISK USAGE:</b>
• Used: {disk.percent}%
• Free: {disk.free / (1024**3):.1f} GB
• Total: {disk.total / (1024**3):.1f} GB
<b>🤖 BOT STATUS:</b>
• Status: {'🔒 Locked' if bot_locked else '✅ Running'}
• Scripts Running: {len(bot_scripts)}
• Uptime: ✅ Online since {datetime.fromtimestamp(process.create_time()).strftime('%Y-%m-%d %H:%M:%S')}
• PID: {os.getpid()}
<b>📊 STORAGE USAGE:</b>
• Projects Directory: {sum(f.stat().st_size for f in UPLOAD_BOTS_DIR.glob('**/*') if f.is_file()) / (1024**3):.2f} GB
• Logs Directory: {sum(f.stat().st_size for f in LOGS_DIR.glob('**/*') if f.is_file()) / (1024**2):.2f} MB
• Database Size: {DATABASE_PATH.stat().st_size / (1024**2):.2f} MB
<b>📅 SYSTEM TIME:</b>
• Current: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
• Bot Start: {datetime.fromtimestamp(process.create_time()).strftime('%Y-%m-%d %H:%M:%S')}
"""
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Refresh", callback_data="admin_system_status")],
            [InlineKeyboardButton(text="📊 Bot Analytics", callback_data="admin_analytics")],
            [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
        ])
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Error in admin_system_status: {e}")
        await callback.message.answer(f"❌ Error: {str(e)}")
        
@dp.callback_query(F.data == "lock_bot")
async def callback_lock_bot(callback: types.CallbackQuery):
    """Handle lock/unlock bot callback"""
    global bot_locked
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
        
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    bot_locked = not bot_locked
    status = "🔒 LOCKED" if bot_locked else "🔓 UNLOCKED"
    action = "locked" if bot_locked else "unlocked"
    log_content = f"Bot {action} by admin {callback.from_user.id} at {datetime.now()}"
    
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    c.execute('''INSERT INTO project_logs
                 (user_id, project_name, log_content, log_date)
                 VALUES (?, ?, ?, ?)''',
              (callback.from_user.id, "ADMIN_ACTION", log_content, datetime.now().isoformat()))
    conn.commit()
    conn.close()
    
    await bot.send_message(callback.from_user.id, f"✅ Bot is now **`{status}`**!", parse_mode='Markdown')
    await callback_admin_panel(callback)
    
@dp.callback_query(F.data == "broadcast")
async def callback_broadcast(callback: types.CallbackQuery):
    """Handle broadcast callback"""
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
        
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    text = """
╔═══════════════════════╗
    📢 <b>BROADCAST MESSAGE</b> 📢
╚═══════════════════════╝
<b>📋 Instructions:</b>
Send a message in this format:
<code>/broadcast Your message here</code>
Example:
<code>/broadcast Hello everyone! We have a new update available!</code>
<b>📊 Statistics:</b>
• Total users: {len(active_users)}
• Will be sent to: {len([u for u in active_users if u not in banned_users])} users
• Banned users will be skipped
<b>⚠️ WARNING:</b>
• This will send a message to ALL active users
• Use this feature responsibly
• Don't spam your users
"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
    ])
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    
@dp.callback_query(F.data == "admin_clean_projects")
async def callback_admin_clean_projects(callback: types.CallbackQuery):
    """Handle admin clean projects callback"""
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
        
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    text = """
╔═══════════════════════╗
    🗑️ <b>CLEAN PROJECTS</b> 🗑️
╚═══════════════════════╝
Clean old or unused projects from the system.
<b>🗑️ Cleaning Options:</b>
1. Delete projects older than 30 days
2. Remove projects from banned users
3. Clean projects with no files (DB entry file_count=0)
4. Delete all truly empty projects (Folder empty/missing)
5. Clean temporary log files (older than 30 days)
<b>⚠️ WARNING:</b>
These actions cannot be undone!
Use with caution.
<b>📋 Command:</b>
<code>/clean OPTION_NUMBER</code>
Example:
<code>/clean 1</code> - Delete projects older than 30 days
"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
    ])
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    
@dp.callback_query(F.data == "admin_backup_db")
async def callback_admin_backup_db(callback: types.CallbackQuery):
    """Handle admin backup database callback"""
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
        
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer("⏳ Creating database backup...", show_alert=False)
    
    try:
        backup_path = IROTECH_DIR / f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        
        # This is a blocking operation, run in executor
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: create_db_backup(DATABASE_PATH, backup_path))
        
        log_content = f"Database backup created at {datetime.now()}\n"
        log_content += f"Backup file: {backup_path.name}\n"
        log_content += f"Database size: {DATABASE_PATH.stat().st_size / (1024*1024):.2f} MB\n"
        
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('''INSERT INTO project_logs
                     (user_id, project_name, log_content, log_date)
                     VALUES (?, ?, ?, ?)''',
                  (callback.from_user.id, "ADMIN_ACTION", log_content, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        
        await callback.message.answer_document(
            FSInputFile(backup_path),
            caption="💾 <b>Database Backup</b>\n\n"
                      f"Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                      f"Size: {backup_path.stat().st_size / (1024*1024):.2f} MB",
            parse_mode="HTML"
        )
        
        # Async task to delete the local backup after 24 hours
        async def delete_backup():
            await asyncio.sleep(86400)
            if backup_path.exists():
                backup_path.unlink()
        asyncio.create_task(delete_backup())
        
    except Exception as e:
        logger.error(f"Backup error: {e}")
        await callback.message.answer(f"❌ Backup failed: {str(e)}")

# Helper for database backup
def create_db_backup(source_path, target_path):
    conn = sqlite3.connect(source_path)
    backup_conn = sqlite3.connect(target_path)
    conn.backup(backup_conn)
    backup_conn.close()
    conn.close()
    
@dp.callback_query(F.data == "admin_view_logs")
async def callback_admin_view_logs(callback: types.CallbackQuery):
    """Handle admin view logs callback"""
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
        
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('''SELECT log_content, log_date FROM project_logs
                     WHERE project_name = 'ADMIN_ACTION'
                     ORDER BY log_date DESC LIMIT 20''')
        logs = c.fetchall()
        conn.close()
        
        if not logs:
            text = """
╔═══════════════════════╗
    📝 <b>ADMIN LOGS</b> 📝
╚═══════════════════════╝
📭 <b>No admin logs found.</b>
"""
        else:
            text = """
╔═══════════════════════╗
    📝 <b>ADMIN LOGS (Last 20)</b> 📝
╚═══════════════════════╝
"""
            for log_content, log_date in logs:
                log_date = datetime.fromisoformat(log_date)
                text += f"\n📅 <b>{log_date.strftime('%Y-%m-%d %H:%M')}</b>\n"
                text += f"<code>{log_content}</code>\n\n"
                
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Refresh Logs", callback_data="admin_view_logs")],
            [InlineKeyboardButton(text="🗑️ Clear Logs", callback_data="admin_clear_logs")],
            [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
        ])
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Error viewing admin logs: {e}")
        await callback.message.answer(f"❌ Error viewing logs: {str(e)}")
        
@dp.callback_query(F.data == "admin_clear_logs")
async def callback_admin_clear_logs(callback: types.CallbackQuery):
    """Handle admin clear logs callback"""
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
        
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer("⏳ Clearing old admin logs...", show_alert=False)
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('''DELETE FROM project_logs
                     WHERE project_name = 'ADMIN_ACTION'
                     AND log_date < ?''',
                  ((datetime.now() - timedelta(days=30)).isoformat(),))
        deleted_rows = c.rowcount
        
        log_content = f"Admin logs cleared (older than 30 days) by {callback.from_user.id} at {datetime.now()}. Deleted {deleted_rows} entries."
        c.execute('''INSERT INTO project_logs
                     (user_id, project_name, log_content, log_date)
                     VALUES (?, ?, ?, ?)''',
                  (callback.from_user.id, "ADMIN_ACTION", log_content, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        
        await bot.send_message(callback.from_user.id, f"✅ Old admin logs cleared! (**`{deleted_rows}`** entries)", parse_mode='Markdown')
        await callback_admin_view_logs(callback)
        
    except Exception as e:
        logger.error(f"Error clearing logs: {e}")
        await callback.message.answer(f"❌ Error clearing logs: {str(e)}")
        
@dp.callback_query(F.data == "admin_restart_bot")
async def callback_admin_restart_bot(callback: types.CallbackQuery):
    """Handle admin restart bot callback"""
    if callback.from_user.id != OWNER_ID:
        await callback.answer("❌ Owner only!", show_alert=True)
        return
        
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    text = """
╔═══════════════════════╗
    🔄 <b>RESTART BOT</b> 🔄
╚═══════════════════════╝
⚠️ <b>WARNING:</b>
This will restart the entire bot!
<b>📋 What will happen:</b>
• All running scripts will be stopped
• Users may experience brief downtime
• Current sessions will be preserved
• The bot will be back online in ~30 seconds
<b>🔴 IMPORTANT:</b>
Only use this if absolutely necessary!
Make sure to backup your database first.
<b>📌 To confirm restart:</b>
Send the command: <code>/restart confirm</code>
"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💾 Backup First", callback_data="admin_backup_db")],
        [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
    ])
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    
@dp.callback_query(F.data == "admin_add_admin")
async def callback_admin_add_admin(callback: types.CallbackQuery):
    """Handle admin add admin callback"""
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
        
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    text = """
╔═══════════════════════╗
    👨‍💼 <b>ADD ADMIN</b> 👨‍💼
╚═══════════════════════╝
<b>📋 Command:</b>
<code>/addadmin USER_ID</code>
<b>💡 Example:</b>
<code>/addadmin 123456789</code>
<b>⚠️ WARNING:</b>
Only add trusted users as admins!
"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
    ])
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    
@dp.callback_query(F.data == "admin_remove_admin")
async def callback_admin_remove_admin(callback: types.CallbackQuery):
    """Handle admin remove admin callback"""
    if callback.from_user.id != OWNER_ID:
        await callback.answer("❌ Owner only!", show_alert=True)
        return
        
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    text = """
╔═══════════════════════╗
    👨‍💼 <b>REMOVE ADMIN</b> 👨‍💼
╚═══════════════════════╝
<b>📋 Command:</b>
<code>/removeadmin USER_ID</code>
<b>💡 Example:</b>
<code>/removeadmin 123456789</code>
<b>⚠️ WARNING:</b>
Removing an admin cannot be undone!
"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
    ])
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    
@dp.callback_query(F.data == "admin_ban_user")
async def callback_admin_ban_user(callback: types.CallbackQuery):
    """Handle admin ban user callback"""
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
        
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    text = """
╔═══════════════════════╗
    🚫 <b>BAN USER</b> 🚫
╚═══════════════════════╝
<b>📋 Command:</b>
<code>/ban USER_ID [REASON]</code>
<b>💡 Example:</b>
<code>/ban 123456789 Spamming</code>
<b>⚠️ WARNING:</b>
Banned users cannot use the bot!
"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
    ])
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    
@dp.callback_query(F.data == "admin_unban_user")
async def callback_admin_unban_user(callback: types.CallbackQuery):
    """Handle admin unban user callback"""
    if callback.from_user.id not in admin_ids:
        await callback.answer("❌ Admin only!", show_alert=True)
        return
        
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    text = """
╔═══════════════════════╗
    ✅ <b>UNBAN USER</b> ✅
╚═══════════════════════╝
<b>📋 Command:</b>
<code>/unban USER_ID</code>
<b>💡 Example:</b>
<code>/unban 123456789</code>
"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_panel")]
    ])
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    
@dp.message(Command("addadmin"))
async def cmd_add_admin(message: types.Message):
    """Handle /addadmin command"""
    if message.from_user.id not in admin_ids:
        await message.answer("❌ Permission denied!")
        return
        
    try:
        args = message.text.split()
        if len(args) != 2:
            await message.answer("Usage: /addadmin USER_ID")
            return
            
        new_admin_id = int(args[1])
        if new_admin_id in admin_ids:
            await message.answer(f"✅ User **`{new_admin_id}`** is already an admin!", parse_mode='Markdown')
            return
            
        admin_ids.add(new_admin_id)
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (new_admin_id,))
        
        log_content = f"Added admin {new_admin_id} by {message.from_user.id} at {datetime.now()}"
        c.execute('''INSERT INTO project_logs
                     (user_id, project_name, log_content, log_date)
                     VALUES (?, ?, ?, ?)''',
                  (message.from_user.id, "ADMIN_ACTION", log_content, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        
        await message.answer(f"✅ User <code>{new_admin_id}</code> added as admin!", parse_mode="HTML")
        
    except ValueError:
        await message.answer("❌ Invalid USER_ID!")
    except Exception as e:
        logger.error(f"Error adding admin: {e}")
        await message.answer(f"❌ Error: {str(e)}")
        
@dp.message(Command("removeadmin"))
async def cmd_remove_admin(message: types.Message):
    """Handle /removeadmin command"""
    if message.from_user.id != OWNER_ID:
        await message.answer("❌ Only owner can remove admins!")
        return
        
    try:
        args = message.text.split()
        if len(args) != 2:
            await message.answer("Usage: /removeadmin USER_ID")
            return
            
        remove_admin_id = int(args[1])
        if remove_admin_id == OWNER_ID:
            await message.answer("❌ Cannot remove owner!")
            return
            
        if remove_admin_id not in admin_ids:
            await message.answer(f"❌ User **`{remove_admin_id}`** is not an admin!", parse_mode='Markdown')
            return
            
        admin_ids.remove(remove_admin_id)
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('DELETE FROM admins WHERE user_id = ?', (remove_admin_id,))
        
        log_content = f"Removed admin {remove_admin_id} by {message.from_user.id} at {datetime.now()}"
        c.execute('''INSERT INTO project_logs
                     (user_id, project_name, log_content, log_date)
                     VALUES (?, ?, ?, ?)''',
                  (message.from_user.id, "ADMIN_ACTION", log_content, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        
        await message.answer(f"✅ User <code>{remove_admin_id}</code> removed from admins!", parse_mode="HTML")
        
    except ValueError:
        await message.answer("❌ Invalid USER_ID!")
    except Exception as e:
        logger.error(f"Error removing admin: {e}")
        await message.answer(f"❌ Error: {str(e)}")
        
@dp.message(Command("addpremium"))
async def cmd_add_premium(message: types.Message):
    """Handle /addpremium command"""
    if message.from_user.id not in admin_ids:
        await message.answer("❌ Permission denied!")
        return
        
    try:
        args = message.text.split()
        if len(args) != 3:
            await message.answer("Usage: /addpremium USER_ID DAYS")
            return
            
        user_id = int(args[1])
        days = int(args[2])
        if days <= 0:
            await message.answer("❌ Days must be greater than 0!")
            return
            
        expiry = datetime.now() + timedelta(days=days)
        if user_id in user_subscriptions and user_subscriptions[user_id]['expiry'] > datetime.now():
            old_expiry = user_subscriptions[user_id]['expiry']
            if old_expiry > datetime.now():
                expiry = old_expiry + timedelta(days=days)
                
        user_subscriptions[user_id] = {'expiry': expiry}
        
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('INSERT OR REPLACE INTO subscriptions (user_id, expiry) VALUES (?, ?)',
                  (user_id, expiry.isoformat()))
        
        log_content = f"Added premium to user {user_id} for {days} days by {message.from_user.id} at {datetime.now()}\n"
        log_content += f"New expiry: {expiry}"
        c.execute('''INSERT INTO project_logs
                     (user_id, project_name, log_content, log_date)
                     VALUES (?, ?, ?, ?)''',
                  (message.from_user.id, "ADMIN_ACTION", log_content, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        
        await message.answer(
            f"✅ <b>Premium Added!</b>\n\n"
            f"User: <code>{user_id}</code>\n"
            f"Duration: {days} days\n"
            f"Expires: {expiry.strftime('%Y-%m-%d %H:%M')}\n\n"
            f"💡 User can now create up to {'∞' if SUBSCRIBED_USER_LIMIT == float('inf') else SUBSCRIBED_USER_LIMIT} projects!",
            parse_mode="HTML"
        )
        
    except ValueError:
        await message.answer("❌ Invalid input!")
    except Exception as e:
        logger.error(f"Error adding premium: {e}")
        await message.answer(f"❌ Error: {str(e)}")
        
@dp.message(Command("ban"))
async def cmd_ban_user(message: types.Message):
    """Handle /ban command"""
    if message.from_user.id not in admin_ids:
        await message.answer("❌ Permission denied!")
        return
        
    try:
        args = message.text.split(maxsplit=2)
        if len(args) < 2:
            await message.answer("Usage: /ban USER_ID [REASON]")
            return
            
        ban_user_id = int(args[1])
        reason = args[2] if len(args) > 2 else "No reason provided"
        
        if ban_user_id in admin_ids:
            await message.answer("❌ Cannot ban an admin!")
            return
            
        if ban_user_id in banned_users:
            await message.answer(f"⚠️ User **`{ban_user_id}`** is already banned!", parse_mode='Markdown')
            return
            
        banned_users.add(ban_user_id)
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('INSERT OR REPLACE INTO banned_users (user_id, banned_date, reason) VALUES (?, ?, ?)',
                  (ban_user_id, datetime.now().isoformat(), reason))
                  
        log_content = f"Banned user {ban_user_id} by {message.from_user.id} at {datetime.now()}\n"
        log_content += f"Reason: {reason}"
        c.execute('''INSERT INTO project_logs
                     (user_id, project_name, log_content, log_date)
                     VALUES (?, ?, ?, ?)''',
                  (message.from_user.id, "ADMIN_ACTION", log_content, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        
        # Stop all running scripts for the banned user
        scripts_to_stop = [k for k in bot_scripts.keys() if k.startswith(f"{ban_user_id}_")]
        for script_key in scripts_to_stop:
            script_info = bot_scripts[script_key]
            try:
                process = script_info['process']
                parent = psutil.Process(process.pid)
                for child in parent.children(recursive=True):
                    child.terminate()
                parent.terminate()
                if 'log_file' in script_info and not script_info['log_file'].closed:
                    script_info['log_file'].close()
                del bot_scripts[script_key]
            except:
                pass
                
        await message.answer(
            f"🚫 User <code>{ban_user_id}</code> has been banned!\n\n"
            f"Reason: {reason}\n"
            f"🛑 All running scripts stopped",
            parse_mode="HTML"
        )
        
    except ValueError:
        await message.answer("❌ Invalid USER_ID!")
    except Exception as e:
        logger.error(f"Error banning user: {e}")
        await message.answer(f"❌ Error: {str(e)}")
        
@dp.message(Command("unban"))
async def cmd_unban_user(message: types.Message):
    """Handle /unban command"""
    if message.from_user.id not in admin_ids:
        await message.answer("❌ Permission denied!")
        return
        
    try:
        args = message.text.split()
        if len(args) != 2:
            await message.answer("Usage: /unban USER_ID")
            return
            
        unban_user_id = int(args[1])
        if unban_user_id not in banned_users:
            await message.answer(f"❌ User **`{unban_user_id}`** is not banned!", parse_mode='Markdown')
            return
            
        banned_users.remove(unban_user_id)
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('DELETE FROM banned_users WHERE user_id = ?', (unban_user_id,))
        
        log_content = f"Unbanned user {unban_user_id} by {message.from_user.id} at {datetime.now()}"
        c.execute('''INSERT INTO project_logs
                     (user_id, project_name, log_content, log_date)
                     VALUES (?, ?, ?, ?)''',
                  (message.from_user.id, "ADMIN_ACTION", log_content, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        
        await message.answer(f"✅ User <code>{unban_user_id}</code> has been unbanned!", parse_mode="HTML")
        
    except ValueError:
        await message.answer("❌ Invalid USER_ID!")
    except Exception as e:
        logger.error(f"Error unbanning user: {e}")
        await message.answer(f"❌ Error: {str(e)}")
        
@dp.message(Command("broadcast"))
async def cmd_broadcast(message: types.Message):
    """Handle /broadcast command"""
    if message.from_user.id not in admin_ids:
        await message.answer("❌ Permission denied!")
        return
        
    try:
        broadcast_text = message.text.replace("/broadcast", "", 1).strip()
        if not broadcast_text:
            await message.answer("Usage: /broadcast Your message here")
            return
            
        sent_count = 0
        failed_count = 0
        status_msg = await message.answer(f"📢 Broadcasting to {len(active_users)} users...")
        
        for user_id in active_users:
            if user_id in banned_users:
                continue
            try:
                await bot.send_message(
                    user_id,
                    f"📢 <b>Announcement:</b>\n\n{broadcast_text}",
                    parse_mode="HTML",
                    disable_web_page_preview=True
                )
                sent_count += 1
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.error(f"Failed to send to {user_id}: {e}")
                failed_count += 1
                
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        log_content = f"Broadcast sent by {message.from_user.id} at {datetime.now()}\n"
        log_content += f"Message: {broadcast_text[:200]}{'...' if len(broadcast_text) > 200 else ''}\n"
        log_content += f"Recipients: {sent_count} successful, {failed_count} failed"
        c.execute('''INSERT INTO project_logs
                     (user_id, project_name, log_content, log_date)
                     VALUES (?, ?, ?, ?)''',
                  (message.from_user.id, "ADMIN_ACTION", log_content, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        
        await status_msg.edit_text(
            f"✅ <b>Broadcast Complete!</b>\n\n"
            f"✅ Sent: {sent_count}\n"
            f"❌ Failed: {failed_count}\n"
            f"📢 Message: {broadcast_text[:100]}{'...' if len(broadcast_text) > 100 else ''}",
            parse_mode="HTML"
        )
        
    except Exception as e:
        logger.error(f"Error broadcasting: {e}")
        await message.answer(f"❌ Error: {str(e)}")
        
@dp.message(Command("searchproject"))
async def cmd_search_project(message: types.Message):
    """Handle /searchproject command"""
    user_id = message.from_user.id
    try:
        args = message.text.split(maxsplit=1)
        if len(args) != 2:
            await message.answer("Usage: /searchproject project_name")
            return
            
        search_term = args[1].lower()
        user_projects_list = user_projects.get(user_id, {})
        matches = [name for name in user_projects_list.keys() if search_term in name.lower()]
        
        if not matches:
            await message.answer(
                f"🔍 No projects found matching '<code>{search_term}</code>'\n\n"
                "Try a different search term or check your project list.",
                parse_mode="HTML"
            )
            return
            
        text = f"🔍 <b>Search Results ({len(matches)})</b>\n\n"
        for project_name in matches:
            project = user_projects_list[project_name]
            is_favorite = project_name in user_favorites.get(user_id, [])
            star = "⭐" if is_favorite else "☆"
            text += f"{star} <code>{project_name}</code>\n"
            text += f"    📄 Files: {project['file_count']}\n"
            text += f"    📅 Created: {datetime.fromisoformat(project['created_at']).strftime('%Y-%m-%d')}\n"
            text += f"    💻 Command: <code>{project['run_command']}</code>\n\n"
            
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📁 My Projects", callback_data="my_projects")],
            [InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
        ])
        await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Search error: {e}")
        await message.answer(f"❌ Error: {str(e)}")
        
@dp.message(Command("newproject"))
async def cmd_new_project(message: types.Message, state: FSMContext):
    """Handle /newproject command"""
    user_id = message.from_user.id
    current_projects = len(user_projects.get(user_id, {}))
    limit = get_user_project_limit(user_id)
    
    if current_projects >= limit:
        await message.answer(
            f"❌ You've reached your project limit ({current_projects}/{int(limit) if limit != float('inf') else '∞'})!\n"
            f"💎 Upgrade to premium for unlimited projects!",
            parse_mode="HTML"
        )
        return
        
    await message.answer(
        "✍️ <b>Please enter a name for your new project</b> (e.g., my-awesome-bot).\n\n"
        "Send <code>/cancel</code> to abort.",
        parse_mode="HTML"
    )
    await state.set_state(ProjectStates.waiting_for_project_name)
    
@dp.message(Command("clean"))
async def cmd_clean(message: types.Message):
    """Handle /clean command"""
    if message.from_user.id not in admin_ids:
        await message.answer("❌ Admin only!")
        return
        
    try:
        args = message.text.split()
        if len(args) != 2:
            await message.answer(
                "Usage: /clean OPTION_NUMBER\n\n"
                "Options:\n"
                "1. Delete projects older than 30 days\n"
                "2. Remove projects from banned users\n"
                "3. Clean projects with no files\n"
                "4. Delete all empty projects\n"
                "5. Clean temporary files",
                parse_mode="HTML"
            )
            return
            
        option = int(args[1])
        deleted_count = 0
        log_entries = []
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        
        if option == 1:
            cutoff_date = (datetime.now() - timedelta(days=30)).isoformat()
            c.execute('''SELECT user_id, project_name FROM projects
                         WHERE last_updated < ?''', (cutoff_date,))
            old_projects = c.fetchall()
            for user_id, project_name in old_projects:
                try:
                    project_dir = UPLOAD_BOTS_DIR / str(user_id) / project_name
                    if project_dir.exists():
                        shutil.rmtree(project_dir)
                    logs_dir = LOGS_DIR / str(user_id) / project_name
                    if logs_dir.exists():
                        shutil.rmtree(logs_dir)
                        
                    c.execute('DELETE FROM projects WHERE user_id = ? AND project_name = ?',
                              (user_id, project_name))
                    c.execute('DELETE FROM project_files WHERE user_id = ? AND project_name = ?',
                              (user_id, project_name))
                    c.execute('DELETE FROM project_logs WHERE user_id = ? AND project_name = ?',
                              (user_id, project_name))
                    c.execute('DELETE FROM favorites WHERE user_id = ? AND project_name = ?',
                              (user_id, project_name))
                              
                    deleted_count += 1
                    log_entries.append(f"Deleted old project {project_name} (user {user_id})")
                    
                    if user_id in user_projects and project_name in user_projects[user_id]:
                        del user_projects[user_id][project_name]
                    if user_id in user_favorites and project_name in user_favorites[user_id]:
                        user_favorites[user_id].remove(project_name)
                        
                except Exception as e:
                    logger.error(f"Error deleting project {project_name}: {e}")
                    log_entries.append(f"Error deleting project {project_name}: {str(e)}")
                    
        elif option == 2:
            for user_id in banned_users:
                if user_id in user_projects:
                    for project_name in list(user_projects[user_id].keys()):
                        try:
                            project_dir = UPLOAD_BOTS_DIR / str(user_id) / project_name
                            if project_dir.exists():
                                shutil.rmtree(project_dir)
                            logs_dir = LOGS_DIR / str(user_id) / project_name
                            if logs_dir.exists():
                                shutil.rmtree(logs_dir)
                                
                            c.execute('DELETE FROM projects WHERE user_id = ? AND project_name = ?',
                                      (user_id, project_name))
                            c.execute('DELETE FROM project_files WHERE user_id = ? AND project_name = ?',
                                      (user_id, project_name))
                            c.execute('DELETE FROM project_logs WHERE user_id = ? AND project_name = ?',
                                      (user_id, project_name))
                            c.execute('DELETE FROM favorites WHERE user_id = ? AND project_name = ?',
                                      (user_id, project_name))
                                      
                            deleted_count += 1
                            log_entries.append(f"Deleted banned user's project {project_name} (user {user_id})")
                            
                            if project_name in user_projects[user_id]:
                                del user_projects[user_id][project_name]
                            if user_id in user_favorites and project_name in user_favorites[user_id]:
                                user_favorites[user_id].remove(project_name)
                                
                        except Exception as e:
                            logger.error(f"Error deleting banned user's project {project_name}: {e}")
                            log_entries.append(f"Error deleting banned user's project {project_name}: {str(e)}")
                            
        elif option == 3:
            c.execute('''SELECT user_id, project_name FROM projects
                         WHERE file_count = 0''')
            empty_projects = c.fetchall()
            for user_id, project_name in empty_projects:
                try:
                    project_dir = UPLOAD_BOTS_DIR / str(user_id) / project_name
                    if project_dir.exists():
                        shutil.rmtree(project_dir)
                    logs_dir = LOGS_DIR / str(user_id) / project_name
                    if logs_dir.exists():
                        shutil.rmtree(logs_dir)
                        
                    c.execute('DELETE FROM projects WHERE user_id = ? AND project_name = ?',
                              (user_id, project_name))
                    c.execute('DELETE FROM project_files WHERE user_id = ? AND project_name = ?',
                              (user_id, project_name))
                    c.execute('DELETE FROM project_logs WHERE user_id = ? AND project_name = ?',
                              (user_id, project_name))
                    c.execute('DELETE FROM favorites WHERE user_id = ? AND project_name = ?',
                              (user_id, project_name))
                              
                    deleted_count += 1
                    log_entries.append(f"Deleted empty DB project {project_name} (user {user_id})")
                    
                    if user_id in user_projects and project_name in user_projects[user_id]:
                        del user_projects[user_id][project_name]
                    if user_id in user_favorites and project_name in user_favorites[user_id]:
                        user_favorites[user_id].remove(project_name)
                        
                except Exception as e:
                    logger.error(f"Error deleting empty DB project {project_name}: {e}")
                    log_entries.append(f"Error deleting empty DB project {project_name}: {str(e)}")
                    
        elif option == 4:
            c.execute('SELECT user_id, project_name FROM projects')
            all_projects = c.fetchall()
            for user_id, project_name in all_projects:
                project_dir = UPLOAD_BOTS_DIR / str(user_id) / project_name
                # Check if folder doesn't exist OR if it exists but is truly empty (no files inside)
                if not project_dir.exists() or not any(f.is_file() for f in project_dir.iterdir()):
                    try:
                        if project_dir.exists():
                            shutil.rmtree(project_dir)
                        logs_dir = LOGS_DIR / str(user_id) / project_name
                        if logs_dir.exists():
                            shutil.rmtree(logs_dir)
                            
                        c.execute('DELETE FROM projects WHERE user_id = ? AND project_name = ?',
                                  (user_id, project_name))
                        c.execute('DELETE FROM project_files WHERE user_id = ? AND project_name = ?',
                                  (user_id, project_name))
                        c.execute('DELETE FROM project_logs WHERE user_id = ? AND project_name = ?',
                                  (user_id, project_name))
                        c.execute('DELETE FROM favorites WHERE user_id = ? AND project_name = ?',
                                  (user_id, project_name))
                                  
                        deleted_count += 1
                        log_entries.append(f"Deleted empty folder project {project_name} (user {user_id})")
                        
                        if user_id in user_projects and project_name in user_projects[user_id]:
                            del user_projects[user_id][project_name]
                        if user_id in user_favorites and project_name in user_favorites[user_id]:
                            user_favorites[user_id].remove(project_name)
                            
                    except Exception as e:
                        logger.error(f"Error deleting empty folder project {project_name}: {e}")
                        log_entries.append(f"Error deleting empty folder project {project_name}: {str(e)}")
                        
        elif option == 5:
            for user_dir in LOGS_DIR.iterdir():
                if user_dir.is_dir():
                    for project_dir in user_dir.iterdir():
                        if project_dir.is_dir():
                            for log_file in project_dir.glob('*.log'):
                                if (datetime.now() - datetime.fromtimestamp(log_file.stat().st_mtime)) > timedelta(days=30):
                                    try:
                                        log_file.unlink()
                                        deleted_count += 1
                                        log_entries.append(f"Deleted old log file {log_file.name}")
                                    except Exception as e:
                                        logger.error(f"Error deleting old log file {log_file}: {e}")
                                        log_entries.append(f"Error deleting old log file {log_file}: {str(e)}")
                                        
                    for project_dir in list(user_dir.iterdir()):
                        if project_dir.is_dir() and not any(project_dir.iterdir()):
                            try:
                                project_dir.rmdir()
                                deleted_count += 1
                                log_entries.append(f"Deleted empty log directory {project_dir.name}")
                            except Exception as e:
                                logger.error(f"Error deleting empty log directory {project_dir}: {e}")
                                log_entries.append(f"Error deleting empty log directory {project_dir}: {str(e)}")
                                
        else:
            await message.answer("❌ Invalid option number!")
            return
            
        conn.commit()
        conn.close()
        
        log_content = f"Cleaning operation {option} performed by {message.from_user.id} at {datetime.now()}\n"
        log_content += f"Deleted {deleted_count} items\n"
        if log_entries:
            log_content += "\nDetails:\n" + "\n".join(log_entries[:10])
            if len(log_entries) > 10:
                log_content += f"\n... and {len(log_entries) - 10} more"
                
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('''INSERT INTO project_logs
                     (user_id, project_name, log_content, log_date)
                     VALUES (?, ?, ?, ?)''',
                  (message.from_user.id, "ADMIN_ACTION", log_content, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        
        await message.answer(
            f"✅ Cleaning operation {option} completed!\n"
            f"Deleted {deleted_count} items.\n\n"
            f"📝 Details have been logged in admin logs.",
            parse_mode="HTML"
        )
        
    except ValueError:
        await message.answer("❌ Invalid option number!")
    except Exception as e:
        logger.error(f"Error during cleaning: {e}")
        await message.answer(f"❌ Error during cleaning: {str(e)}")
        
@dp.message(Command("restart"))
async def cmd_restart(message: types.Message):
    """Handle /restart command"""
    if message.from_user.id != OWNER_ID:
        await message.answer("❌ Owner only!")
        return
        
    try:
        args = message.text.split()
        if len(args) != 2 or args[1] != "confirm":
            await message.answer(
                "⚠️ To restart the bot, send:\n"
                "<code>/restart confirm</code>\n\n"
                "⚠️ This will stop all running scripts!",
                parse_mode="HTML"
            )
            return
            
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        log_content = f"Bot restart initiated by {message.from_user.id} at {datetime.now()}\n"
        log_content += f"Running scripts: {len(bot_scripts)}"
        c.execute('''INSERT INTO project_logs
                     (user_id, project_name, log_content, log_date)
                     VALUES (?, ?, ?, ?)''',
                  (message.from_user.id, "ADMIN_ACTION", log_content, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        
        await message.answer("🔄 Restarting bot... All running scripts will be stopped.")
        
        for script_key, script_info in list(bot_scripts.items()):
            try:
                process = script_info['process']
                parent = psutil.Process(process.pid)
                for child in parent.children(recursive=True):
                    child.terminate()
                parent.terminate()
                if 'log_file' in script_info and not script_info['log_file'].closed:
                    script_info['log_file'].close()
                del bot_scripts[script_key]
            except:
                pass
                
        await dp.storage.close()
        os._exit(0)
        
    except Exception as e:
        logger.error(f"Error during restart: {e}")
        await message.answer(f"❌ Error during restart: {str(e)}")
        
@dp.callback_query(F.data.startswith("edit_command:"))
async def callback_edit_command(callback: types.CallbackQuery, state: FSMContext):
    """Handle edit command callback"""
    user_id = callback.from_user.id
    project_name = callback.data.split(":", 1)[1]
    
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    if user_id not in user_projects or project_name not in user_projects[user_id]:
        await callback.message.answer("❌ Project not found!")
        return
        
    current_command = user_projects[user_id][project_name]['run_command']
    await callback.message.edit_text(
        f"""📝 <b>EDIT RUN COMMAND</b>
<b>Current command:</b>
<code>{current_command}</code>
<b>Enter the new run command for {project_name}:</b>
Example: python3 bot.py
Send <code>/cancel</code> to abort.""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏠 Back to Project", callback_data=f"view_project:{project_name}")]
        ]),
        parse_mode="HTML"
    )
    await state.update_data(project_name=project_name)
    await state.set_state(ProjectStates.waiting_for_edit_command)
    
@dp.message(ProjectStates.waiting_for_edit_command)
async def process_edit_command(message: types.Message, state: FSMContext):
    """Process the edited command"""
    user_id = message.from_user.id
    text = message.text.strip()
    
    if text.lower() == "/cancel":
        await message.answer("❌ Command edit cancelled.")
        await state.clear()
        return
        
    if not text:
        await message.answer(
            "❌ Invalid command!\n\n"
            "Please enter a valid command (e.g., python3 bot.py).\n"
            "Send <code>/cancel</code> to abort.",
            parse_mode="HTML"
        )
        return
        
    user_data = await state.get_data()
    project_name = user_data.get('project_name')
    if not project_name or user_id not in user_projects or project_name not in user_projects[user_id]:
        await message.answer("❌ Project not found. Please start over.")
        await state.clear()
        return
        
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('''UPDATE projects
                     SET run_command = ?
                     WHERE user_id = ? AND project_name = ?''',
                  (text, user_id, project_name))
        conn.commit()
        conn.close()
        
        user_projects[user_id][project_name]['run_command'] = text
        
        await message.answer(
            f"✅ Run command updated successfully!\n\n"
            f"New command: <code>{text}</code>\n\n"
            f"💡 This command will be used when you run scripts in this project.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📁 View Project", callback_data=f"view_project:{project_name}")],
                [InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_to_main")]
            ]),
            parse_mode="HTML"
        )
        await state.clear()
        
    except Exception as e:
        logger.error(f"Error updating command: {e}")
        await message.answer(f"❌ Error updating command: {str(e)}")
        await state.clear()
        
@dp.callback_query(F.data.startswith("pip_install:"))
async def callback_pip_install(callback: types.CallbackQuery, state: FSMContext):
    """Handle pip install callback - now available to ALL users"""
    user_id = callback.from_user.id
    project_name = callback.data.split(":", 1)[1]
    
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    if user_id not in user_projects or project_name not in user_projects[user_id]:
        await callback.message.answer("❌ Project not found!")
        return
        
    await callback.message.edit_text(
        f"""👨‍💻 <b>PIP INSTALL FOR {project_name}</b>
<b>📋 Instructions:</b>
Send a message in one of these formats:
<code>pip install package-name</code>
or
<code>package-name</code>
or
<code>package-name==version</code>
Examples:
<code>pip install python-telegram-bot</code>
<code>python-telegram-bot</code>
<code>requests==2.28.1</code>
<code>pip install requests==2.28.1</code>
Send <code>/cancel</code> to abort.""",
        parse_mode="HTML"
    )
    await state.update_data(project_name=project_name)
    await state.set_state(ProjectStates.waiting_for_pip_install)
    
@dp.message(ProjectStates.waiting_for_pip_install)
async def process_pip_install(message: types.Message, state: FSMContext):
    """Process pip install commands - now available to ALL users"""
    user_id = message.from_user.id
    text = message.text.strip()
    
    if text.lower() == "/cancel":
        await message.answer("❌ Pip install cancelled.")
        await state.clear()
        return
        
    package_spec = text
    if text.lower().startswith("pip install "):
        package_spec = text[len("pip install "):].strip()
        
    if not package_spec:
        await message.answer(
            "❌ Invalid package specification!\n\n"
            "Please use one of these formats:\n"
            "<code>pip install package-name</code>\n"
            "<code>package-name</code>\n"
            "<code>package-name==version</code>\n\n"
            "Send <code>/cancel</code> to abort.",
            parse_mode="HTML"
        )
        return
        
    user_data = await state.get_data()
    project_name = user_data.get('project_name')
    
    if not project_name or user_id not in user_projects or project_name not in user_projects[user_id]:
        await message.answer("❌ Project not found. Please start over.")
        await state.clear()
        return
        
    project_dir = UPLOAD_BOTS_DIR / str(user_id) / project_name
    
    try:
        venv_path = project_dir / "venv"
        status_msg = None
        
        if not venv_path.exists():
            status_msg = await message.answer(
                "🔧 <b>Creating virtual environment...</b>\n\n"
                "▓░░░░░░░░░ 10%",
                parse_mode="HTML"
            )
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, lambda: venv.create(venv_path, with_pip=True))
            
            if status_msg:
                await status_msg.edit_text(
                    "🔧 <b>Virtual environment created!</b>\n\n"
                    f"📦 Installing {package_spec}...\n\n"
                    "▓▓▓▓░░░░░░ 40%",
                    parse_mode="HTML"
                )
        else:
            status_msg = await message.answer(
                f"📦 <b>Installing {package_spec}...</b>\n\n"
                "▓▓▓▓░░░░░░ 40%",
                parse_mode="HTML"
            )
            
        pip_path = venv_path / "bin" / "pip"
        if not pip_path.exists():
            pip_path = venv_path / "Scripts" / "pip.exe"
            
        if not pip_path.exists():
            raise FileNotFoundError(f"pip executable not found in venv path: {venv_path}")
            
        process = await asyncio.create_subprocess_exec(
            str(pip_path), "install", package_spec,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(project_dir)
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode == 0:
            success_text = f"""
╔═══════════════════════╗
    ✅ <b>PACKAGE INSTALLED</b> ✅
╚═══════════════════════╝
📦 <b>Package:</b> <code>{package_spec}</code>
📁 <b>Project:</b> <code>{project_name}</code>
✅ <b>Status:</b> Successfully installed
💡 You can now use this package in your project!
"""
            keyboard_buttons = [
                [InlineKeyboardButton(text="📁 View Project", callback_data=f"view_project:{project_name}")],
                [InlineKeyboardButton(text="🏠 My Projects", callback_data="my_projects")]
            ]
            keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
            
            if status_msg:
                await bot.delete_message(chat_id=message.chat.id, message_id=status_msg.message_id)
                
            log_content = f"Package installed via Telegram at {datetime.now()}\n"
            log_content += f"Package: {package_spec}\n"
            log_content += f"Virtual environment: {venv_path}\n"
            log_content += f"Return code: {process.returncode}\n"
            
            conn = sqlite3.connect(DATABASE_PATH)
            c = conn.cursor()
            c.execute('''INSERT INTO project_logs
                         (user_id, project_name, log_content, log_date)
                         VALUES (?, ?, ?, ?)''',
                      (user_id, project_name, log_content, datetime.now().isoformat()))
            conn.commit()
            conn.close()
            
            await message.answer(success_text, reply_markup=keyboard, parse_mode="HTML")
            
        else:
            error_msg = stderr.decode().strip() or stdout.decode().strip()
            error_log = f"Package installation failed at {datetime.now()}\n"
            error_log += f"Package: {package_spec}\n"
            error_log += f"Error: {error_msg[:2000]}\n"
            error_log += f"Command: pip install {package_spec}\n"
            error_log += f"Working directory: {project_dir}\n"
            
            conn = sqlite3.connect(DATABASE_PATH)
            c = conn.cursor()
            c.execute('''INSERT INTO project_logs
                         (user_id, project_name, log_content, log_date)
                         VALUES (?, ?, ?, ?)''',
                      (user_id, project_name, error_log, datetime.now().isoformat()))
            conn.commit()
            conn.close()
            
            if status_msg:
                await bot.delete_message(chat_id=message.chat.id, message_id=status_msg.message_id)
                
            await message.answer(
                f"❌ <b>Installation failed!</b>\n\n"
                f"Package: <code>{package_spec}</code>\n"
                f"Error: <code>{error_msg[:1000]}</code>\n\n"
                f"💡 The error has been logged. You can view it in the project logs.\n"
                f"Try installing manually or check the package name",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="📥 Download Logs", callback_data=f"download_logs:{project_name}")],
                    [InlineKeyboardButton(text="📁 View Project", callback_data=f"view_project:{project_name}")],
                    [InlineKeyboardButton(text="🏠 My Projects", callback_data="my_projects")]
                ]),
                parse_mode="HTML"
            )
            
    except Exception as e:
        logger.error(f"Error installing package: {e}")
        error_log = f"Package installation error at {datetime.now()}\n"
        error_log += f"Error: {str(e)}\n"
        error_log += f"Project: {project_name}\n"
        error_log += f"Package: {package_spec}\n"
        
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('''INSERT INTO project_logs
                     (user_id, project_name, log_content, log_date)
                     VALUES (?, ?, ?, ?)''',
                  (user_id, project_name, error_log, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        
        if status_msg:
            await bot.delete_message(chat_id=message.chat.id, message_id=status_msg.message_id)
            
        await message.answer(f"❌ Error installing package: {str(e)}")
        
    await state.clear()
    
@dp.callback_query(F.data == "back_to_main")
async def callback_back_to_main(callback: types.CallbackQuery):
    """Handle the back to main menu callback"""
    user_id = callback.from_user.id
    
    # CRITICAL FIX: Answer the callback immediately
    await callback.answer()
    
    await callback.message.edit_text(
        "🏠 <b>MAIN MENU</b>\n\nSelect an option below:",
        reply_markup=await get_main_keyboard(user_id),
        parse_mode="HTML"
    )

async def web_server():
    """Run a simple web server for health checks"""
    app = web.Application()
    async def handle(request):
        return web.Response(text="🚀 Project Host Bot - Powered by Aiogram & Aiohttp!")
    async def health_check(request):
        status = {
            "status": "running",
            "timestamp": datetime.now().isoformat(),
            "active_users": len(active_users),
            "running_scripts": len(bot_scripts),
            "total_projects": sum(len(projects) for projects in user_projects.values()),
            "bot_locked": bot_locked
        }
        return web.json_response(status)
    app.router.add_get('/', handle)
    app.router.add_get('/health', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 5000)
    await site.start()
    logger.info("🌐 Web server started on port 5000")

async def cleanup_old_scripts():
    """Background task to clean up old script processes"""
    while True:
        await asyncio.sleep(3600)  # Check every hour
        current_time = datetime.now()
        scripts_to_clean = []
        for script_key, script_info in list(bot_scripts.items()):
            try:
                process = script_info['process']
                if process.poll() is not None:  # Process has terminated
                    scripts_to_clean.append(script_key)
                    continue
            except Exception as e:
                logger.error(f"Error checking script {script_key}: {e}")
                scripts_to_clean.append(script_key)
                
        for script_key in scripts_to_clean:
            if script_key in bot_scripts:
                script_info = bot_scripts[script_key]
                if 'log_file' in script_info and not script_info['log_file'].closed:
                    script_info['log_file'].close()
                del bot_scripts[script_key]
                
async def main():
    """Main entry point"""
    logger.info("🚀 Starting Project Host Bot...")
    # NOTE: The web server task must be created before starting the dispatcher.
    # We do not need to await it, as it runs forever.
    asyncio.create_task(web_server())
    asyncio.create_task(cleanup_old_scripts())
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Error during dp.start_polling: {e}", exc_info=True)


if __name__ == "__main__":
    try:
        # NOTE: aiogram 3+ recommends asyncio.run(main())
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped manually by user (KeyboardInterrupt)")
    except Exception as e:
        logger.error(f"An unexpected error occurred in main: {e}", exc_info=True)