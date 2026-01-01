import telebot
import subprocess
import os
import zipfile
import tempfile
import shutil
from telebot import types
import time
from datetime import datetime, timedelta
import psutil
import sqlite3
import json
import logging
import signal
import threading
import re
import sys
import atexit
import requests

# --- Flask Keep Alive ---
from flask import Flask
from threading import Thread

app = Flask('')

@app.route('/')
def home():
    return "Blink OP Hi Kede"

def run_flask():
    port = int(os.environ.get("PORT", 2828))
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run_flask)
    t.daemon = True
    t.start()
    print("Flask Keep-Alive server started.")
# --- End Flask Keep Alive ---

# --- Configuration ---
TOKEN = '8374962247:AAEl9_yFT72Ydtn980KN17UM5kr2-ZYIcwc'
OWNER_ID = '7863737666'
ADMIN_ID = '6716407197'
YOUR_USERNAME = '@S_1xG'
UPDATE_CHANNEL = 'https://t.me/xb1ns'

# Folder setup
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_BOTS_DIR = os.path.join(BASE_DIR, 'upload_bots')
IROTECH_DIR = os.path.join(BASE_DIR, 'inf')
DATABASE_PATH = os.path.join(IROTECH_DIR, 'bot_data.db')

# File upload limits
FREE_USER_LIMIT = 11
SUBSCRIBED_USER_LIMIT = 53
ADMIN_LIMIT = 9999
OWNER_LIMIT = float('inf')

# Create necessary directories
os.makedirs(UPLOAD_BOTS_DIR, exist_ok=True)
os.makedirs(IROTECH_DIR, exist_ok=True)

# Initialize bot
bot = telebot.TeleBot(TOKEN)

# --- Data structures ---
bot_scripts = {}
user_subscriptions = {}
user_files = {}
active_users = set()
admin_ids = {ADMIN_ID, OWNER_ID}
bot_locked = False

# Lists of potentially malicious patterns in code
MALICIOUS_PATTERNS = [
    r'os\.system\s*\([^)]*shutdown[^)]*\)',
    r'os\.system\s*\([^)]*reboot[^)]*\)',
    r'os\.system\s*\([^)]*rm\s+-rf[^)]*\)',
    r'subprocess\.call\s*\([^)]*shutdown[^)]*\)',
    r'subprocess\.call\s*\([^)]*reboot[^)]*\)',
    r'import\s+os\s*,\s*sys\s*;\s*sys\.exit\(0\)',
    r'while\s+True:\s*pass',
    r'import\s+signal\s*;\s*signal\.alarm\(1\)',
    r'eval\s*\([^)]*open\([^)]*\)[^)]*\)',
    r'__import__\s*\(\s*[\'"][\w\s]*os[\w\s]*[\'"]\s*\)\.system',
    r'import\s+ctypes\s*;\s*ctypes\.windll',
    r'import\s+platform\s*;\s*platform\.system\(\)',
]

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Command Button Layouts ---
COMMAND_BUTTONS_LAYOUT_USER_SPEC = [
    ["👀 Updates Channel"],
    ["🪨 Upload File", "💎 Check Files"],
    ["🏂 Bot Speed", "📊 Statistics"],
    ["🗿 Contact Owner"]
]

ADMIN_COMMAND_BUTTONS_LAYOUT_USER_SPEC = [
    ["👀 Updates Channel"],
    ["🪨 Upload File", "💎 Check Files"],
    ["🏂 Bot Speed", "📊 Statistics"],
    ["💤 Subscriptions", "☔ Broadcast"],
    ["😎 Lock Bot", "💤 Running All Code"],
    ["👀 Admin Panel", "🗿 Contact Owner"],
    ["🚫 Ban User", "✅ Unban User"]  # Added ban/unban buttons
]

# --- Database Setup ---
def init_db():
    """Initialize the database with required tables"""
    logger.info(f"Initializing database at: {DATABASE_PATH}")
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS subscriptions
                     (user_id INTEGER PRIMARY KEY, expiry TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS user_files
                     (user_id INTEGER, file_name TEXT, file_type TEXT,
                      PRIMARY KEY (user_id, file_name))''')
        c.execute('''CREATE TABLE IF NOT EXISTS active_users
                     (user_id INTEGER PRIMARY KEY)''')
        c.execute('''CREATE TABLE IF NOT EXISTS admins
                     (user_id INTEGER PRIMARY KEY)''')
        c.execute('''CREATE TABLE IF NOT EXISTS banned_users
                     (user_id INTEGER PRIMARY KEY, reason TEXT, banned_by TEXT, banned_at TEXT)''')  # Added banned users table
        # Ensure owner and initial admin are in admins table
        c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (OWNER_ID,))
        if ADMIN_ID != OWNER_ID:
             c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (ADMIN_ID,))
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"❌ Database initialization error: {e}", exc_info=True)

def load_data():
    """Load data from database into memory"""
    logger.info("Loading data from database...")
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()

        # Load subscriptions
        c.execute('SELECT user_id, expiry FROM subscriptions')
        for user_id, expiry in c.fetchall():
            try:
                user_subscriptions[user_id] = {'expiry': datetime.fromisoformat(expiry)}
            except ValueError:
                logger.warning(f"⚠️ Invalid expiry date format for user {user_id}: {expiry}. Skipping.")

        # Load user files
        c.execute('SELECT user_id, file_name, file_type FROM user_files')
        for user_id, file_name, file_type in c.fetchall():
            if user_id not in user_files:
                user_files[user_id] = []
            user_files[user_id].append((file_name, file_type))

        # Load active users
        c.execute('SELECT user_id FROM active_users')
        active_users.update(user_id for (user_id,) in c.fetchall())

        # Load admins
        c.execute('SELECT user_id FROM admins')
        admin_ids.update(user_id for (user_id,) in c.fetchall())

        conn.close()
        logger.info(f"Data loaded: {len(active_users)} users, {len(user_subscriptions)} subscriptions, {len(admin_ids)} admins.")
    except Exception as e:
        logger.error(f"❌ Error loading data: {e}", exc_info=True)

# Initialize DB and Load Data at startup
init_db()
load_data()

# --- Ban/Unban System ---
def is_user_banned(user_id):
    """Check if a user is banned"""
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute('SELECT 1 FROM banned_users WHERE user_id = ?', (user_id,))
        result = c.fetchone() is not None
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Error checking if user {user_id} is banned: {e}")
        return False

def ban_user(user_id, reason="No reason provided", banned_by="System"):
    """Ban a user and stop all their running bots"""
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        banned_at = datetime.now().isoformat()
        c.execute('INSERT OR REPLACE INTO banned_users (user_id, reason, banned_by, banned_at) VALUES (?, ?, ?, ?)',
                  (user_id, reason, banned_by, banned_at))
        conn.commit()
        conn.close()
        
        # Stop all running bots for this user
        stop_all_user_bots(user_id)
        
        logger.warning(f"User {user_id} banned by {banned_by}. Reason: {reason}")
        return True
    except Exception as e:
        logger.error(f"Error banning user {user_id}: {e}")
        return False

def unban_user(user_id):
    """Unban a user"""
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute('DELETE FROM banned_users WHERE user_id = ?', (user_id,))
        conn.commit()
        deleted = c.rowcount > 0
        conn.close()
        
        if deleted:
            logger.info(f"User {user_id} unbanned.")
        return deleted
    except Exception as e:
        logger.error(f"Error unbanning user {user_id}: {e}")
        return False

def stop_all_user_bots(user_id):
    """Stop all running bots for a specific user"""
    stopped_count = 0
    script_keys_to_remove = []
    
    for script_key, script_info in list(bot_scripts.items()):
        if script_info.get('script_owner_id') == user_id:
            logger.info(f"Stopping bot {script_key} for banned user {user_id}")
            kill_process_tree(script_info)
            script_keys_to_remove.append(script_key)
            stopped_count += 1
    
    # Clean up from bot_scripts dict
    for key in script_keys_to_remove:
        if key in bot_scripts:
            del bot_scripts[key]
    
    logger.info(f"Stopped {stopped_count} bots for banned user {user_id}")
    return stopped_count

# --- Helper Functions ---
def get_user_folder(user_id):
    """Get or create user's folder for storing files"""
    user_folder = os.path.join(UPLOAD_BOTS_DIR, str(user_id))
    os.makedirs(user_folder, exist_ok=True)
    return user_folder

def get_user_file_limit(user_id):
    """Get the file upload limit for a user"""
    if is_user_banned(user_id):
        return 0  # Banned users cannot upload files
    
    if user_id == OWNER_ID: return OWNER_LIMIT
    if user_id in admin_ids: return ADMIN_LIMIT
    if user_id in user_subscriptions and user_subscriptions[user_id]['expiry'] > datetime.now():
        return SUBSCRIBED_USER_LIMIT
    return FREE_USER_LIMIT

def get_user_file_count(user_id):
    """Get the number of files uploaded by a user"""
    return len(user_files.get(user_id, []))

def is_bot_running(script_owner_id, file_name):
    """Check if a bot script is currently running for a specific user"""
    script_key = f"{script_owner_id}_{file_name}"
    script_info = bot_scripts.get(script_key)
    if script_info and script_info.get('process'):
        try:
            proc = psutil.Process(script_info['process'].pid)
            is_running = proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
            if not is_running:
                logger.warning(f"Process {script_info['process'].pid} for {script_key} found in memory but not running/zombie. Cleaning up.")
                if 'log_file' in script_info and hasattr(script_info['log_file'], 'close') and not script_info['log_file'].closed:
                    try:
                        script_info['log_file'].close()
                    except Exception as log_e:
                        logger.error(f"Error closing log file during zombie cleanup {script_key}: {log_e}")
                if script_key in bot_scripts:
                    del bot_scripts[script_key]
            return is_running
        except psutil.NoSuchProcess:
            logger.warning(f"Process for {script_key} not found (NoSuchProcess). Cleaning up.")
            if 'log_file' in script_info and hasattr(script_info['log_file'], 'close') and not script_info['log_file'].closed:
                try:
                     script_info['log_file'].close()
                except Exception as log_e:
                     logger.error(f"Error closing log file during cleanup of non-existent process {script_key}: {log_e}")
            if script_key in bot_scripts:
                 del bot_scripts[script_key]
            return False
        except Exception as e:
            logger.error(f"Error checking process status for {script_key}: {e}", exc_info=True)
            return False
    return False

def kill_process_tree(process_info):
    """Kill a process and all its children, ensuring log file is closed."""
    pid = None
    log_file_closed = False
    script_key = process_info.get('script_key', 'N/A') 

    try:
        if 'log_file' in process_info and hasattr(process_info['log_file'], 'close') and not process_info['log_file'].closed:
            try:
                process_info['log_file'].close()
                log_file_closed = True
                logger.info(f"Closed log file for {script_key} (PID: {process_info.get('process', {}).get('pid', 'N/A')})")
            except Exception as log_e:
                logger.error(f"Error closing log file during kill for {script_key}: {log_e}")

        process = process_info.get('process')
        if process and hasattr(process, 'pid'):
           pid = process.pid
           if pid: 
                try:
                    parent = psutil.Process(pid)
                    children = parent.children(recursive=True)
                    logger.info(f"Attempting to kill process tree for {script_key} (PID: {pid}, Children: {[c.pid for c in children]})")

                    for child in children:
                        try:
                            child.terminate()
                            logger.info(f"Terminated child process {child.pid} for {script_key}")
                        except psutil.NoSuchProcess:
                            logger.warning(f"Child process {child.pid} for {script_key} already gone.")
                        except Exception as e:
                            logger.error(f"Error terminating child {child.pid} for {script_key}: {e}. Trying kill...")
                            try: child.kill(); logger.info(f"Killed child process {child.pid} for {script_key}")
                            except Exception as e2: logger.error(f"Failed to kill child {child.pid} for {script_key}: {e2}")

                    gone, alive = psutil.wait_procs(children, timeout=1)
                    for p in alive:
                        logger.warning(f"Child process {p.pid} for {script_key} still alive. Killing.")
                        try: p.kill()
                        except Exception as e: logger.error(f"Failed to kill child {p.pid} for {script_key} after wait: {e}")

                    try:
                        parent.terminate()
                        logger.info(f"Terminated parent process {pid} for {script_key}")
                        try: parent.wait(timeout=1)
                        except psutil.TimeoutExpired:
                            logger.warning(f"Parent process {pid} for {script_key} did not terminate. Killing.")
                            parent.kill()
                            logger.info(f"Killed parent process {pid} for {script_key}")
                    except psutil.NoSuchProcess:
                        logger.warning(f"Parent process {pid} for {script_key} already gone.")
                    except Exception as e:
                        logger.error(f"Error terminating parent {pid} for {script_key}: {e}. Trying kill...")
                        try: parent.kill(); logger.info(f"Killed parent process {pid} for {script_key}")
                        except Exception as e2: logger.error(f"Failed to kill parent {pid} for {script_key}: {e2}")

                except psutil.NoSuchProcess:
                    logger.warning(f"Process {pid or 'N/A'} for {script_key} not found during kill. Already terminated?")
           else: logger.error(f"Process PID is None for {script_key}.")
        elif log_file_closed: logger.warning(f"Process object missing for {script_key}, but log file closed.")
        else: logger.error(f"Process object missing for {script_key}, and no log file. Cannot kill.")
    except Exception as e:
        logger.error(f"❌ Unexpected error killing process tree for PID {pid or 'N/A'} ({script_key}): {e}", exc_info=True)

# --- Malicious File Detection ---
def check_for_malicious_code(file_content, file_name):
    """Check if file contains potentially malicious code"""
    suspicious_patterns = []
    
    for pattern in MALICIOUS_PATTERNS:
        if re.search(pattern, file_content, re.IGNORECASE):
            suspicious_patterns.append(pattern)
    
    # Additional checks for JS files
    if file_name.endswith('.js'):
        js_patterns = [
            r'process\.exit\s*\(\)',
            r'process\.kill\s*\(\)',
            r'require\s*\(\s*[\'"][\w\s]*child_process[\w\s]*[\'"]\s*\)\.exec',
            r'require\s*\(\s*[\'"][\w\s]*fs[\w\s]*[\'"]\s*\)\.unlinkSync',
            r'while\s*\(\s*true\s*\)\s*\{',
            r'setInterval\s*\(\s*function\s*\(\s*\)\s*\{\s*\},\s*\d+\s*\)',
        ]
        for pattern in js_patterns:
            if re.search(pattern, file_content, re.IGNORECASE):
                suspicious_patterns.append(pattern)
    
    return suspicious_patterns

def send_file_for_review(message, file_content, file_name, user_id, user_name):
    """Send file to owner for review with approve/reject buttons"""
    # Create inline keyboard for review
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("✅ Confirm & Notify", callback_data=f"confirm_file_{user_id}_{file_name}"),
        types.InlineKeyboardButton("❌ Cancel & Notify", callback_data=f"reject_file_{user_id}_{file_name}")
    )
    
    # Send warning to owner
    warning_msg = (f"⚠️ POTENTIALLY MALICIOUS FILE DETECTED!\n\n"
                   f"👤 User: {user_name} (ID: `{user_id}`)\n"
                   f"📁 File: `{file_name}`\n\n"
                   f"File has been flagged as potentially malicious.\n"
                   f"Review the file and choose an action:")
    
    try:
        # Send file content as document for review
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix=f"_{file_name}", delete=False, encoding='utf-8')
        temp_file.write(file_content[:50000])  # Limit size for review
        temp_file.close()
        
        with open(temp_file.name, 'rb') as f:
            bot.send_document(OWNER_ID, f, caption=warning_msg, reply_markup=markup, parse_mode='Markdown')
        
        # Notify user that file is under review
        bot.reply_to(message, "⚠️ Your file has been flagged for security review. It will be checked by the admin before running.")
        
        os.unlink(temp_file.name)
        return True
    except Exception as e:
        logger.error(f"Error sending file for review: {e}")
        bot.reply_to(message, "❌ Error processing file. Please try again.")
        return False

# --- Automatic Package Installation & Script Running ---

def attempt_install_pip(module_name, message):
    package_name = TELEGRAM_MODULES.get(module_name.lower(), module_name) 
    if package_name is None: 
        logger.info(f"Module '{module_name}' is core. Skipping pip install.")
        return False 
    try:
        bot.reply_to(message, f"🐍 Module `{module_name}` not found. Installing `{package_name}`...", parse_mode='Markdown')
        command = [sys.executable, '-m', 'pip', 'install', package_name]
        logger.info(f"Running install: {' '.join(command)}")
        result = subprocess.run(command, capture_output=True, text=True, check=False, encoding='utf-8', errors='ignore')
        if result.returncode == 0:
            logger.info(f"Installed {package_name}. Output:\n{result.stdout}")
            bot.reply_to(message, f"✅ Package `{package_name}` (for `{module_name}`) installed.", parse_mode='Markdown')
            return True
        else:
            error_msg = f"❌ Failed to install `{package_name}` for `{module_name}`.\nLog:\n```\n{result.stderr or result.stdout}\n```"
            logger.error(error_msg)
            if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (Log truncated)"
            bot.reply_to(message, error_msg, parse_mode='Markdown')
            return False
    except Exception as e:
        error_msg = f"❌ Error installing `{package_name}`: {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message, error_msg)
        return False

# --- Fixed & Completed: attempt_install_npm ---
def attempt_install_npm(module_name, user_folder, message):
    try:
        bot.reply_to(message, f"🟠 Node package `{module_name}` not found. Installing locally in user folder...", parse_mode='Markdown')
        command = ['npm', 'install', module_name]
        logger.info(f"Running npm install in {user_folder}: {' '.join(command)}")
        
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            cwd=user_folder,
            timeout=120,  # don't hang forever
            encoding='utf-8',
            errors='ignore'
        )
        
        if result.returncode == 0:
            logger.info(f"Successfully installed npm package {module_name}")
            bot.reply_to(message, f"✅ Node package `{module_name}` installed locally.", parse_mode='Markdown')
            return True
        else:
            error_msg = f"❌ Failed to install npm package `{module_name}`.\nError:\n```\n{result.stderr[-3000:]}\n```"
            logger.error(f"npm install failed: {result.stderr}")
            bot.reply_to(message, error_msg, parse_mode='Markdown')
            return False
    except subprocess.TimeoutExpired:
        bot.reply_to(message, f"⏰ npm install for `{module_name}` timed out. Too slow or dead lock.")
        logger.error(f"npm install timeout for {module_name}")
        return False
    except Exception as e:
        logger.error(f"Exception during npm install {module_name}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Critical error installing `{module_name}`: {str(e)}")
        return False

def run_script(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt=1):
    """Run Python script."""
    max_attempts = 2 
    if attempt > max_attempts:
        bot.reply_to(message_obj_for_reply, f"❌ Failed to run '{file_name}' after {max_attempts} attempts. Check logs.")
        return

    script_key = f"{script_owner_id}_{file_name}"
    logger.info(f"Attempt {attempt} to run Python script: {script_path} (Key: {script_key}) for user {script_owner_id}")

    try:
        if not os.path.exists(script_path):
             bot.reply_to(message_obj_for_reply, f"❌ Error: Script '{file_name}' not found at '{script_path}'!")
             logger.error(f"Script not found: {script_path} for user {script_owner_id}")
             if script_owner_id in user_files:
                 user_files[script_owner_id] = [f for f in user_files.get(script_owner_id, []) if f[0] != file_name]
             remove_user_file_db(script_owner_id, file_name)
             return

        if attempt == 1:
            check_command = [sys.executable, script_path]
            logger.info(f"Running Python pre-check: {' '.join(check_command)}")
            check_proc = None
            try:
                check_proc = subprocess.Popen(check_command, cwd=user_folder, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='ignore')
                stdout, stderr = check_proc.communicate(timeout=5)
                return_code = check_proc.returncode
                logger.info(f"Python Pre-check early. RC: {return_code}. Stderr: {stderr[:200]}...")
                if return_code != 0 and stderr:
                    match_py = re.search(r"ModuleNotFoundError: No module named '(.+?)'", stderr)
                    if match_py:
                        module_name = match_py.group(1).strip().strip("'\"")
                        logger.info(f"Detected missing Python module: {module_name}")
                        if attempt_install_pip(module_name, message_obj_for_reply):
                            logger.info(f"Install OK for {module_name}. Retrying run_script...")
                            bot.reply_to(message_obj_for_reply, f"🔄 Install successful. Retrying '{file_name}'...")
                            time.sleep(2)
                            threading.Thread(target=run_script, args=(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt + 1)).start()
                            return
                        else:
                            bot.reply_to(message_obj_for_reply, f"❌ Install failed. Cannot run '{file_name}'.")
                            return
                    else:
                         error_summary = stderr[:500]
                         bot.reply_to(message_obj_for_reply, f"❌ Error in script pre-check for '{file_name}':\n```\n{error_summary}\n```\nFix the script.", parse_mode='Markdown')
                         return
            except subprocess.TimeoutExpired:
                logger.info("Python Pre-check timed out (>5s), imports likely OK. Killing check process.")
                if check_proc and check_proc.poll() is None: check_proc.kill(); check_proc.communicate()
                logger.info("Python Check process killed. Proceeding to long run.")
            except FileNotFoundError:
                 logger.error(f"Python interpreter not found: {sys.executable}")
                 bot.reply_to(message_obj_for_reply, f"❌ Error: Python interpreter '{sys.executable}' not found.")
                 return
            except Exception as e:
                 logger.error(f"Error in Python pre-check for {script_key}: {e}", exc_info=True)
                 bot.reply_to(message_obj_for_reply, f"❌ Unexpected error in script pre-check for '{file_name}': {e}")
                 return
            finally:
                 if check_proc and check_proc.poll() is None:
                     logger.warning(f"Python Check process {check_proc.pid} still running. Killing.")
                     check_proc.kill(); check_proc.communicate()

        logger.info(f"Starting long-running Python process for {script_key}")
        log_file_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        log_file = None; process = None
        try: log_file = open(log_file_path, 'w', encoding='utf-8', errors='ignore')
        except Exception as e:
             logger.error(f"Failed to open log file '{log_file_path}' for {script_key}: {e}", exc_info=True)
             bot.reply_to(message_obj_for_reply, f"❌ Failed to open log file '{log_file_path}': {e}")
             return
        try:
            startupinfo = None; creationflags = 0
            if os.name == 'nt':
                 startupinfo = subprocess.STARTUPINFO(); startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                 startupinfo.wShowWindow = subprocess.SW_HIDE
            process = subprocess.Popen(
                [sys.executable, script_path], cwd=user_folder, stdout=log_file, stderr=log_file,
                stdin=subprocess.PIPE, startupinfo=startupinfo, creationflags=creationflags,
                encoding='utf-8', errors='ignore'
            )
            logger.info(f"Started Python process {process.pid} for {script_key}")
            bot_scripts[script_key] = {
                'process': process, 'log_file': log_file, 'file_name': file_name,
                'chat_id': message_obj_for_reply.chat.id,
                'script_owner_id': script_owner_id,
                'start_time': datetime.now(), 'user_folder': user_folder, 'type': 'py', 'script_key': script_key
            }
            bot.reply_to(message_obj_for_reply, f"✅ Python script '{file_name}' started! (PID: {process.pid}) (For User: {script_owner_id})")
        except FileNotFoundError:
             logger.error(f"Python interpreter {sys.executable} not found for long run {script_key}")
             bot.reply_to(message_obj_for_reply, f"❌ Error: Python interpreter '{sys.executable}' not found.")
             if log_file and not log_file.closed: log_file.close()
             if script_key in bot_scripts: del bot_scripts[script_key]
        except Exception as e:
            if log_file and not log_file.closed: log_file.close()
            error_msg = f"❌ Error starting Python script '{file_name}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            bot.reply_to(message_obj_for_reply, error_msg)
            if process and process.poll() is None:
                 logger.warning(f"Killing potentially started Python process {process.pid} for {script_key}")
                 kill_process_tree({'process': process, 'log_file': log_file, 'script_key': script_key})
            if script_key in bot_scripts: del bot_scripts[script_key]
    except Exception as e:
        error_msg = f"❌ Unexpected error running Python script '{file_name}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message_obj_for_reply, error_msg)
        if script_key in bot_scripts:
             logger.warning(f"Cleaning up {script_key} due to error in run_script.")
             kill_process_tree(bot_scripts[script_key])
             del bot_scripts[script_key]

def run_js_script(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt=1):
    """Run JS script."""
    max_attempts = 2
    if attempt > max_attempts:
        bot.reply_to(message_obj_for_reply, f"❌ Failed to run '{file_name}' after {max_attempts} attempts. Check logs.")
        return

    script_key = f"{script_owner_id}_{file_name}"
    logger.info(f"Attempt {attempt} to run JS script: {script_path} (Key: {script_key}) for user {script_owner_id}")

    try:
        if not os.path.exists(script_path):
             bot.reply_to(message_obj_for_reply, f"❌ Error: Script '{file_name}' not found at '{script_path}'!")
             logger.error(f"JS Script not found: {script_path} for user {script_owner_id}")
             if script_owner_id in user_files:
                 user_files[script_owner_id] = [f for f in user_files.get(script_owner_id, []) if f[0] != file_name]
             remove_user_file_db(script_owner_id, file_name)
             return

        if attempt == 1:
            check_command = ['node', script_path]
            logger.info(f"Running JS pre-check: {' '.join(check_command)}")
            check_proc = None
            try:
                check_proc = subprocess.Popen(check_command, cwd=user_folder, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='ignore')
                stdout, stderr = check_proc.communicate(timeout=5)
                return_code = check_proc.returncode
                logger.info(f"JS Pre-check early. RC: {return_code}. Stderr: {stderr[:200]}...")
                if return_code != 0 and stderr:
                    match_js = re.search(r"Cannot find module '(.+?)'", stderr)
                    if match_js:
                        module_name = match_js.group(1).strip().strip("'\"")
                        if not module_name.startswith('.') and not module_name.startswith('/'):
                             logger.info(f"Detected missing Node module: {module_name}")
                             if attempt_install_npm(module_name, user_folder, message_obj_for_reply):
                                 logger.info(f"NPM Install OK for {module_name}. Retrying run_js_script...")
                                 bot.reply_to(message_obj_for_reply, f"🔄 NPM Install successful. Retrying '{file_name}'...")
                                 time.sleep(2)
                                 threading.Thread(target=run_js_script, args=(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt + 1)).start()
                                 return
                             else:
                                 bot.reply_to(message_obj_for_reply, f"❌ NPM Install failed. Cannot run '{file_name}'.")
                                 return
                        else: logger.info(f"Skipping npm install for relative/core: {module_name}")
                    error_summary = stderr[:500]
                    bot.reply_to(message_obj_for_reply, f"❌ Error in JS script pre-check for '{file_name}':\n```\n{error_summary}\n```\nFix script or install manually.", parse_mode='Markdown')
                    return
            except subprocess.TimeoutExpired:
                logger.info("JS Pre-check timed out (>5s), imports likely OK. Killing check process.")
                if check_proc and check_proc.poll() is None: check_proc.kill(); check_proc.communicate()
                logger.info("JS Check process killed. Proceeding to long run.")
            except FileNotFoundError:
                 error_msg = "❌ Error: 'node' not found. Ensure Node.js is installed for JS files."
                 logger.error(error_msg)
                 bot.reply_to(message_obj_for_reply, error_msg)
                 return
            except Exception as e:
                 logger.error(f"Error in JS pre-check for {script_key}: {e}", exc_info=True)
                 bot.reply_to(message_obj_for_reply, f"❌ Unexpected error in JS pre-check for '{file_name}': {e}")
                 return
            finally:
                 if check_proc and check_proc.poll() is None:
                     logger.warning(f"JS Check process {check_proc.pid} still running. Killing.")
                     check_proc.kill(); check_proc.communicate()

        logger.info(f"Starting long-running JS process for {script_key}")
        log_file_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        log_file = None; process = None
        try: log_file = open(log_file_path, 'w', encoding='utf-8', errors='ignore')
        except Exception as e:
            logger.error(f"Failed to open log file '{log_file_path}' for JS script {script_key}: {e}", exc_info=True)
            bot.reply_to(message_obj_for_reply, f"❌ Failed to open log file '{log_file_path}': {e}")
            return
        try:
            startupinfo = None; creationflags = 0
            if os.name == 'nt':
                 startupinfo = subprocess.STARTUPINFO(); startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                 startupinfo.wShowWindow = subprocess.SW_HIDE
            process = subprocess.Popen(
                ['node', script_path], cwd=user_folder, stdout=log_file, stderr=log_file,
                stdin=subprocess.PIPE, startupinfo=startupinfo, creationflags=creationflags,
                encoding='utf-8', errors='ignore'
            )
            logger.info(f"Started JS process {process.pid} for {script_key}")
            bot_scripts[script_key] = {
                'process': process, 'log_file': log_file, 'file_name': file_name,
                'chat_id': message_obj_for_reply.chat.id,
                'script_owner_id': script_owner_id,
                'start_time': datetime.now(), 'user_folder': user_folder, 'type': 'js', 'script_key': script_key
            }
            bot.reply_to(message_obj_for_reply, f"✅ JS script '{file_name}' started! (PID: {process.pid}) (For User: {script_owner_id})")
        except FileNotFoundError:
             error_msg = "❌ Error: 'node' not found for long run. Ensure Node.js is installed."
             logger.error(error_msg)
             if log_file and not log_file.closed: log_file.close()
             bot.reply_to(message_obj_for_reply, error_msg)
             if script_key in bot_scripts: del bot_scripts[script_key]
        except Exception as e:
            if log_file and not log_file.closed: log_file.close()
            error_msg = f"❌ Error starting JS script '{file_name}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            bot.reply_to(message_obj_for_reply, error_msg)
            if process and process.poll() is None:
                 logger.warning(f"Killing potentially started JS process {process.pid} for {script_key}")
                 kill_process_tree({'process': process, 'log_file': log_file, 'script_key': script_key})
            if script_key in bot_scripts: del bot_scripts[script_key]
    except Exception as e:
        error_msg = f"❌ Unexpected error running JS script '{file_name}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message_obj_for_reply, error_msg)
        if script_key in bot_scripts:
             logger.warning(f"Cleaning up {script_key} due to error in run_js_script.")
             kill_process_tree(bot_scripts[script_key])
             del bot_scripts[script_key]

# --- Map Telegram import names to actual PyPI package names ---
TELEGRAM_MODULES = {
    'telebot': 'pyTelegramBotAPI',
    'telegram': 'python-telegram-bot',
    'python_telegram_bot': 'python-telegram-bot',
    'aiogram': 'aiogram',
    'pyrogram': 'pyrogram',
    'telethon': 'telethon',
    'telethon.sync': 'telethon',
    'from telethon.sync import telegramclient': 'telethon',
    'telepot': 'telepot',
    'pytg': 'pytg',
    'tgcrypto': 'tgcrypto',
    'telegram_upload': 'telegram-upload',
    'telegram_send': 'telegram-send',
    'telegram_text': 'telegram-text',
    'mtproto': 'telegram-mtproto',
    'tl': 'telethon',
    'telegram_utils': 'telegram-utils',
    'telegram_logger': 'telegram-logger',
    'telegram_handlers': 'python-telegram-handlers',
    'telegram_redis': 'telegram-redis',
    'telegram_sqlalchemy': 'telegram-sqlalchemy',
    'telegram_payment': 'telegram-payment',
    'telegram_shop': 'telegram-shop-sdk',
    'pytest_telegram': 'pytest-telegram',
    'telegram_debug': 'telegram-debug',
    'telegram_scraper': 'telegram-scraper',
    'telegram_analytics': 'telegram-analytics',
    'telegram_nlp': 'telegram-nlp-toolkit',
    'telegram_ai': 'telegram-ai',
    'telegram_api': 'telegram-api-client',
    'telegram_web': 'telegram-web-integration',
    'telegram_games': 'telegram-games',
    'telegram_quiz': 'telegram-quiz-bot',
    'telegram_ffmpeg': 'telegram-ffmpeg',
    'telegram_media': 'telegram-media-utils',
    'telegram_2fa': 'telegram-twofa',
    'telegram_crypto': 'telegram-crypto-bot',
    'telegram_i18n': 'telegram-i18n',
    'telegram_translate': 'telegram-translate',
    'bs4': 'beautifulsoup4',
    'requests': 'requests',
    'pillow': 'Pillow',
    'cv2': 'opencv-python',
    'yaml': 'PyYAML',
    'dotenv': 'python-dotenv',
    'dateutil': 'python-dateutil',
    'pandas': 'pandas',
    'numpy': 'numpy',
    'flask': 'Flask',
    'django': 'Django',
    'sqlalchemy': 'SQLAlchemy',
    'asyncio': None,
    'json': None,
    'datetime': None,
    'os': None,
    'sys': None,
    're': None,
    'time': None,
    'math': None,
    'random': None,
    'logging': None,
    'threading': None,
    'subprocess': None,
    'zipfile': None,
    'tempfile': None,
    'shutil': None,
    'sqlite3': None,
    'psutil': 'psutil',
    'atexit': None
}
# --- End Automatic Package Installation & Script Running ---

# --- Database Operations ---
DB_LOCK = threading.Lock() 

def save_user_file(user_id, file_name, file_type='py'):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('INSERT OR REPLACE INTO user_files (user_id, file_name, file_type) VALUES (?, ?, ?)',
                      (user_id, file_name, file_type))
            conn.commit()
            if user_id not in user_files: user_files[user_id] = []
            user_files[user_id] = [(fn, ft) for fn, ft in user_files[user_id] if fn != file_name]
            user_files[user_id].append((file_name, file_type))
            logger.info(f"Saved file '{file_name}' ({file_type}) for user {user_id}")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error saving file for user {user_id}, {file_name}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error saving file for {user_id}, {file_name}: {e}", exc_info=True)
        finally: conn.close()

def remove_user_file_db(user_id, file_name):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('DELETE FROM user_files WHERE user_id = ? AND file_name = ?', (user_id, file_name))
            conn.commit()
            if user_id in user_files:
                user_files[user_id] = [f for f in user_files[user_id] if f[0] != file_name]
                if not user_files[user_id]: del user_files[user_id]
            logger.info(f"Removed file '{file_name}' for user {user_id} from DB")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error removing file for {user_id}, {file_name}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error removing file for {user_id}, {file_name}: {e}", exc_info=True)
        finally: conn.close()

def add_active_user(user_id):
    active_users.add(user_id) 
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('INSERT OR IGNORE INTO active_users (user_id) VALUES (?)', (user_id,))
            conn.commit()
            logger.info(f"Added/Confirmed active user {user_id} in DB")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error adding active user {user_id}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error adding active user {user_id}: {e}", exc_info=True)
        finally: conn.close()

def save_subscription(user_id, expiry):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            expiry_str = expiry.isoformat()
            c.execute('INSERT OR REPLACE INTO subscriptions (user_id, expiry) VALUES (?, ?)', (user_id, expiry_str))
            conn.commit()
            user_subscriptions[user_id] = {'expiry': expiry}
            logger.info(f"Saved subscription for {user_id}, expiry {expiry_str}")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error saving subscription for {user_id}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error saving subscription for {user_id}: {e}", exc_info=True)
        finally: conn.close()

def remove_subscription_db(user_id):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('DELETE FROM subscriptions WHERE user_id = ?', (user_id,))
            conn.commit()
            if user_id in user_subscriptions: del user_subscriptions[user_id]
            logger.info(f"Removed subscription for {user_id} from DB")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error removing subscription for {user_id}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error removing subscription for {user_id}: {e}", exc_info=True)
        finally: conn.close()

def add_admin_db(admin_id):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (admin_id,))
            conn.commit()
            admin_ids.add(admin_id) 
            logger.info(f"Added admin {admin_id} to DB")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error adding admin {admin_id}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error adding admin {admin_id}: {e}", exc_info=True)
        finally: conn.close()

def remove_admin_db(admin_id):
    if admin_id == OWNER_ID:
        logger.warning("Attempted to remove OWNER_ID from admins.")
        return False 
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        removed = False
        try:
            c.execute('SELECT 1 FROM admins WHERE user_id = ?', (admin_id,))
            if c.fetchone():
                c.execute('DELETE FROM admins WHERE user_id = ?', (admin_id,))
                conn.commit()
                removed = c.rowcount > 0 
                if removed: admin_ids.discard(admin_id); logger.info(f"Removed admin {admin_id} from DB")
                else: logger.warning(f"Admin {admin_id} found but delete affected 0 rows.")
            else:
                logger.warning(f"Admin {admin_id} not found in DB.")
                admin_ids.discard(admin_id)
            return removed
        except sqlite3.Error as e: logger.error(f"❌ SQLite error removing admin {admin_id}: {e}"); return False
        except Exception as e: logger.error(f"❌ Unexpected error removing admin {admin_id}: {e}", exc_info=True); return False
        finally: conn.close()
# --- End Database Operations ---

# --- Menu creation (Inline and ReplyKeyboards) ---
def create_main_menu_inline(user_id):
    markup = types.InlineKeyboardMarkup(row_width=2)
    buttons = [
        types.InlineKeyboardButton('📢 Updates Channel', url=UPDATE_CHANNEL),
        types.InlineKeyboardButton('📤 Upload File', callback_data='upload'),
        types.InlineKeyboardButton('🏜️ Check Files', callback_data='check_files'),
        types.InlineKeyboardButton('⚡ Bot Speed', callback_data='speed'),
        types.InlineKeyboardButton('🗿 Contact Owner', url=f'https://t.me/{YOUR_USERNAME.replace("@", "")}')
    ]

    if user_id in admin_ids:
        admin_buttons = [
            types.InlineKeyboardButton('💳 Subscriptions', callback_data='subscription'),
            types.InlineKeyboardButton('📊 Statistics', callback_data='stats'),
            types.InlineKeyboardButton('🔒 Lock Bot' if not bot_locked else '🔓 Unlock Bot',
                                     callback_data='lock_bot' if not bot_locked else 'unlock_bot'),
            types.InlineKeyboardButton('📢 Broadcast', callback_data='broadcast'),
            types.InlineKeyboardButton('👀 Admin Panel', callback_data='admin_panel'),
            types.InlineKeyboardButton('🟢 Run All User Scripts', callback_data='run_all_scripts'),
            types.InlineKeyboardButton('🚫 Ban User', callback_data='ban_user'),  # Added
            types.InlineKeyboardButton('✅ Unban User', callback_data='unban_user')  # Added
        ]
        markup.add(buttons[0])
        markup.add(buttons[1], buttons[2])
        markup.add(buttons[3], admin_buttons[0])
        markup.add(admin_buttons[1], admin_buttons[3])
        markup.add(admin_buttons[2], admin_buttons[5])
        markup.add(admin_buttons[4])
        markup.add(admin_buttons[6], admin_buttons[7])  # Ban/Unban buttons
        markup.add(buttons[4])
    else:
        markup.add(buttons[0])
        markup.add(buttons[1], buttons[2])
        markup.add(buttons[3])
        markup.add(types.InlineKeyboardButton('📊 Statistics', callback_data='stats'))
        markup.add(buttons[4])
    return markup

def create_reply_keyboard_main_menu(user_id):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    layout_to_use = ADMIN_COMMAND_BUTTONS_LAYOUT_USER_SPEC if user_id in admin_ids else COMMAND_BUTTONS_LAYOUT_USER_SPEC
    for row_buttons_text in layout_to_use:
        markup.add(*[types.KeyboardButton(text) for text in row_buttons_text])
    return markup

def create_control_buttons(script_owner_id, file_name, is_running=True):
    markup = types.InlineKeyboardMarkup(row_width=2)
    if is_running:
        markup.row(
            types.InlineKeyboardButton("🔴 Stop", callback_data=f'stop_{script_owner_id}_{file_name}'),
            types.InlineKeyboardButton("🔄 Restart", callback_data=f'restart_{script_owner_id}_{file_name}')
        )
        markup.row(
            types.InlineKeyboardButton("🗑️ Delete", callback_data=f'delete_{script_owner_id}_{file_name}'),
            types.InlineKeyboardButton("📜 Logs", callback_data=f'logs_{script_owner_id}_{file_name}')
        )
    else:
        markup.row(
            types.InlineKeyboardButton("🟢 Start", callback_data=f'start_{script_owner_id}_{file_name}'),
            types.InlineKeyboardButton("🗑️ Delete", callback_data=f'delete_{script_owner_id}_{file_name}')
        )
        markup.row(
            types.InlineKeyboardButton("📜 View Logs", callback_data=f'logs_{script_owner_id}_{file_name}')
        )
    markup.add(types.InlineKeyboardButton("🔙 Back to Files", callback_data='check_files'))
    return markup

def create_admin_panel():
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton('➕ Add Admin', callback_data='add_admin'),
        types.InlineKeyboardButton('➖ Remove Admin', callback_data='remove_admin')
    )
    markup.row(types.InlineKeyboardButton('📋 List Admins', callback_data='list_admins'))
    markup.row(types.InlineKeyboardButton('🔙 Back to Main', callback_data='back_to_main'))
    return markup

def create_subscription_menu():
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton('➕ Add Subscription', callback_data='add_subscription'),
        types.InlineKeyboardButton('➖ Remove Subscription', callback_data='remove_subscription')
    )
    markup.row(types.InlineKeyboardButton('🔍 Check Subscription', callback_data='check_subscription'))
    markup.row(types.InlineKeyboardButton('🔙 Back to Main', callback_data='back_to_main'))
    return markup
# --- End Menu Creation ---

# --- File Handling ---
def handle_zip_file(downloaded_file_content, file_name_zip, message):
    user_id = message.from_user.id
    user_folder = get_user_folder(user_id)
    temp_dir = None 
    try:
        temp_dir = tempfile.mkdtemp(prefix=f"user_{user_id}_zip_")
        logger.info(f"Temp dir for zip: {temp_dir}")
        zip_path = os.path.join(temp_dir, file_name_zip)
        with open(zip_path, 'wb') as new_file: new_file.write(downloaded_file_content)
        
        # Check zip for malicious content
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for member in zip_ref.infolist():
                    if member.filename.endswith(('.py', '.js')):
                        with zip_ref.open(member) as file:
                            content = file.read().decode('utf-8', errors='ignore')
                            suspicious = check_for_malicious_code(content, member.filename)
                            if suspicious:
                                bot.reply_to(message, f"⚠️ File `{member.filename}` in ZIP contains potentially malicious code. Sending for admin review...")
                                send_file_for_review(message, content, member.filename, user_id, message.from_user.first_name)
                                # Extract but don't run until approved
                                zip_ref.extractall(temp_dir)
                                # Move files but don't run
                                moved_count = 0
                                for item_name in os.listdir(temp_dir):
                                    src_path = os.path.join(temp_dir, item_name)
                                    dest_path = os.path.join(user_folder, item_name)
                                    if os.path.isdir(dest_path): shutil.rmtree(dest_path)
                                    elif os.path.exists(dest_path): os.remove(dest_path)
                                    shutil.move(src_path, dest_path); moved_count +=1
                                logger.info(f"Moved {moved_count} items to {user_folder} (awaiting approval)")
                                bot.reply_to(message, f"✅ ZIP extracted. Files await admin approval before running.")
                                return
        except Exception as e:
            logger.error(f"Error checking zip for malicious content: {e}")
        
        # If no malicious content found, proceed normally
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for member in zip_ref.infolist():
                member_path = os.path.abspath(os.path.join(temp_dir, member.filename))
                if not member_path.startswith(os.path.abspath(temp_dir)):
                    raise zipfile.BadZipFile(f"Zip has unsafe path: {member.filename}")
            zip_ref.extractall(temp_dir)
            logger.info(f"Extracted zip to {temp_dir}")

        extracted_items = os.listdir(temp_dir)
        py_files = [f for f in extracted_items if f.endswith('.py')]
        js_files = [f for f in extracted_items if f.endswith('.js')]
        req_file = 'requirements.txt' if 'requirements.txt' in extracted_items else None
        pkg_json = 'package.json' if 'package.json' in extracted_items else None

        if req_file:
            req_path = os.path.join(temp_dir, req_file)
            logger.info(f"requirements.txt found, installing: {req_path}")
            bot.reply_to(message, f"🔄 Installing Python deps from `{req_file}`...")
            try:
                command = [sys.executable, '-m', 'pip', 'install', '-r', req_path]
                result = subprocess.run(command, capture_output=True, text=True, check=True, encoding='utf-8', errors='ignore')
                logger.info(f"pip install from requirements.txt OK. Output:\n{result.stdout}")
                bot.reply_to(message, f"✅ Python deps from `{req_file}` installed.")
            except subprocess.CalledProcessError as e:
                error_msg = f"❌ Failed to install Python deps from `{req_file}`.\nLog:\n```\n{e.stderr or e.stdout}\n```"
                logger.error(error_msg)
                if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (Log truncated)"
                bot.reply_to(message, error_msg, parse_mode='Markdown'); return
            except Exception as e:
                 error_msg = f"❌ Unexpected error installing Python deps: {e}"
                 logger.error(error_msg, exc_info=True); bot.reply_to(message, error_msg); return

        if pkg_json:
            logger.info(f"package.json found, npm install in: {temp_dir}")
            bot.reply_to(message, f"🔄 Installing Node deps from `{pkg_json}`...")
            try:
                command = ['npm', 'install']
                result = subprocess.run(command, capture_output=True, text=True, check=True, cwd=temp_dir, encoding='utf-8', errors='ignore')
                logger.info(f"npm install OK. Output:\n{result.stdout}")
                bot.reply_to(message, f"✅ Node deps from `{pkg_json}` installed.")
            except FileNotFoundError:
                bot.reply_to(message, "❌ 'npm' not found. Cannot install Node deps."); return 
            except subprocess.CalledProcessError as e:
                error_msg = f"❌ Failed to install Node deps from `{pkg_json}`.\nLog:\n```\n{e.stderr or e.stdout}\n```"
                logger.error(error_msg)
                if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (Log truncated)"
                bot.reply_to(message, error_msg, parse_mode='Markdown'); return
            except Exception as e:
                 error_msg = f"❌ Unexpected error installing Node deps: {e}"
                 logger.error(error_msg, exc_info=True); bot.reply_to(message, error_msg); return

        main_script_name = None; file_type = None
        preferred_py = ['main.py', 'bot.py', 'app.py']; preferred_js = ['index.js', 'main.js', 'bot.js', 'app.js']
        for p in preferred_py:
            if p in py_files: main_script_name = p; file_type = 'py'; break
        if not main_script_name:
             for p in preferred_js:
                 if p in js_files: main_script_name = p; file_type = 'js'; break
        if not main_script_name:
            if py_files: main_script_name = py_files[0]; file_type = 'py'
            elif js_files: main_script_name = js_files[0]; file_type = 'js'
        if not main_script_name:
            bot.reply_to(message, "❌ No `.py` or `.js` script found in archive!"); return

        logger.info(f"Moving extracted files from {temp_dir} to {user_folder}")
        moved_count = 0
        for item_name in os.listdir(temp_dir):
            src_path = os.path.join(temp_dir, item_name)
            dest_path = os.path.join(user_folder, item_name)
            if os.path.isdir(dest_path): shutil.rmtree(dest_path)
            elif os.path.exists(dest_path): os.remove(dest_path)
            shutil.move(src_path, dest_path); moved_count +=1
        logger.info(f"Moved {moved_count} items to {user_folder}")

        save_user_file(user_id, main_script_name, file_type)
        logger.info(f"Saved main script '{main_script_name}' ({file_type}) for {user_id} from zip.")
        main_script_path = os.path.join(user_folder, main_script_name)
        bot.reply_to(message, f"✅ Files extracted. Starting main script: `{main_script_name}`...", parse_mode='Markdown')

        if file_type == 'py':
             threading.Thread(target=run_script, args=(main_script_path, user_id, user_folder, main_script_name, message)).start()
        elif file_type == 'js':
             threading.Thread(target=run_js_script, args=(main_script_path, user_id, user_folder, main_script_name, message)).start()

    except zipfile.BadZipFile as e:
        logger.error(f"Bad zip file from {user_id}: {e}")
        bot.reply_to(message, f"❌ Error: Invalid/corrupted ZIP. {e}")
    except Exception as e:
        logger.error(f"❌ Error processing zip for {user_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Error processing zip: {str(e)}")
    finally:
        if temp_dir and os.path.exists(temp_dir):
            try: shutil.rmtree(temp_dir); logger.info(f"Cleaned temp dir: {temp_dir}")
            except Exception as e: logger.error(f"Failed to clean temp dir {temp_dir}: {e}", exc_info=True)

def handle_js_file(file_path, script_owner_id, user_folder, file_name, message):
    try:
        # Check for malicious content
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        suspicious = check_for_malicious_code(content, file_name)
        if suspicious:
            bot.reply_to(message, f"⚠️ File `{file_name}` contains potentially malicious code. Sending for admin review...")
            send_file_for_review(message, content, file_name, script_owner_id, message.from_user.first_name)
            # Save file but don't run until approved
            save_user_file(script_owner_id, file_name, 'js')
            bot.reply_to(message, "✅ File saved. Awaiting admin approval before running.")
            return
        
        save_user_file(script_owner_id, file_name, 'js')
        threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, message)).start()
    except Exception as e:
        logger.error(f"❌ Error processing JS file {file_name} for {script_owner_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Error processing JS file: {str(e)}")

def handle_py_file(file_path, script_owner_id, user_folder, file_name, message):
    try:
        # Check for malicious content
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        suspicious = check_for_malicious_code(content, file_name)
        if suspicious:
            bot.reply_to(message, f"⚠️ File `{file_name}` contains potentially malicious code. Sending for admin review...")
            send_file_for_review(message, content, file_name, script_owner_id, message.from_user.first_name)
            # Save file but don't run until approved
            save_user_file(script_owner_id, file_name, 'py')
            bot.reply_to(message, "✅ File saved. Awaiting admin approval before running.")
            return
        
        save_user_file(script_owner_id, file_name, 'py')
        threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, message)).start()
    except Exception as e:
        logger.error(f"❌ Error processing Python file {file_name} for {script_owner_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Error processing Python file: {str(e)}")
# --- End File Handling ---

# --- Logic Functions (called by commands and text handlers) ---
def _logic_send_welcome(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    user_name = message.from_user.first_name
    user_username = message.from_user.username

    logger.info(f"Welcome request from user_id: {user_id}, username: @{user_username}")

    # Check if user is banned
    if is_user_banned(user_id):
        bot.send_message(chat_id, "❌ You are banned from using this bot.")
        return

    if bot_locked and user_id not in admin_ids:
        bot.send_message(chat_id, "⚠️ Bot locked by admin. Try later.")
        return

    user_bio = "Could not fetch bio"; photo_file_id = None
    try: user_bio = bot.get_chat(user_id).bio or "No bio"
    except Exception: pass
    try:
        user_profile_photos = bot.get_user_profile_photos(user_id, limit=1)
        if user_profile_photos.photos: photo_file_id = user_profile_photos.photos[0][-1].file_id
    except Exception: pass

    if user_id not in active_users:
        add_active_user(user_id)
        try:
            owner_notification = (f"🎉 New user!\n👤 Name: {user_name}\n✳️ User: @{user_username or 'N/A'}\n"
                                  f"🆔 ID: `{user_id}`\n📝 Bio: {user_bio}")
            bot.send_message(OWNER_ID, owner_notification, parse_mode='Markdown')
            if photo_file_id: bot.send_photo(OWNER_ID, photo_file_id, caption=f"Pic of new user {user_id}")
        except Exception as e: logger.error(f"⚠️ Failed to notify owner about new user {user_id}: {e}")

    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
    expiry_info = ""
    if user_id == OWNER_ID: user_status = "👑 Owner"
    elif user_id in admin_ids: user_status = "🛡️ Admin"
    elif is_user_banned(user_id): user_status = "🚫 Banned"
    elif user_id in user_subscriptions:
        expiry_date = user_subscriptions[user_id].get('expiry')
        if expiry_date and expiry_date > datetime.now():
            user_status = "⭐ Premium"; days_left = (expiry_date - datetime.now()).days
            expiry_info = f"\n⏳ Subscription expires in: {days_left} days"
        else: user_status = "🆓 Free User (Expired Sub)"; remove_subscription_db(user_id)
    else: user_status = "🆓 Free User"

    welcome_msg_text = (f"〽️ Welcome, {user_name}!\n\n🆔 Your User ID: `{user_id}`\n"
                        f"✳️ Username: `@{user_username or 'Not set'}`\n"
                        f"🔰 Your Status: {user_status}{expiry_info}\n"
                        f"📁 Files Uploaded: {current_files} / {limit_str}\n\n"
                        f"🤖 Host & run Python (`.py`) or JS (`.js`) scripts.\n"
                        f"   Upload single scripts or `.zip` archives.\n\n"
                        f"👇 Use buttons or type commands.")
    main_reply_markup = create_reply_keyboard_main_menu(user_id)
    try:
        if photo_file_id: bot.send_photo(chat_id, photo_file_id)
        bot.send_message(chat_id, welcome_msg_text, reply_markup=main_reply_markup, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error sending welcome to {user_id}: {e}", exc_info=True)
        try: bot.send_message(chat_id, welcome_msg_text, reply_markup=main_reply_markup, parse_mode='Markdown')
        except Exception as fallback_e: logger.error(f"Fallback send_message failed for {user_id}: {fallback_e}")

def _logic_updates_channel(message):
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton('📢 Updates Channel', url=UPDATE_CHANNEL))
    bot.reply_to(message, "Visit our Updates Channel:", reply_markup=markup)

def _logic_upload_file(message):
    user_id = message.from_user.id
    
    # Check if user is banned
    if is_user_banned(user_id):
        bot.reply_to(message, "❌ You are banned from using this bot.")
        return
    
    if bot_locked and user_id not in admin_ids:
        bot.reply_to(message, "⚠️ Bot locked by admin, cannot accept files.")
        return

    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    if current_files >= file_limit:
        limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
        bot.reply_to(message, f"⚠️ File limit ({current_files}/{limit_str}) reached. Delete files first.")
        return
    bot.reply_to(message, "📤 Send your Python (`.py`), JS (`.js`), or ZIP (`.zip`) file.")

def _logic_check_files(message):
    user_id = message.from_user.id
    
    # Check if user is banned
    if is_user_banned(user_id):
        bot.reply_to(message, "❌ You are banned from using this bot.")
        return
    
    user_files_list = user_files.get(user_id, [])
    if not user_files_list:
        bot.reply_to(message, "💎 Your files:\n\n(No files uploaded yet)")
        return
    markup = types.InlineKeyboardMarkup(row_width=1)
    for file_name, file_type in sorted(user_files_list):
        is_running = is_bot_running(user_id, file_name)
        status_icon = "🟢 Running" if is_running else "🔴 Stopped"
        btn_text = f"{file_name} ({file_type}) - {status_icon}"
        markup.add(types.InlineKeyboardButton(btn_text, callback_data=f'file_{user_id}_{file_name}'))
    bot.reply_to(message, "💎 Your files:\nClick to manage.", reply_markup=markup, parse_mode='Markdown')

def _logic_bot_speed(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    # Check if user is banned
    if is_user_banned(user_id):
        bot.reply_to(message, "❌ You are banned from using this bot.")
        return
    
    start_time_ping = time.time()
    wait_msg = bot.reply_to(message, "🏃 Testing speed...")
    try:
        bot.send_chat_action(chat_id, 'typing')
        response_time = round((time.time() - start_time_ping) * 1000, 2)
        status = "🔓 Unlocked" if not bot_locked else "🔒 Locked"
        if user_id == OWNER_ID: user_level = "👑 Owner"
        elif user_id in admin_ids: user_level = "🛡️ Admin"
        elif is_user_banned(user_id): user_level = "🚫 Banned"
        elif user_id in user_subscriptions and user_subscriptions[user_id].get('expiry', datetime.min) > datetime.now(): user_level = "⭐ Premium"
        else: user_level = "🆓 Free User"
        speed_msg = (f"⚡ Bot Speed & Status:\n\n⏱️ API Response Time: {response_time} ms\n"
                     f"🚦 Bot Status: {status}\n"
                     f"👤 Your Level: {user_level}")
        bot.edit_message_text(speed_msg, chat_id, wait_msg.message_id)
    except Exception as e:
        logger.error(f"Error during speed test (cmd): {e}", exc_info=True)
        bot.edit_message_text("❌ Error during speed test.", chat_id, wait_msg.message_id)

def _logic_contact_owner(message):
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton('📞 Contact Owner', url=f'https://t.me/{YOUR_USERNAME.replace("@", "")}'))
    bot.reply_to(message, "Click to contact Owner:", reply_markup=markup)

# --- Admin Logic Functions ---
def _logic_subscriptions_panel(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    bot.reply_to(message, "💳 Subscription Management\nUse inline buttons from /start or admin command menu.", reply_markup=create_subscription_menu())

def _logic_statistics(message):
    user_id = message.from_user.id
    
    # Check if user is banned
    if is_user_banned(user_id):
        bot.reply_to(message, "❌ You are banned from using this bot.")
        return
    
    total_users = len(active_users)
    total_files_records = sum(len(files) for files in user_files.values())

    running_bots_count = 0
    user_running_bots = 0
    user_uploaded_files = 0

    for script_key_iter, script_info_iter in list(bot_scripts.items()):
        s_owner_id, _ = script_key_iter.split('_', 1)
        if is_bot_running(int(s_owner_id), script_info_iter['file_name']):
            running_bots_count += 1
            if int(s_owner_id) == user_id:
                user_running_bots +=1

    # Get banned users count
    banned_count = 0
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM banned_users')
        banned_count = c.fetchone()[0]
        conn.close()
    except Exception as e:
        logger.error(f"Error getting banned count: {e}")

    # User-specific stats
    user_uploaded_files = len(user_files.get(user_id, []))
    
    if user_id in admin_ids:
        # Admin/Owner sees full statistics
        stats_msg_admin = (f"📊 ADMIN STATISTICS:\n\n"
                          f"👥 Total Users: {total_users}\n"
                          f"🚫 Banned Users: {banned_count}\n"
                          f"💎 Total File Records: {total_files_records}\n"
                          f"🟢 Total Active Bots: {running_bots_count}\n"
                          f"🔒 Bot Status: {'🔴 Locked' if bot_locked else '🟢 Unlocked'}\n"
                          f"🤖 Your Running Bots: {user_running_bots}\n"
                          f"📁 Your Uploaded Files: {user_uploaded_files}")
        stats_msg = stats_msg_admin
    else:
        # Regular users see only their own stats
        stats_msg = (f"📊 YOUR STATISTICS:\n\n"
                    f"🤖 Your Running Bots: {user_running_bots}\n"
                    f"📁 Your Uploaded Files: {user_uploaded_files}\n"
                    f"💎 Total Files Allowed: {get_user_file_limit(user_id)}")

    bot.reply_to(message, stats_msg)

def _logic_broadcast_init(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    msg = bot.reply_to(message, "📢 Send message to broadcast to all active users.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_broadcast_message)

def _logic_toggle_lock_bot(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    global bot_locked
    bot_locked = not bot_locked
    status = "locked" if bot_locked else "unlocked"
    logger.warning(f"Bot {status} by Admin {message.from_user.id} via command/button.")
    bot.reply_to(message, f"🔒 Bot has been {status}.")

def _logic_admin_panel(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    bot.reply_to(message, "💎 Admin Panel\nManage admins. Use inline buttons from /start or admin menu.",
                 reply_markup=create_admin_panel())

def _logic_run_all_scripts(message_or_call):
    if isinstance(message_or_call, telebot.types.Message):
        admin_user_id = message_or_call.from_user.id
        admin_chat_id = message_or_call.chat.id
        reply_func = lambda text, **kwargs: bot.reply_to(message_or_call, text, **kwargs)
        admin_message_obj_for_script_runner = message_or_call
    elif isinstance(message_or_call, telebot.types.CallbackQuery):
        admin_user_id = message_or_call.from_user.id
        admin_chat_id = message_or_call.message.chat.id
        bot.answer_callback_query(message_or_call.id)
        reply_func = lambda text, **kwargs: bot.send_message(admin_chat_id, text, **kwargs)
        admin_message_obj_for_script_runner = message_or_call.message 
    else:
        logger.error("Invalid argument for _logic_run_all_scripts")
        return

    if admin_user_id not in admin_ids:
        reply_func("⚠️ Admin permissions required.")
        return

    reply_func("⏳ Starting process to run all user scripts. This may take a while...")
    logger.info(f"Admin {admin_user_id} initiated 'run all scripts' from chat {admin_chat_id}.")

    started_count = 0; attempted_users = 0; skipped_files = 0; error_files_details = []

    all_user_files_snapshot = dict(user_files)

    for target_user_id, files_for_user in all_user_files_snapshot.items():
        # Skip banned users
        if is_user_banned(target_user_id):
            logger.info(f"Skipping banned user {target_user_id}")
            continue
            
        if not files_for_user: continue
        attempted_users += 1
        logger.info(f"Processing scripts for user {target_user_id}...")
        user_folder = get_user_folder(target_user_id)

        for file_name, file_type in files_for_user:
            if not is_bot_running(target_user_id, file_name):
                file_path = os.path.join(user_folder, file_name)
                if os.path.exists(file_path):
                    logger.info(f"Admin {admin_user_id} attempting to start '{file_name}' ({file_type}) for user {target_user_id}.")
                    try:
                        if file_type == 'py':
                            threading.Thread(target=run_script, args=(file_path, target_user_id, user_folder, file_name, admin_message_obj_for_script_runner)).start()
                            started_count += 1
                        elif file_type == 'js':
                            threading.Thread(target=run_js_script, args=(file_path, target_user_id, user_folder, file_name, admin_message_obj_for_script_runner)).start()
                            started_count += 1
                        else:
                            logger.warning(f"Unknown file type '{file_type}' for {file_name} (user {target_user_id}). Skipping.")
                            error_files_details.append(f"`{file_name}` (User {target_user_id}) - Unknown type")
                            skipped_files += 1
                        time.sleep(0.7)
                    except Exception as e:
                        logger.error(f"Error queueing start for '{file_name}' (user {target_user_id}): {e}")
                        error_files_details.append(f"`{file_name}` (User {target_user_id}) - Start error")
                        skipped_files += 1
                else:
                    logger.warning(f"File '{file_name}' for user {target_user_id} not found at '{file_path}'. Skipping.")
                    error_files_details.append(f"`{file_name}` (User {target_user_id}) - File not found")
                    skipped_files += 1

    summary_msg = (f"✅ All Users' Scripts - Processing Complete:\n\n"
                   f"▶️ Attempted to start: {started_count} scripts.\n"
                   f"👥 Users processed: {attempted_users}.\n")
    if skipped_files > 0:
        summary_msg += f"⚠️ Skipped/Error files: {skipped_files}\n"
        if error_files_details:
             summary_msg += "Details (first 5):\n" + "\n".join([f"  - {err}" for err in error_files_details[:5]])
             if len(error_files_details) > 5: summary_msg += "\n  ... and more (check logs)."

    reply_func(summary_msg, parse_mode='Markdown')
    logger.info(f"Run all scripts finished. Admin: {admin_user_id}. Started: {started_count}. Skipped/Errors: {skipped_files}")

# --- Ban/Unban Logic Functions ---
def _logic_ban_user(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    msg = bot.reply_to(message, "🚫 Enter User ID to ban (and optional reason).\nFormat: `12345678 reason here`\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_ban_user)

def process_ban_user(message):
    admin_id = message.from_user.id
    if admin_id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    if message.text.lower() == '/cancel':
        bot.reply_to(message, "Ban cancelled.")
        return
    
    try:
        parts = message.text.split(' ', 1)
        user_id = int(parts[0].strip())
        reason = parts[1] if len(parts) > 1 else "No reason provided"
        
        if user_id <= 0:
            raise ValueError("Invalid user ID")
        
        # Don't allow banning owner
        if str(user_id) == OWNER_ID:
            bot.reply_to(message, "❌ Cannot ban the owner!")
            return
        
        # Don't allow banning self
        if user_id == admin_id:
            bot.reply_to(message, "❌ Cannot ban yourself!")
            return
        
        if ban_user(user_id, reason, str(admin_id)):
            bot.reply_to(message, f"✅ User `{user_id}` banned successfully.\nReason: {reason}")
            try:
                bot.send_message(user_id, f"❌ You have been banned from using this bot.\nReason: {reason}")
            except:
                pass
        else:
            bot.reply_to(message, f"❌ Failed to ban user `{user_id}`.")
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid user ID. Please provide a numeric user ID.")
        msg = bot.send_message(message.chat.id, "🚫 Enter User ID to ban (and optional reason).\nFormat: `12345678 reason here`\n/cancel to abort.")
        bot.register_next_step_handler(msg, process_ban_user)
    except Exception as e:
        logger.error(f"Error banning user: {e}")
        bot.reply_to(message, f"❌ Error banning user: {str(e)}")

def _logic_unban_user(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    msg = bot.reply_to(message, "✅ Enter User ID to unban.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_unban_user)

def process_unban_user(message):
    admin_id = message.from_user.id
    if admin_id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    if message.text.lower() == '/cancel':
        bot.reply_to(message, "Unban cancelled.")
        return
    
    try:
        user_id = int(message.text.strip())
        
        if user_id <= 0:
            raise ValueError("Invalid user ID")
        
        if unban_user(user_id):
            bot.reply_to(message, f"✅ User `{user_id}` unbanned successfully.")
            try:
                bot.send_message(user_id, "✅ You have been unbanned and can now use the bot again.")
            except:
                pass
        else:
            bot.reply_to(message, f"ℹ️ User `{user_id}` was not banned or doesn't exist.")
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid user ID. Please provide a numeric user ID.")
        msg = bot.send_message(message.chat.id, "✅ Enter User ID to unban.\n/cancel to abort.")
        bot.register_next_step_handler(msg, process_unban_user)
    except Exception as e:
        logger.error(f"Error unbanning user: {e}")
        bot.reply_to(message, f"❌ Error unbanning user: {str(e)}")

# --- Command Handlers & Text Handlers for ReplyKeyboard ---
@bot.message_handler(commands=['start', 'help'])
def command_send_welcome(message): 
    # Check if user is banned
    if is_user_banned(message.from_user.id):
        bot.reply_to(message, "❌ You are banned from using this bot.")
        return
    _logic_send_welcome(message)

@bot.message_handler(commands=['status'])
def command_show_status(message): _logic_statistics(message)

BUTTON_TEXT_TO_LOGIC = {
    "👀 Updates Channel": _logic_updates_channel,
    "🪨 Upload File": _logic_upload_file,
    "💎 Check Files": _logic_check_files,
    "🏂 Bot Speed": _logic_bot_speed,
    "🗿 Contact Owner": _logic_contact_owner,
    "📊 Statistics": _logic_statistics, 
    "💤 Subscriptions": _logic_subscriptions_panel,
    "☔ Broadcast": _logic_broadcast_init,
    "😎 Lock Bot": _logic_toggle_lock_bot,
    "💤 Running All Code": _logic_run_all_scripts,
    "👀 Admin Panel": _logic_admin_panel,
    "🚫 Ban User": _logic_ban_user,  # Added
    "✅ Unban User": _logic_unban_user,  # Added
}

@bot.message_handler(func=lambda message: message.text in BUTTON_TEXT_TO_LOGIC)
def handle_button_text(message):
    logic_func = BUTTON_TEXT_TO_LOGIC.get(message.text)
    if logic_func: logic_func(message)
    else: logger.warning(f"Button text '{message.text}' matched but no logic func.")

@bot.message_handler(commands=['updateschannel'])
def command_updates_channel(message): _logic_updates_channel(message)
@bot.message_handler(commands=['uploadfile'])
def command_upload_file(message): _logic_upload_file(message)
@bot.message_handler(commands=['checkfiles'])
def command_check_files(message): _logic_check_files(message)
@bot.message_handler(commands=['botspeed'])
def command_bot_speed(message): _logic_bot_speed(message)
@bot.message_handler(commands=['contactowner'])
def command_contact_owner(message): _logic_contact_owner(message)
@bot.message_handler(commands=['subscriptions'])
def command_subscriptions(message): _logic_subscriptions_panel(message)
@bot.message_handler(commands=['statistics'])
def command_statistics(message): _logic_statistics(message)
@bot.message_handler(commands=['broadcast'])
def command_broadcast(message): _logic_broadcast_init(message)
@bot.message_handler(commands=['lockbot']) 
def command_lock_bot(message): _logic_toggle_lock_bot(message)
@bot.message_handler(commands=['adminpanel'])
def command_admin_panel(message): _logic_admin_panel(message)
@bot.message_handler(commands=['runningallcode'])
def command_run_all_code(message): _logic_run_all_scripts(message)
@bot.message_handler(commands=['ban'])
def handle_ban_command(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "❌ You're not authorized to use this command.")
        return
    
    try:
        reply = message.reply_to_message
        if not reply:
            bot.reply_to(message, "⚠️ Reply to a user's message to ban them.")
            return
        
        target_user_id = reply.from_user.id
        target_username = reply.from_user.username or "No username"
        target_name = reply.from_user.first_name
        
        # Prevent banning admins/owner
        if target_user_id in admin_ids:
            bot.reply_to(message, "❌ Cannot ban an admin or owner.")
            return
        
        # Get reason (optional)
        reason = "No reason provided"
        try:
            reason = message.text.split(maxsplit=1)[1]
        except:
            pass
        
        if ban_user(target_user_id, reason, f"{message.from_user.first_name} ({message.from_user.id})"):
            bot.reply_to(message, f"🔨 User {target_name} (@{target_username}) [ID: {target_user_id}] has been BANNED.\nReason: {reason}")
            try:
                bot.send_message(target_user_id, "🔨 You have been BANNED from using this bot.\nReason: {reason}\nContact owner if you think this is a mistake.")
            except:
                pass  # User might have blocked bot
        else:
            bot.reply_to(message, "❌ Failed to ban user.")
    except Exception as e:
        logger.error(f"Ban command error: {e}")
        bot.reply_to(message, "❌ Error processing ban.")

@bot.message_handler(commands=['unban'])
def handle_unban_command(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "❌ You're not authorized to use this command.")
        return
    
    try:
        args = message.text.split()
        if len(args) < 2:
            bot.reply_to(message, "⚠️ Usage: /unban <user_id>")
            return
        
        target_user_id = int(args[1])
        
        if unban_user(target_user_id):
            bot.reply_to(message, f"✅ User {target_user_id} has been UNBANNED.")
            try:
                bot.send_message(target_user_id, "✅ You have been UNBANNED. You can now use the bot again.")
            except:
                pass
        else:
            bot.reply_to(message, "❌ User not found in banned list or error unbanning.")
    except ValueError:
        bot.reply_to(message, "❌ Invalid user ID. Must be a number.")
    except Exception as e:
        logger.error(f"Unban command error: {e}")
        bot.reply_to(message, "❌ Error processing unban.")
        
# Handle the Ban/Unban buttons from admin panel
@bot.callback_query_handler(func=lambda call: call.data in ['ban_user_btn', 'unban_user_btn'])
def handle_ban_unban_buttons(call):
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "❌ Not authorized.", show_alert=True)
        return
    
    if call.data == 'ban_user_btn':
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, "🔨 Reply to a user's message with /ban [reason] to ban them.")
    
    if call.data == 'unban_user_btn':
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, "✅ Use /unban <user_id> to unban someone.\nExample: /unban 123456789")

@bot.message_handler(commands=['ping'])
def ping(message):
    # Check if user is banned
    if is_user_banned(message.from_user.id):
        bot.reply_to(message, "❌ You are banned from using this bot.")
        return
    
    start_ping_time = time.time() 
    msg = bot.reply_to(message, "Pong!")
    latency = round((time.time() - start_ping_time) * 1000, 2)
    bot.edit_message_text(f"Pong! Latency: {latency} ms", message.chat.id, msg.message_id)

# --- Document (File) Handler ---
@bot.message_handler(content_types=['document'])
def handle_file_upload_doc(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    doc = message.document
    logger.info(f"Doc from {user_id}: {doc.file_name} ({doc.mime_type}), Size: {doc.file_size}")

    # Check if user is banned
    if is_user_banned(user_id):
        bot.reply_to(message, "❌ You are banned from using this bot.")
        return

    if bot_locked and user_id not in admin_ids:
        bot.reply_to(message, "⚠️ Bot locked, cannot accept files.")
        return

    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    if current_files >= file_limit:
        limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
        bot.reply_to(message, f"⚠️ File limit ({current_files}/{limit_str}) reached. Delete files via /checkfiles.")
        return

    file_name = doc.file_name
    if not file_name: bot.reply_to(message, "⚠️ No file name. Ensure file has a name."); return
    file_ext = os.path.splitext(file_name)[1].lower()
    if file_ext not in ['.py', '.js', '.zip']:
        bot.reply_to(message, "⚠️ Unsupported type! Only `.py`, `.js`, `.zip` allowed.")
        return
    max_file_size = 20 * 1024 * 1024
    if doc.file_size > max_file_size:
        bot.reply_to(message, f"⚠️ File too large (Max: {max_file_size // 1024 // 1024} MB)."); return

    try:
        try:
            bot.forward_message(OWNER_ID, chat_id, message.message_id)
            bot.send_message(OWNER_ID, f"⬆️ File '{file_name}' from {message.from_user.first_name} (`{user_id}`)", parse_mode='Markdown')
        except Exception as e: logger.error(f"Failed to forward uploaded file to OWNER_ID {OWNER_ID}: {e}")

        download_wait_msg = bot.reply_to(message, f"⏳ Downloading `{file_name}`...")
        file_info_tg_doc = bot.get_file(doc.file_id)
        downloaded_file_content = bot.download_file(file_info_tg_doc.file_path)
        bot.edit_message_text(f"✅ Downloaded `{file_name}`. Processing...", chat_id, download_wait_msg.message_id)
        logger.info(f"Downloaded {file_name} for user {user_id}")
        user_folder = get_user_folder(user_id)

        if file_ext == '.zip':
            handle_zip_file(downloaded_file_content, file_name, message)
        else:
            file_path = os.path.join(user_folder, file_name)
            with open(file_path, 'wb') as f: f.write(downloaded_file_content)
            logger.info(f"Saved single file to {file_path}")
            if file_ext == '.js': handle_js_file(file_path, user_id, user_folder, file_name, message)
            elif file_ext == '.py': handle_py_file(file_path, user_id, user_folder, file_name, message)
    except telebot.apihelper.ApiTelegramException as e:
         logger.error(f"Telegram API Error handling file for {user_id}: {e}", exc_info=True)
         if "file is too big" in str(e).lower():
              bot.reply_to(message, f"❌ Telegram API Error: File too large to download (~20MB limit).")
         else: bot.reply_to(message, f"❌ Telegram API Error: {str(e)}. Try later.")
    except Exception as e:
        logger.error(f"❌ General error handling file for {user_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Unexpected error: {str(e)}")

# --- Callback Query Handlers (for Inline Buttons) ---
@bot.callback_query_handler(func=lambda call: True) 
def handle_callbacks(call):
    user_id = call.from_user.id
    data = call.data
    logger.info(f"Callback: User={user_id}, Data='{data}'")

    # Check if user is banned for most actions
    if not data.startswith(('back_to_main', 'speed', 'stats')) and is_user_banned(user_id):
        bot.answer_callback_query(call.id, "❌ You are banned from using this bot.", show_alert=True)
        return

    if bot_locked and user_id not in admin_ids and data not in ['back_to_main', 'speed', 'stats']:
        bot.answer_callback_query(call.id, "⚠️ Bot locked by admin.", show_alert=True)
        return
    
    try:
        if data == 'upload': upload_callback(call)
        elif data == 'check_files': check_files_callback(call)
        elif data.startswith('file_'): file_control_callback(call)
        elif data.startswith('start_'): start_bot_callback(call)
        elif data.startswith('stop_'): stop_bot_callback(call)
        elif data.startswith('restart_'): restart_bot_callback(call)
        elif data.startswith('delete_'): delete_bot_callback(call)
        elif data.startswith('logs_'): logs_bot_callback(call)
        elif data == 'speed': speed_callback(call)
        elif data == 'back_to_main': back_to_main_callback(call)
        elif data.startswith('confirm_broadcast_'): handle_confirm_broadcast(call)
        elif data == 'cancel_broadcast': handle_cancel_broadcast(call)
        # --- Admin Callbacks ---
        elif data == 'subscription': admin_required_callback(call, subscription_management_callback)
        elif data == 'stats': stats_callback(call)
        elif data == 'lock_bot': admin_required_callback(call, lock_bot_callback)
        elif data == 'unlock_bot': admin_required_callback(call, unlock_bot_callback)
        elif data == 'run_all_scripts': admin_required_callback(call, run_all_scripts_callback)
        elif data == 'broadcast': admin_required_callback(call, broadcast_init_callback) 
        elif data == 'admin_panel': admin_required_callback(call, admin_panel_callback)
        elif data == 'add_admin': owner_required_callback(call, add_admin_init_callback) 
        elif data == 'remove_admin': owner_required_callback(call, remove_admin_init_callback) 
        elif data == 'list_admins': admin_required_callback(call, list_admins_callback)
        elif data == 'add_subscription': admin_required_callback(call, add_subscription_init_callback) 
        elif data == 'remove_subscription': admin_required_callback(call, remove_subscription_init_callback) 
        elif data == 'check_subscription': admin_required_callback(call, check_subscription_init_callback) 
        elif data == 'ban_user': admin_required_callback(call, ban_user_callback)  # Added
        elif data == 'unban_user': admin_required_callback(call, unban_user_callback)  # Added
        # --- File Review Callbacks ---
        elif data.startswith('confirm_file_'): handle_confirm_file(call)
        elif data.startswith('reject_file_'): handle_reject_file(call)
        else:
            bot.answer_callback_query(call.id, "Unknown action.")
            logger.warning(f"Unhandled callback data: {data} from user {user_id}")
    except Exception as e:
        logger.error(f"Error handling callback '{data}' for {user_id}: {e}", exc_info=True)
        try: bot.answer_callback_query(call.id, "Error processing request.", show_alert=True)
        except Exception as e_ans: logger.error(f"Failed to answer callback after error: {e_ans}")

def admin_required_callback(call, func_to_run):
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    func_to_run(call) 

def owner_required_callback(call, func_to_run):
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Owner permissions required.", show_alert=True)
        return
    func_to_run(call)

# --- File Review Callback Handlers ---
def handle_confirm_file(call):
    """Handle when owner confirms a file is safe"""
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Owner only.", show_alert=True)
        return
    
    try:
        _, _, user_id_str, file_name = call.data.split('_', 3)
        user_id = int(user_id_str)
        
        bot.answer_callback_query(call.id, "✅ File approved. User notified.")
        
        # Notify user
        try:
            bot.send_message(user_id, f"✅ Your file `{file_name}` has been approved by admin and will now run.")
        except:
            pass
        
        # Find and run the file
        user_folder = get_user_folder(user_id)
        file_path = os.path.join(user_folder, file_name)
        
        if os.path.exists(file_path):
            file_ext = os.path.splitext(file_name)[1].lower()
            if file_ext == '.py':
                threading.Thread(target=run_script, args=(file_path, user_id, user_folder, file_name, call.message)).start()
            elif file_ext == '.js':
                threading.Thread(target=run_js_script, args=(file_path, user_id, user_folder, file_name, call.message)).start()
        
        # Delete the review message
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except:
            pass
            
    except Exception as e:
        logger.error(f"Error handling file confirmation: {e}")
        bot.answer_callback_query(call.id, "❌ Error processing approval.", show_alert=True)

def handle_reject_file(call):
    """Handle when owner rejects a file"""
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Owner only.", show_alert=True)
        return
    
    try:
        _, _, user_id_str, file_name = call.data.split('_', 3)
        user_id = int(user_id_str)
        
        # Ask owner for rejection reason
        msg = bot.send_message(call.message.chat.id, "📝 Please send the rejection reason to send to the user:\n/cancel to skip")
        
        def process_rejection_reason(message):
            if message.text.lower() == '/cancel':
                reason = "File rejected by admin."
            else:
                reason = message.text
            
            # Notify user
            try:
                bot.send_message(user_id, f"❌ Your file `{file_name}` was rejected by admin.\nReason: {reason}")
            except:
                pass
            
            # Delete the file
            user_folder = get_user_folder(user_id)
            file_path = os.path.join(user_folder, file_name)
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except:
                    pass
            
            # Remove from database
            remove_user_file_db(user_id, file_name)
            
            # Delete review message
            try:
                bot.delete_message(call.message.chat.id, call.message.message_id)
            except:
                pass
            
            bot.reply_to(message, f"✅ User notified. File `{file_name}` removed.")
        
        bot.register_next_step_handler(msg, process_rejection_reason)
        bot.answer_callback_query(call.id, "Please provide rejection reason.")
        
    except Exception as e:
        logger.error(f"Error handling file rejection: {e}")
        bot.answer_callback_query(call.id, "❌ Error processing rejection.", show_alert=True)

# --- Existing callback functions (truncated for brevity, keep all existing ones) ---
def upload_callback(call):
    user_id = call.from_user.id
    
    # Check if user is banned
    if is_user_banned(user_id):
        bot.answer_callback_query(call.id, "❌ You are banned from using this bot.", show_alert=True)
        return
    
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    if current_files >= file_limit:
        limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
        bot.answer_callback_query(call.id, f"⚠️ File limit ({current_files}/{limit_str}) reached.", show_alert=True)
        return
    bot.answer_callback_query(call.id) 
    bot.send_message(call.message.chat.id, "📤 Send your Python (`.py`), JS (`.js`), or ZIP (`.zip`) file.")

def check_files_callback(call):
    user_id = call.from_user.id
    
    # Check if user is banned
    if is_user_banned(user_id):
        bot.answer_callback_query(call.id, "❌ You are banned from using this bot.", show_alert=True)
        return
    
    chat_id = call.message.chat.id 
    user_files_list = user_files.get(user_id, [])
    if not user_files_list:
        bot.answer_callback_query(call.id, "⚠️ No files uploaded.", show_alert=True)
        try:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🔙 Back to Main", callback_data='back_to_main'))
            bot.edit_message_text("💎 Your files:\n\n(No files uploaded)", chat_id, call.message.message_id, reply_markup=markup)
        except Exception as e: logger.error(f"Error editing msg for empty file list: {e}")
        return
    bot.answer_callback_query(call.id) 
    markup = types.InlineKeyboardMarkup(row_width=1) 
    for file_name, file_type in sorted(user_files_list): 
        is_running = is_bot_running(user_id, file_name)
        status_icon = "🟢 Running" if is_running else "🔴 Stopped"
        btn_text = f"{file_name} ({file_type}) - {status_icon}"
        markup.add(types.InlineKeyboardButton(btn_text, callback_data=f'file_{user_id}_{file_name}'))
    markup.add(types.InlineKeyboardButton("🔙 Back to Main", callback_data='back_to_main'))
    try:
        bot.edit_message_text("💎 Your files:\nClick to manage.", chat_id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')
    except telebot.apihelper.ApiTelegramException as e:
         if "message is not modified" in str(e): logger.warning("Msg not modified (files).")
         else: logger.error(f"Error editing msg for file list: {e}")
    except Exception as e: logger.error(f"Unexpected error editing msg for file list: {e}", exc_info=True)

def file_control_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id

        # Allow owner/admin to control any file, or user to control their own
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            logger.warning(f"User {requesting_user_id} tried to access file '{file_name}' of user {script_owner_id} without permission.")
            bot.answer_callback_query(call.id, "⚠️ You can only manage your own files.", show_alert=True)
            check_files_callback(call)
            return

        user_files_list = user_files.get(script_owner_id, [])
        if not any(f[0] == file_name for f in user_files_list):
            logger.warning(f"File '{file_name}' not found for user {script_owner_id} during control.")
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True)
            check_files_callback(call) 
            return

        bot.answer_callback_query(call.id) 
        is_running = is_bot_running(script_owner_id, file_name)
        status_text = '🟢 Running' if is_running else '🔴 Stopped'
        file_type = next((f[1] for f in user_files_list if f[0] == file_name), '?') 
        try:
            bot.edit_message_text(
                f"⚙️ Controls for: `{file_name}` ({file_type}) of User `{script_owner_id}`\nStatus: {status_text}",
                call.message.chat.id, call.message.message_id,
                reply_markup=create_control_buttons(script_owner_id, file_name, is_running),
                parse_mode='Markdown'
            )
        except telebot.apihelper.ApiTelegramException as e:
             if "message is not modified" in str(e): logger.warning(f"Msg not modified (controls for {file_name})")
             else: raise 
    except (ValueError, IndexError) as ve:
        logger.error(f"Error parsing file control callback: {ve}. Data: '{call.data}'")
        bot.answer_callback_query(call.id, "Error: Invalid action data.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in file_control_callback for data '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "An error occurred.", show_alert=True)

def start_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id

        logger.info(f"Start request: Requester={requesting_user_id}, Owner={script_owner_id}, File='{file_name}'")

        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied to start this script.", show_alert=True); return

        user_files_list = user_files.get(script_owner_id, [])
        file_info = next((f for f in user_files_list if f[0] == file_name), None)
        if not file_info:
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True); check_files_callback(call); return

        file_type = file_info[1]
        user_folder = get_user_folder(script_owner_id)
        file_path = os.path.join(user_folder, file_name)

        if not os.path.exists(file_path):
            bot.answer_callback_query(call.id, f"⚠️ Error: File `{file_name}` missing! Re-upload.", show_alert=True)
            remove_user_file_db(script_owner_id, file_name); check_files_callback(call); return

        if is_bot_running(script_owner_id, file_name):
            bot.answer_callback_query(call.id, f"⚠️ Script '{file_name}' already running.", show_alert=True)
            try: bot.edit_message_reply_markup(chat_id_for_reply, call.message.message_id, reply_markup=create_control_buttons(script_owner_id, file_name, True))
            except Exception as e: logger.error(f"Error updating buttons (already running): {e}")
            return

        bot.answer_callback_query(call.id, f"⏳ Attempting to start {file_name} for user {script_owner_id}...")

        if file_type == 'py':
            threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        elif file_type == 'js':
            threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        else:
             bot.send_message(chat_id_for_reply, f"❌ Error: Unknown file type '{file_type}' for '{file_name}'."); return 

        time.sleep(1.5)
        is_now_running = is_bot_running(script_owner_id, file_name) 
        status_text = '🟢 Running' if is_now_running else '🟡 Starting (or failed, check logs/replies)'
        try:
            bot.edit_message_text(
                f"⚙️ Controls for: `{file_name}` ({file_type}) of User `{script_owner_id}`\nStatus: {status_text}",
                chat_id_for_reply, call.message.message_id,
                reply_markup=create_control_buttons(script_owner_id, file_name, is_now_running), parse_mode='Markdown'
            )
        except telebot.apihelper.ApiTelegramException as e:
             if "message is not modified" in str(e): logger.warning(f"Msg not modified after starting {file_name}")
             else: raise
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing start callback '{call.data}': {e}")
        bot.answer_callback_query(call.id, "Error: Invalid start command.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in start_bot_callback for '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Error starting script.", show_alert=True)
        try:
            _, script_owner_id_err_str, file_name_err = call.data.split('_', 2)
            script_owner_id_err = int(script_owner_id_err_str)
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=create_control_buttons(script_owner_id_err, file_name_err, False))
        except Exception as e_btn: logger.error(f"Failed to update buttons after start error: {e_btn}")

# ... (keep all other existing callback functions as they are, they remain unchanged)
# --- Missing Callback Functions ---

def speed_callback(call):
    """Handle speed test callback"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    # Check if user is banned
    if is_user_banned(user_id):
        bot.answer_callback_query(call.id, "❌ You are banned from using this bot.", show_alert=True)
        return
    
    bot.answer_callback_query(call.id)
    start_time_ping = time.time()
    try:
        bot.send_chat_action(chat_id, 'typing')
        response_time = round((time.time() - start_time_ping) * 1000, 2)
        status = "🔓 Unlocked" if not bot_locked else "🔒 Locked"
        
        # Determine user level
        if user_id == OWNER_ID:
            user_level = "👑 Owner"
        elif user_id in admin_ids:
            user_level = "🛡️ Admin"
        elif is_user_banned(user_id):
            user_level = "🚫 Banned"
        elif user_id in user_subscriptions and user_subscriptions[user_id].get('expiry', datetime.min) > datetime.now():
            user_level = "⭐ Premium"
        else:
            user_level = "🆓 Free User"
            
        speed_msg = (f"⚡ Bot Speed & Status:\n\n⏱️ API Response Time: {response_time} ms\n"
                     f"🚦 Bot Status: {status}\n"
                     f"👤 Your Level: {user_level}")
        
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔙 Back to Main", callback_data='back_to_main'))
        
        try:
            bot.edit_message_text(
                speed_msg,
                chat_id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"Error editing speed message: {e}")
            bot.send_message(chat_id, speed_msg, reply_markup=markup, parse_mode='Markdown')
            
    except Exception as e:
        logger.error(f"Error during speed test: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "❌ Error during speed test.", show_alert=True)

def back_to_main_callback(call):
    """Handle back to main menu callback"""
    user_id = call.from_user.id
    
    # Don't check ban status for back_to_main - allow banned users to see main menu
    # but they'll be blocked from other actions
    
    bot.answer_callback_query(call.id)
    
    # Create main menu for the user
    markup = create_main_menu_inline(user_id)
    
    welcome_msg = f"〽️ Welcome back!\n\n🆔 Your User ID: `{user_id}`"
    
    if is_user_banned(user_id):
        welcome_msg += "\n🚫 **YOU ARE BANNED FROM USING THIS BOT**"
    
    try:
        bot.edit_message_text(
            welcome_msg,
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup,
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Error editing back to main message: {e}")
        bot.send_message(call.message.chat.id, welcome_msg, reply_markup=markup, parse_mode='Markdown')

def subscription_management_callback(call):
    """Handle subscription management callback"""
    user_id = call.from_user.id
    bot.answer_callback_query(call.id)
    
    markup = create_subscription_menu()
    
    try:
        bot.edit_message_text(
            "💳 Subscription Management\nSelect an option:",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup
        )
    except Exception as e:
        logger.error(f"Error editing subscription message: {e}")

def lock_bot_callback(call):
    """Handle lock bot callback"""
    user_id = call.from_user.id
    global bot_locked
    bot_locked = True
    logger.warning(f"Bot locked by Admin {user_id} via callback.")
    bot.answer_callback_query(call.id, "🔒 Bot has been locked.", show_alert=True)
    
    # Update the button text in the menu
    markup = create_main_menu_inline(user_id)
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
    except Exception as e:
        logger.error(f"Error updating lock button: {e}")

def unlock_bot_callback(call):
    """Handle unlock bot callback"""
    user_id = call.from_user.id
    global bot_locked
    bot_locked = False
    logger.warning(f"Bot unlocked by Admin {user_id} via callback.")
    bot.answer_callback_query(call.id, "🔓 Bot has been unlocked.", show_alert=True)
    
    # Update the button text in the menu
    markup = create_main_menu_inline(user_id)
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
    except Exception as e:
        logger.error(f"Error updating unlock button: {e}")

def run_all_scripts_callback(call):
    """Handle run all scripts callback"""
    _logic_run_all_scripts(call)  # This function is already defined

def broadcast_init_callback(call):
    """Handle broadcast initiation callback"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "📢 Send message to broadcast to all active users.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_broadcast_message)

def admin_panel_callback(call):
    """Handle admin panel callback"""
    bot.answer_callback_query(call.id)
    
    markup = create_admin_panel()
    
    try:
        bot.edit_message_text(
            "💎 Admin Panel\nManage admins:",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup
        )
    except Exception as e:
        logger.error(f"Error editing admin panel message: {e}")

def add_admin_init_callback(call):
    """Handle add admin initiation callback"""
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Owner permissions required.", show_alert=True)
        return
    
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "👤 Enter user ID to add as admin:\n/cancel to abort")
    bot.register_next_step_handler(msg, process_add_admin)

def remove_admin_init_callback(call):
    """Handle remove admin initiation callback"""
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Owner permissions required.", show_alert=True)
        return
    
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "👤 Enter user ID to remove from admins:\n/cancel to abort")
    bot.register_next_step_handler(msg, process_remove_admin)

def list_admins_callback(call):
    """Handle list admins callback"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    bot.answer_callback_query(call.id)
    
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute('SELECT user_id FROM admins ORDER BY user_id')
        admins_list = c.fetchall()
        conn.close()
        
        if not admins_list:
            admin_list_text = "No admins found (except owner)."
        else:
            admin_list_text = "👥 Admin List:\n\n"
            for (admin_id,) in admins_list:
                admin_list_text += f"• `{admin_id}`"
                if admin_id == OWNER_ID:
                    admin_list_text += " 👑 Owner\n"
                else:
                    admin_list_text += "\n"
        
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔙 Back to Admin Panel", callback_data='admin_panel'))
        
        try:
            bot.edit_message_text(
                admin_list_text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )
        except Exception as e:
            bot.send_message(call.message.chat.id, admin_list_text, reply_markup=markup, parse_mode='Markdown')
            
    except Exception as e:
        logger.error(f"Error listing admins: {e}")
        bot.answer_callback_query(call.id, "❌ Error listing admins.", show_alert=True)

def add_subscription_init_callback(call):
    """Handle add subscription initiation callback"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "👤 Enter user ID to add subscription:\n/cancel to abort")
    bot.register_next_step_handler(msg, process_add_subscription)

def remove_subscription_init_callback(call):
    """Handle remove subscription initiation callback"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "👤 Enter user ID to remove subscription:\n/cancel to abort")
    bot.register_next_step_handler(msg, process_remove_subscription)

def check_subscription_init_callback(call):
    """Handle check subscription initiation callback"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "👤 Enter user ID to check subscription:\n/cancel to abort")
    bot.register_next_step_handler(msg, process_check_subscription)

def stats_callback(call):
    """Handle statistics callback"""
    user_id = call.from_user.id
    
    # Check if user is banned
    if is_user_banned(user_id):
        bot.answer_callback_query(call.id, "❌ You are banned from using this bot.", show_alert=True)
        return
    
    bot.answer_callback_query(call.id)
    
    total_users = len(active_users)
    total_files_records = sum(len(files) for files in user_files.values())
    
    running_bots_count = 0
    user_running_bots = 0
    user_uploaded_files = 0
    
    for script_key_iter, script_info_iter in list(bot_scripts.items()):
        try:
            s_owner_id, _ = script_key_iter.split('_', 1)
            if is_bot_running(int(s_owner_id), script_info_iter['file_name']):
                running_bots_count += 1
                if int(s_owner_id) == user_id:
                    user_running_bots += 1
        except:
            pass
    
    # Get banned users count
    banned_count = 0
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM banned_users')
        banned_count = c.fetchone()[0]
        conn.close()
    except Exception as e:
        logger.error(f"Error getting banned count: {e}")
    
    # User-specific stats
    user_uploaded_files = len(user_files.get(user_id, []))
    
    if user_id in admin_ids:
        # Admin/Owner sees full statistics
        stats_msg = (f"📊 ADMIN STATISTICS:\n\n"
                    f"👥 Total Users: {total_users}\n"
                    f"🚫 Banned Users: {banned_count}\n"
                    f"💎 Total File Records: {total_files_records}\n"
                    f"🟢 Total Active Bots: {running_bots_count}\n"
                    f"🔒 Bot Status: {'🔴 Locked' if bot_locked else '🟢 Unlocked'}\n"
                    f"🤖 Your Running Bots: {user_running_bots}\n"
                    f"📁 Your Uploaded Files: {user_uploaded_files}")
    else:
        # Regular users see only their own stats
        stats_msg = (f"📊 YOUR STATISTICS:\n\n"
                    f"🤖 Your Running Bots: {user_running_bots}\n"
                    f"📁 Your Uploaded Files: {user_uploaded_files}\n"
                    f"💎 Total Files Allowed: {get_user_file_limit(user_id)}")
    
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔙 Back to Main", callback_data='back_to_main'))
    
    try:
        bot.edit_message_text(
            stats_msg,
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup
        )
    except Exception as e:
        logger.error(f"Error editing stats message: {e}")
        bot.send_message(call.message.chat.id, stats_msg, reply_markup=markup)

# --- Missing Process Functions for Callbacks ---

def process_broadcast_message(message):
    """Process broadcast message"""
    admin_id = message.from_user.id
    if admin_id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    
    if message.text and message.text.lower() == '/cancel':
        bot.reply_to(message, "Broadcast cancelled.")
        return
    
    # Create confirmation buttons
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("✅ Confirm Broadcast", callback_data=f"confirm_broadcast_{message.message_id}"),
        types.InlineKeyboardButton("❌ Cancel", callback_data="cancel_broadcast")
    )
    
    bot.reply_to(message, 
                f"📢 Confirm broadcast this message to {len(active_users)} users?\n\nMessage:\n{message.text if message.text else 'File/Media'}",
                reply_markup=markup)

def handle_confirm_broadcast(call):
    """Handle broadcast confirmation"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    try:
        _, _, message_id = call.data.split('_', 2)
        original_message = bot.copy_message(call.message.chat.id, call.message.chat.id, message_id)
        
        bot.answer_callback_query(call.id, "📢 Broadcasting started...")
        
        success_count = 0
        fail_count = 0
        total = len(active_users)
        
        for user_id in list(active_users):
            try:
                # Skip banned users
                if is_user_banned(user_id):
                    continue
                    
                if original_message.content_type == 'text':
                    bot.send_message(user_id, original_message.text)
                else:
                    # Handle other content types (photo, document, etc.)
                    if original_message.content_type == 'photo':
                        bot.send_photo(user_id, original_message.photo[-1].file_id, caption=original_message.caption)
                    elif original_message.content_type == 'document':
                        bot.send_document(user_id, original_message.document.file_id, caption=original_message.caption)
                    elif original_message.content_type == 'video':
                        bot.send_video(user_id, original_message.video.file_id, caption=original_message.caption)
                    else:
                        bot.copy_message(user_id, call.message.chat.id, message_id)
                success_count += 1
                time.sleep(0.1)  # Prevent rate limiting
            except Exception as e:
                logger.error(f"Failed to send broadcast to {user_id}: {e}")
                fail_count += 1
        
        result_msg = f"✅ Broadcast completed!\n\n✅ Success: {success_count}\n❌ Failed: {fail_count}\n👥 Total: {total}"
        bot.edit_message_text(result_msg, call.message.chat.id, call.message.message_id)
        
    except Exception as e:
        logger.error(f"Error in broadcast: {e}")
        bot.answer_callback_query(call.id, "❌ Error during broadcast.", show_alert=True)

def handle_cancel_broadcast(call):
    """Handle broadcast cancellation"""
    bot.answer_callback_query(call.id, "Broadcast cancelled.")
    bot.edit_message_text("❌ Broadcast cancelled.", call.message.chat.id, call.message.message_id)

def process_add_admin(message):
    """Process adding admin"""
    if message.from_user.id != OWNER_ID:
        bot.reply_to(message, "⚠️ Owner permissions required.")
        return
    
    if message.text and message.text.lower() == '/cancel':
        bot.reply_to(message, "Add admin cancelled.")
        return
    
    try:
        new_admin_id = int(message.text.strip())
        add_admin_db(new_admin_id)
        bot.reply_to(message, f"✅ User `{new_admin_id}` added as admin.")
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid user ID. Please provide a numeric ID.")
    except Exception as e:
        logger.error(f"Error adding admin: {e}")
        bot.reply_to(message, f"❌ Error adding admin: {str(e)}")

def process_remove_admin(message):
    """Process removing admin"""
    if message.from_user.id != OWNER_ID:
        bot.reply_to(message, "⚠️ Owner permissions required.")
        return
    
    if message.text and message.text.lower() == '/cancel':
        bot.reply_to(message, "Remove admin cancelled.")
        return
    
    try:
        admin_id_to_remove = int(message.text.strip())
        
        if admin_id_to_remove == OWNER_ID:
            bot.reply_to(message, "❌ Cannot remove owner!")
            return
        
        if remove_admin_db(admin_id_to_remove):
            bot.reply_to(message, f"✅ User `{admin_id_to_remove}` removed from admins.")
        else:
            bot.reply_to(message, f"ℹ️ User `{admin_id_to_remove}` was not an admin.")
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid user ID. Please provide a numeric ID.")
    except Exception as e:
        logger.error(f"Error removing admin: {e}")
        bot.reply_to(message, f"❌ Error removing admin: {str(e)}")

def process_add_subscription(message):
    """Process adding subscription"""
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    
    if message.text and message.text.lower() == '/cancel':
        bot.reply_to(message, "Add subscription cancelled.")
        return
    
    try:
        user_id = int(message.text.strip())
        
        # Ask for duration
        markup = types.InlineKeyboardMarkup(row_width=3)
        markup.add(
            types.InlineKeyboardButton("1 Month", callback_data=f"sub_1_{user_id}"),
            types.InlineKeyboardButton("3 Months", callback_data=f"sub_3_{user_id}"),
            types.InlineKeyboardButton("6 Months", callback_data=f"sub_6_{user_id}"),
            types.InlineKeyboardButton("1 Year", callback_data=f"sub_12_{user_id}"),
            types.InlineKeyboardButton("Custom", callback_data=f"sub_custom_{user_id}")
        )
        
        bot.reply_to(message, f"Select subscription duration for user `{user_id}`:", reply_markup=markup, parse_mode='Markdown')
        
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid user ID. Please provide a numeric ID.")
        msg = bot.send_message(message.chat.id, "👤 Enter user ID to add subscription:\n/cancel to abort")
        bot.register_next_step_handler(msg, process_add_subscription)
    except Exception as e:
        logger.error(f"Error adding subscription: {e}")
        bot.reply_to(message, f"❌ Error adding subscription: {str(e)}")

def process_remove_subscription(message):
    """Process removing subscription"""
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    
    if message.text and message.text.lower() == '/cancel':
        bot.reply_to(message, "Remove subscription cancelled.")
        return
    
    try:
        user_id = int(message.text.strip())
        
        if remove_subscription_db(user_id):
            bot.reply_to(message, f"✅ Subscription removed for user `{user_id}`.")
        else:
            bot.reply_to(message, f"ℹ️ User `{user_id}` had no active subscription.")
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid user ID. Please provide a numeric ID.")
        msg = bot.send_message(message.chat.id, "👤 Enter user ID to remove subscription:\n/cancel to abort")
        bot.register_next_step_handler(msg, process_remove_subscription)
    except Exception as e:
        logger.error(f"Error removing subscription: {e}")
        bot.reply_to(message, f"❌ Error removing subscription: {str(e)}")

def process_check_subscription(message):
    """Process checking subscription"""
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    
    if message.text and message.text.lower() == '/cancel':
        bot.reply_to(message, "Check subscription cancelled.")
        return
    
    try:
        user_id = int(message.text.strip())
        
        if user_id in user_subscriptions:
            expiry = user_subscriptions[user_id]['expiry']
            if expiry > datetime.now():
                days_left = (expiry - datetime.now()).days
                status = f"✅ ACTIVE (expires in {days_left} days)"
            else:
                status = "❌ EXPIRED"
            expiry_str = expiry.strftime("%Y-%m-%d %H:%M:%S")
            bot.reply_to(message, f"📅 Subscription for user `{user_id}`:\n\nStatus: {status}\nExpiry: {expiry_str}")
        else:
            bot.reply_to(message, f"ℹ️ User `{user_id}` has no subscription.")
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid user ID. Please provide a numeric ID.")
        msg = bot.send_message(message.chat.id, "👤 Enter user ID to check subscription:\n/cancel to abort")
        bot.register_next_step_handler(msg, process_check_subscription)
    except Exception as e:
        logger.error(f"Error checking subscription: {e}")
        bot.reply_to(message, f"❌ Error checking subscription: {str(e)}")

# --- Missing Bot Control Callbacks ---

def stop_bot_callback(call):
    """Handle stop bot callback"""
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True)
            return
        
        script_key = f"{script_owner_id}_{file_name}"
        
        if script_key in bot_scripts:
            kill_process_tree(bot_scripts[script_key])
            
            # Wait a moment for cleanup
            time.sleep(0.5)
            
            # Remove from dictionary
            if script_key in bot_scripts:
                del bot_scripts[script_key]
            
            bot.answer_callback_query(call.id, f"✅ Script '{file_name}' stopped.")
            
            # Update message with new control buttons
            try:
                bot.edit_message_reply_markup(
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=create_control_buttons(script_owner_id, file_name, False)
                )
            except Exception as e:
                logger.error(f"Error updating buttons after stop: {e}")
        else:
            bot.answer_callback_query(call.id, "⚠️ Script not running.", show_alert=True)
            
    except Exception as e:
        logger.error(f"Error in stop_bot_callback: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "❌ Error stopping script.", show_alert=True)

def restart_bot_callback(call):
    """Handle restart bot callback"""
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True)
            return
        
        # First stop if running
        script_key = f"{script_owner_id}_{file_name}"
        if script_key in bot_scripts:
            kill_process_tree(bot_scripts[script_key])
            time.sleep(1)
            if script_key in bot_scripts:
                del bot_scripts[script_key]
        
        # Then start
        user_folder = get_user_folder(script_owner_id)
        file_path = os.path.join(user_folder, file_name)
        
        # Find file type
        user_files_list = user_files.get(script_owner_id, [])
        file_info = next((f for f in user_files_list if f[0] == file_name), None)
        
        if not file_info:
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True)
            return
        
        file_type = file_info[1]
        bot.answer_callback_query(call.id, f"🔄 Restarting {file_name}...")
        
        if file_type == 'py':
            threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        elif file_type == 'js':
            threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        
        # Wait and update status
        time.sleep(2)
        is_now_running = is_bot_running(script_owner_id, file_name)
        
        try:
            bot.edit_message_reply_markup(
                call.message.chat.id,
                call.message.message_id,
                reply_markup=create_control_buttons(script_owner_id, file_name, is_now_running)
            )
        except Exception as e:
            logger.error(f"Error updating buttons after restart: {e}")
            
    except Exception as e:
        logger.error(f"Error in restart_bot_callback: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "❌ Error restarting script.", show_alert=True)

def delete_bot_callback(call):
    """Handle delete bot callback"""
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True)
            return
        
        # Stop if running
        script_key = f"{script_owner_id}_{file_name}"
        if script_key in bot_scripts:
            kill_process_tree(bot_scripts[script_key])
            time.sleep(0.5)
            if script_key in bot_scripts:
                del bot_scripts[script_key]
        
        # Delete file
        user_folder = get_user_folder(script_owner_id)
        file_path = os.path.join(user_folder, file_name)
        
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                # Also delete log file if exists
                log_file = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
                if os.path.exists(log_file):
                    os.remove(log_file)
            except Exception as e:
                logger.error(f"Error deleting file {file_path}: {e}")
        
        # Remove from database
        remove_user_file_db(script_owner_id, file_name)
        
        bot.answer_callback_query(call.id, f"🗑️ File '{file_name}' deleted.")
        
        # Go back to files list
        check_files_callback(call)
        
    except Exception as e:
        logger.error(f"Error in delete_bot_callback: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "❌ Error deleting file.", show_alert=True)

def logs_bot_callback(call):
    """Handle view logs callback"""
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True)
            return
        
        user_folder = get_user_folder(script_owner_id)
        log_file_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        
        if not os.path.exists(log_file_path):
            bot.answer_callback_query(call.id, "📄 No log file found.", show_alert=True)
            return
        
        bot.answer_callback_query(call.id)
        
        # Read last 100 lines of log
        try:
            with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
                if len(lines) > 100:
                    log_content = ''.join(lines[-100:])
                    log_content = f"...showing last 100 lines...\n{log_content}"
                else:
                    log_content = ''.join(lines)
                
                if not log_content.strip():
                    log_content = "Log file is empty."
        except Exception as e:
            log_content = f"Error reading log file: {str(e)}"
        
        # Create back button
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔙 Back to Controls", callback_data=f'file_{script_owner_id}_{file_name}'))
        
        # Send log content (Telegram has 4096 char limit per message)
        if len(log_content) > 4000:
            log_content = log_content[-4000:] + "\n\n... (truncated)"
        
        log_display = f"📜 Logs for `{file_name}`:\n```\n{log_content}\n```"
        
        try:
            bot.send_message(
                call.message.chat.id,
                log_display,
                reply_markup=markup,
                parse_mode='Markdown'
            )
        except Exception as e:
            # If too long, send as file
            if "message is too long" in str(e):
                with open(log_file_path, 'rb') as log_file:
                    bot.send_document(
                        call.message.chat.id,
                        log_file,
                        caption=f"Logs for {file_name}",
                        reply_markup=markup
                    )
            else:
                raise e
                
    except Exception as e:
        logger.error(f"Error in logs_bot_callback: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "❌ Error viewing logs.", show_alert=True)

# --- Subscription Duration Callbacks ---

@bot.callback_query_handler(func=lambda call: call.data.startswith('sub_'))
def handle_subscription_duration(call):
    """Handle subscription duration selection"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    
    try:
        _, duration_str, user_id_str = call.data.split('_', 2)
        user_id = int(user_id_str)
        
        if duration_str == 'custom':
            bot.answer_callback_query(call.id, "Enter custom duration in days:")
            msg = bot.send_message(call.message.chat.id, "📅 Enter subscription duration in days:")
            
            def process_custom_duration(message):
                if message.text and message.text.lower() == '/cancel':
                    bot.reply_to(message, "Cancelled.")
                    return
                
                try:
                    days = int(message.text.strip())
                    expiry = datetime.now() + timedelta(days=days)
                    save_subscription(user_id, expiry)
                    bot.reply_to(message, f"✅ Added {days} days subscription for user `{user_id}`.\nExpiry: {expiry.strftime('%Y-%m-%d %H:%M:%S')}")
                except ValueError:
                    bot.reply_to(message, "⚠️ Invalid number. Please enter days as a number.")
                except Exception as e:
                    logger.error(f"Error adding custom subscription: {e}")
                    bot.reply_to(message, f"❌ Error: {str(e)}")
            
            bot.register_next_step_handler(msg, process_custom_duration)
            return
        
        # Process predefined durations
        duration = int(duration_str)  # in months
        expiry = datetime.now() + timedelta(days=duration * 30)  # approx 30 days per month
        
        save_subscription(user_id, expiry)
        
        bot.answer_callback_query(call.id, f"✅ Added {duration} month(s) subscription.")
        
        # Update the message
        bot.edit_message_text(
            f"✅ Subscription added for user `{user_id}`:\n\nDuration: {duration} month(s)\nExpiry: {expiry.strftime('%Y-%m-%d %H:%M:%S')}",
            call.message.chat.id,
            call.message.message_id
        )
        
    except Exception as e:
        logger.error(f"Error handling subscription duration: {e}")
        bot.answer_callback_query(call.id, "❌ Error adding subscription.", show_alert=True)

def ban_user_callback(call):
    """Callback for ban user button"""
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "🚫 Enter User ID to ban (and optional reason).\nFormat: `12345678 reason here`\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_ban_user)

def unban_user_callback(call):
    """Callback for unban user button"""
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "✅ Enter User ID to unban.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_unban_user)

# --- Cleanup Function ---
def cleanup():
    logger.warning("Shutdown. Cleaning up processes...")
    script_keys_to_stop = list(bot_scripts.keys()) 
    if not script_keys_to_stop: logger.info("No scripts running. Exiting."); return
    logger.info(f"Stopping {len(script_keys_to_stop)} scripts...")
    for key in script_keys_to_stop:
        if key in bot_scripts: logger.info(f"Stopping: {key}"); kill_process_tree(bot_scripts[key])
        else: logger.info(f"Script {key} already removed.")
    logger.warning("Cleanup finished.")
atexit.register(cleanup)

# --- Main Execution ---
if __name__ == '__main__':
    logger.info("="*40 + "\n🤖 Bot Starting Up...\n" + f"🐍 Python: {sys.version.split()[0]}\n" +
                f"🔧 Base Dir: {BASE_DIR}\n📁 Upload Dir: {UPLOAD_BOTS_DIR}\n" +
                f"📊 Data Dir: {IROTECH_DIR}\n🔑 Owner ID: {OWNER_ID}\n🛡️ Admins: {admin_ids}\n" + "="*40)
    keep_alive()
    logger.info("🚀 Starting polling...")
    while True:
        try:
            bot.infinity_polling(logger_level=logging.INFO, timeout=60, long_polling_timeout=30)
        except requests.exceptions.ReadTimeout: logger.warning("Polling ReadTimeout. Restarting in 5s..."); time.sleep(5)
        except requests.exceptions.ConnectionError as ce: logger.error(f"Polling ConnectionError: {ce}. Retrying in 15s..."); time.sleep(15)
        except Exception as e:
            logger.critical(f"💥 Unrecoverable polling error: {e}", exc_info=True)
            logger.info("Restarting polling in 30s due to critical error..."); time.sleep(30)
        finally: logger.warning("Polling attempt finished. Will restart if in loop."); time.sleep(1)