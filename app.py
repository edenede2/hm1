import io
import uuid
import random
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone, date
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError
# -------------------------------------------------------------------
# CONFIG & CONSTANTS
# -------------------------------------------------------------------

st.set_page_config(page_title="Household Manager", layout="wide")

# Google Sheets & Drive
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

PAYCHECKS_SHEET = "paychecks"
ITEMS_SHEET = "items"
ARCHIVE_SHEET = "archive"

PAYCHECKS_HEADERS = ["username", "pay1", "pay2", "pay3", "average"]

ITEMS_HEADERS = [
    "id",
    "purchase_id",
    "timestamp",
    "purchase_date",
    "uploader",
    "debtor",
    "description",
    "amount_total",
    "amount_owed",
    "share_type",
    "receipt_url",
    "paid",
    "paid_at",
    "paid_by",
]

ARCHIVE_HEADERS = ITEMS_HEADERS + [
    "approved",
    "approved_at",
    "approved_by",
]

ITEMS_COL_INDEX = {name: idx + 1 for idx, name in enumerate(ITEMS_HEADERS)}
ARCHIVE_COL_INDEX = {name: idx + 1 for idx, name in enumerate(ARCHIVE_HEADERS)}

# Random greetings and emojis
GREETINGS = [
    "Welcome back, {name}! 🎉",
    "Hello, {name}! 💰",
    "Hey there, {name}! 👋",
    "Good to see you, {name}! ✨",
    "Hi {name}! 🌟",
    "Greetings, {name}! 🚀",
    "What's up, {name}? 💫",
    "Nice to have you here, {name}! 🎊",
    "Wonderful to see you, {name}! 🌺",
    "Great day to you, {name}! ☀️",
    "Looking good, {name}! 🌸",
    "You're awesome, {name}! 🎯",
]

DASHBOARD_EMOJIS = ["💸", "💰", "💳", "🏦", "💵", "💴", "💶", "💷"]
PAYCHECK_EMOJIS = ["💼", "💵", "💰", "📊", "💳", "🏦"]
EXPENSE_EMOJIS = ["🛒", "🛍️", "🧾", "💳", "📝", "✍️"]
APPROVE_EMOJIS = ["✅", "👍", "✔️", "👌", "🎯"]
HISTORY_EMOJIS = ["📜", "📋", "📊", "📈", "📁", "🗂️"]

# Positive psychology messages for different sections
DASHBOARD_INTRO = [
    "Great job keeping track of your finances! 🌟",
    "You're doing amazing sweety, with your household management! 💪",
    "Financial transparency builds stronger relationships! 🤝",
    "Every payment brings you closer to financial freedom! 🦋",
    "You're making smart money moves today! 🎯",
    "Your financial awareness is impressive! 🌈",
    "Taking control of expenses shows real maturity! 🌱",
    "You're building healthy financial habits! ✨",
    "Managing together makes everything easier! 🏡",
    "Your dedication to fairness is admirable! 💎",
]

NO_DEBT_MESSAGES = [
    "You're all clear! Time to celebrate! 🎉",
    "Debt-free zone! You're crushing it! 🌈",
    "Nothing owed! You're financially free! ✨",
    "Clean slate! You're doing fantastic! 🧼",
    "All squared away! Keep up the great work! 🔲",
    "You're golden! Enjoy this moment! 🌟",
    "Perfectly balanced! You're a star! ⭐",
    "Zero debts! You're rocking this! 🚀",
    "All paid up! You're amazing! 🎊",
    "No obligations! You're ahead of the game! 🏆",
    "Clear account! You should feel proud! 💫",
    "Nothing pending! You're on top of it! 🎯",
]

NO_CREDITS_MESSAGES = [
    "No one owes you money right now. Time to treat yourself! 🎁",
    "All settled! Your generosity creates great relationships! 💝",
    "Everyone's caught up! You're fostering trust! 🤝",
    "Nothing pending! You make managing easy! 🌸",
    "All clear! Your patience is appreciated! 🌺",
    "Fully reconciled! You're creating harmony! 🎵",
    "No debts owed! You're building strong bonds! 💪",
    "Everyone's square! You're a great household partner! 🏡",
]

PAYCHECK_INTRO = [
    "Keeping income updated ensures fair sharing! 📊",
    "Your transparency makes everything work smoothly! 🌟",
    "Regular updates show responsibility! 💼",
    "Accurate data creates trust and fairness! 🤝",
    "You're contributing to household harmony! 🎵",
    "Income tracking leads to better planning! 📈",
    "Your diligence is the key to success! 🔑",
    "Staying current shows you care! 💙",
]

EXPENSE_INTRO = [
    "Sharing expenses brings everyone closer! 🤝",
    "You're making fair sharing easy! 🌟",
    "Every entry helps maintain balance! ⚖️",
    "Your honesty creates trust! 💎",
    "Recording expenses shows integrity! ✨",
    "You're building financial transparency! 🌈",
    "Tracking together makes life simpler! 🧘",
    "Your contribution keeps things fair! 🎯",
]

EXPENSE_SUCCESS = [
    "Expense added successfully! You're staying organized! 🎉",
    "Great job tracking that expense! 🌟",
    "Added! Your diligence is paying off! 💪",
    "Recorded! You're building good habits! 🌱",
    "Done! Financial clarity feels good! ✨",
    "Success! You're making smart moves! 🚀",
    "Added! Transparency creates trust! 🤝",
    "Saved! You're doing amazing! 💫",
]

PAYMENT_MARKED_MESSAGES = [
    "Payment marked! You're being responsible! 👏",
    "Great job staying on top of debts! 🌟",
    "Marked as paid! You're building trust! 🤝",
    "Awesome! Your reliability is admirable! 💎",
    "Payment noted! You're doing the right thing! ✨",
    "Well done! Your integrity shines! 🌈",
    "Marked! You're creating positive relationships! 💝",
    "Recorded! Your commitment is inspiring! 🚀",
]

APPROVAL_INTRO = [
    "Your review helps maintain household harmony! 🏡",
    "Approving payments builds mutual trust! 🤝",
    "Your confirmation creates peace of mind! 💙",
    "Quick reviews keep relationships strong! 💪",
    "You're the final piece of the puzzle! 🧩",
    "Your approval validates their effort! 🌟",
    "Checking in shows you care! 💝",
    "Your diligence protects everyone! 🛡️",
]

NO_APPROVALS_MESSAGES = [
    "All caught up! No payments need your attention! ✨",
    "Nothing to approve! Everyone's in sync! 🎵",
    "All clear! Your household is running smoothly! 🌊",
    "No pending approvals! You're all set! 🎯",
    "Everything's approved! Great teamwork! 🤝",
    "Nothing waiting! You're on top of things! 🏔️",
    "All processed! The system works! ⚙️",
    "Zero pending! You're doing great! 🌟",
]

APPROVAL_SUCCESS = [
    "Payment approved! You're building trust! 🤝",
    "Approved! Financial harmony achieved! 🎵",
    "Confirmed! You're a great partner! 💝",
    "Done! Your fairness is appreciated! 🌟",
    "Approved! Relationships strengthened! 💪",
    "Success! You're creating positivity! ✨",
    "Confirmed! Trust grows with every approval! 🌱",
    "Approved! You're making a difference! 🌈",
]

HISTORY_INTRO = [
    "Looking back helps you move forward! 🚀",
    "Your history shows your growth! 📈",
    "Transparency creates accountability! 📊",
    "Every transaction tells your story! 📖",
    "Reviewing builds awareness! 🧠",
    "Your records show responsibility! 💼",
    "History reveals your progress! 🌱",
    "Past data guides future success! 🎯",
]

NO_HISTORY_MESSAGES = [
    "No transaction history yet. Start fresh! 🌱",
    "Empty canvas! Your journey begins now! 🎨",
    "Clean slate! Build something great! ✨",
    "No history yet! Every journey starts somewhere! 🚀",
    "Fresh start! Your story begins today! 📖",
]

DELETE_SUCCESS = [
    "Deleted successfully! Staying organized! 🗑️",
    "Removed! You're maintaining clarity! ✨",
    "Deleted! Clean records feel good! 🧼",
    "Gone! You're keeping things tidy! 🌟",
    "Removed! Accuracy matters! 🎯",
    "Deleted! You're in control! 💪",
]

NO_EXPENSES_MESSAGES = [
    "You haven't created any expenses yet. Ready to start? 🌱",
    "No expenses recorded! Your journey begins! 🚀",
    "Clean slate! Add your first expense! ✨",
    "Empty list! Time to begin tracking! 📝",
]

def get_random_greeting(display_name: str) -> str:
    """Get a random personalized greeting."""
    return random.choice(GREETINGS).format(name=display_name)

def get_random_message(message_list: List[str]) -> str:
    """Get a random message from a list."""
    return random.choice(message_list)

def get_random_emoji(emoji_list: List[str]) -> str:
    """Get a random emoji from a list."""
    return random.choice(emoji_list)

# -------------------------------------------------------------------
# EMAIL NOTIFICATIONS
# -------------------------------------------------------------------

def get_user_email(username: str) -> str:
    """Get the full email address for a username."""
    return f"{username}@gmail.com"

def get_all_user_emails() -> List[str]:
    """Get email addresses for all users."""
    users_cfg = st.secrets.get("users", {})
    return [get_user_email(username) for username in users_cfg.keys()]

def create_email_html(title: str, body_content: str, action_user: str) -> str:
    """Create a beautiful HTML email template."""
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background-color: #f5f5f5;
                margin: 0;
                padding: 0;
            }}
            .container {{
                max-width: 600px;
                margin: 20px auto;
                background-color: #ffffff;
                border-radius: 10px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                overflow: hidden;
            }}
            .header {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 30px;
                text-align: center;
            }}
            .header h1 {{
                margin: 0;
                font-size: 28px;
            }}
            .content {{
                padding: 30px;
                color: #333;
            }}
            .content h2 {{
                color: #667eea;
                margin-top: 0;
            }}
            .info-box {{
                background-color: #f8f9fa;
                border-left: 4px solid #667eea;
                padding: 15px;
                margin: 20px 0;
                border-radius: 4px;
            }}
            .button {{
                display: inline-block;
                background-color: #667eea;
                color: white;
                padding: 12px 30px;
                text-decoration: none;
                border-radius: 5px;
                margin: 20px 0;
            }}
            .footer {{
                background-color: #f8f9fa;
                color: #666;
                text-align: center;
                padding: 20px;
                font-size: 12px;
            }}
            .emoji {{
                font-size: 24px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🏠 Household Manager</h1>
                <p style="margin: 10px 0 0 0; opacity: 0.9;">{title}</p>
            </div>
            <div class="content">
                {body_content}
                <p style="margin-top: 30px; color: #666;">
                    <strong>Action by:</strong> {action_user}
                </p>
            </div>
            <div class="footer">
                <p>This is an automated notification from your Household Manager app.</p>
                <p>💙 Manage your household expenses with ease</p>
            </div>
        </div>
    </body>
    </html>
    """

def send_email_notification(
    subject: str,
    title: str,
    body_content: str,
    action_user: str,
    recipients: Optional[List[str]] = None
) -> None:
    """Send email notification to users."""
    try:
        sender_email = st.secrets["app"]["SENDER_EMAIL_ADDRESS"]
        sender_password = st.secrets["app"]["SENDER_EMAIL_PASSWORD"]
    except KeyError:
        # Email not configured, skip silently
        return
    
    if recipients is None:
        recipients = get_all_user_emails()
    
    if not recipients:
        return
    
    try:
        # Create message
        msg = MIMEMultipart('alternative')
        msg['From'] = sender_email
        msg['To'] = ', '.join(recipients)
        msg['Subject'] = f"🏠 {subject}"
        
        # Create HTML content
        html_content = create_email_html(title, body_content, action_user)
        html_part = MIMEText(html_content, 'html')
        msg.attach(html_part)
        
        # Send email
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender_email, sender_password)
            server.send_message(msg)
    except Exception as e:
        # Don't crash the app if email fails, just log it
        print(f"Failed to send email: {e}")

def notify_new_expense(
    uploader: str,
    description: str,
    total_amount: float,
    affected_users: List[str]
) -> None:
    """Send notification when a new expense is added."""
    users_cfg = st.secrets.get("users", {})
    uploader_name = users_cfg.get(uploader, uploader)
    
    affected_names = [users_cfg.get(u, u) for u in affected_users]
    affected_list = ", ".join(affected_names)
    
    body = f"""
    <h2>💸 New Expense Added</h2>
    <div class="info-box">
        <p><strong>Description:</strong> {description}</p>
        <p><strong>Total Amount:</strong> ${total_amount:,.2f}</p>
        <p><strong>Shared with:</strong> {affected_list}</p>
    </div>
    <p>{uploader_name} has added a new shared expense. Check your dashboard to see your share!</p>
    """
    
    send_email_notification(
        subject=f"New Expense: {description}",
        title="New Expense Alert",
        body_content=body,
        action_user=uploader_name
    )

def notify_multiple_expenses(
    uploader: str,
    expenses: List[Dict],
    affected_users: List[str]
) -> None:
    """Send notification when multiple expenses are added."""
    users_cfg = st.secrets.get("users", {})
    uploader_name = users_cfg.get(uploader, uploader)
    
    affected_names = [users_cfg.get(u, u) for u in affected_users]
    affected_list = ", ".join(affected_names)
    
    total_all = sum(exp['amount'] for exp in expenses)
    
    # Build expense list
    expense_items = ""
    for exp in expenses:
        expense_items += f"<li><strong>{exp['description']}</strong> - ${exp['amount']:,.2f}</li>"
    
    body = f"""
    <h2>💸 Multiple Expenses Added</h2>
    <p>{uploader_name} has added {len(expenses)} new shared expenses:</p>
    <div class="info-box">
        <ul style="margin: 10px 0;">
            {expense_items}
        </ul>
        <p style="border-top: 2px solid #667eea; padding-top: 10px; margin-top: 10px;">
            <strong>Total Amount:</strong> ${total_all:,.2f}
        </p>
        <p><strong>Shared with:</strong> {affected_list}</p>
    </div>
    <p>Check your dashboard to see your share of each expense!</p>
    """
    
    send_email_notification(
        subject=f"{len(expenses)} New Expenses Added",
        title="Multiple Expenses Alert",
        body_content=body,
        action_user=uploader_name
    )

def notify_payment_marked(
    debtor: str,
    uploader: str,
    description: str,
    amount: float
) -> None:
    """Send notification when a debt is marked as paid."""
    users_cfg = st.secrets.get("users", {})
    debtor_name = users_cfg.get(debtor, debtor)
    uploader_name = users_cfg.get(uploader, uploader)
    
    body = f"""
    <h2>✅ Payment Marked as Paid</h2>
    <div class="info-box">
        <p><strong>Description:</strong> {description}</p>
        <p><strong>Amount:</strong> ${amount:,.2f}</p>
        <p><strong>Paid by:</strong> {debtor_name}</p>
        <p><strong>Awaiting approval from:</strong> {uploader_name}</p>
    </div>
    <p>{debtor_name} has marked a payment as completed. {uploader_name}, please review and approve this payment.</p>
    """
    
    # Send to uploader who needs to approve
    send_email_notification(
        subject=f"Payment Pending Approval: {description}",
        title="Payment Marked as Paid",
        body_content=body,
        action_user=debtor_name,
        recipients=[get_user_email(uploader)]
    )

def notify_payment_approved(
    debtor: str,
    uploader: str,
    description: str,
    amount: float
) -> None:
    """Send notification when a payment is approved."""
    users_cfg = st.secrets.get("users", {})
    debtor_name = users_cfg.get(debtor, debtor)
    uploader_name = users_cfg.get(uploader, uploader)
    
    body = f"""
    <h2>🎉 Payment Approved!</h2>
    <div class="info-box">
        <p><strong>Description:</strong> {description}</p>
        <p><strong>Amount:</strong> ${amount:,.2f}</p>
        <p><strong>Approved by:</strong> {uploader_name}</p>
    </div>
    <p>Great news! {uploader_name} has approved your payment for "{description}". This transaction is now complete!</p>
    """
    
    # Send to debtor who made the payment
    send_email_notification(
        subject=f"Payment Approved: {description}",
        title="Payment Approved",
        body_content=body,
        action_user=uploader_name,
        recipients=[get_user_email(debtor)]
    )

def notify_expense_deleted(
    uploader: str,
    description: str,
    total_amount: float,
    affected_users: List[str]
) -> None:
    """Send notification when an expense is deleted."""
    users_cfg = st.secrets.get("users", {})
    uploader_name = users_cfg.get(uploader, uploader)
    
    affected_names = [users_cfg.get(u, u) for u in affected_users]
    affected_list = ", ".join(affected_names)
    
    body = f"""
    <h2>🗑️ Expense Deleted</h2>
    <div class="info-box">
        <p><strong>Description:</strong> {description}</p>
        <p><strong>Total Amount:</strong> ${total_amount:,.2f}</p>
        <p><strong>Previously shared with:</strong> {affected_list}</p>
    </div>
    <p>{uploader_name} has deleted this expense. All associated debts have been removed.</p>
    """
    
    send_email_notification(
        subject=f"Expense Deleted: {description}",
        title="Expense Removed",
        body_content=body,
        action_user=uploader_name
    )

# How we present the share types in the UI vs. how we store them
SHARE_TYPE_OPTIONS = {
    "Only me (no sharing)": "self",
    "Relative to income – all users": "relative_all",
    "Relative to income – other users only": "relative_others",
}


# -------------------------------------------------------------------
# GOOGLE CLIENTS
# -------------------------------------------------------------------

@st.cache_resource(show_spinner=False)
def get_clients():
    """
    Returns (gspread_client, drive_service, spreadsheet) using
    service-account credentials from st.secrets["gcp_service_account"].

    st.secrets expected structure:

    [auth]                # OIDC config for Google login (see docs)
    redirect_uri = ...
    cookie_secret = ...
    client_id = ...
    client_secret = ...
    server_metadata_url = "https://accounts.google.com/.well-known/openid-configuration"

    [gcp_service_account] # full JSON of service account

    [app]
    spreadsheet_id = "your-google-sheet-id"
    drive_receipts_folder_id = "optional-drive-folder-id"

    [users]
    eden = "Eden Eldar"
    alice = "Alice Example"
    ...
    """
    credentials = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=SCOPES,
    )
    gc = gspread.authorize(credentials)
    drive = build("drive", "v3", credentials=credentials)
    spreadsheet = gc.open_by_key(st.secrets["app"]["spreadsheet_id"])
    return gc, drive, spreadsheet


def get_or_create_worksheet(spreadsheet, title: str, headers: List[str]):
    """Get a worksheet by name or create it with the given header row."""
    try:
        ws = spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows=1000, cols=len(headers))
        ws.append_row(headers)
        return ws

    # Skip the header check - assume headers exist if worksheet exists
    # This saves one API call per worksheet access
    return ws


# -------------------------------------------------------------------
# DATA ACCESS HELPERS WITH CACHING
# -------------------------------------------------------------------

def invalidate_data_cache():
    """Call this after any data modification to clear cached data."""
    st.cache_data.clear()


@st.cache_data(ttl=30, show_spinner=False)
def _cached_load_paychecks() -> List[Dict]:
    """Cached version of paychecks loading - returns raw records."""
    _, _, spreadsheet = get_clients()
    ws = get_or_create_worksheet(spreadsheet, PAYCHECKS_SHEET, PAYCHECKS_HEADERS)
    return ws.get_all_records()


def load_paychecks_df() -> pd.DataFrame:
    records = _cached_load_paychecks()
    if not records:
        return pd.DataFrame(columns=PAYCHECKS_HEADERS)

    df = pd.DataFrame(records)
    for col in PAYCHECKS_HEADERS:
        if col not in df.columns:
            df[col] = None
    return df[PAYCHECKS_HEADERS]


def upsert_paychecks(username: str, p1: float, p2: float, p3: float) -> None:
    _, _, spreadsheet = get_clients()
    ws = get_or_create_worksheet(spreadsheet, PAYCHECKS_SHEET, PAYCHECKS_HEADERS)

    df = load_paychecks_df()
    avg_val = float(pd.Series([p1, p2, p3]).mean())

    if not df.empty and username in df["username"].values:
        idx = df.index[df["username"] == username][0]
        row_number = idx + 2  # +1 for 0-based index, +1 for header row
        ws.update(f"A{row_number}:E{row_number}", [[username, p1, p2, p3, avg_val]])
    else:
        ws.append_row([username, p1, p2, p3, avg_val])
    
    # Invalidate cache after modification
    invalidate_data_cache()


@st.cache_data(ttl=30, show_spinner=False)
def _cached_compute_income_means() -> Dict[str, float]:
    """Cached computation of income means."""
    records = _cached_load_paychecks()
    if not records:
        return {}

    df = pd.DataFrame(records)
    for col in PAYCHECKS_HEADERS:
        if col not in df.columns:
            df[col] = None
    df = df[PAYCHECKS_HEADERS]

    for col in ["pay1", "pay2", "pay3"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["average"] = df[["pay1", "pay2", "pay3"]].mean(axis=1)

    df = df.dropna(subset=["username", "average"])
    return dict(zip(df["username"], df["average"]))


def compute_income_means() -> Dict[str, float]:
    """Return {username: mean_of_last_3_paychecks}."""
    return _cached_compute_income_means()


@st.cache_data(ttl=30, show_spinner=False)
def _cached_load_items() -> List[Dict]:
    """Cached version of items loading - returns raw records."""
    _, _, spreadsheet = get_clients()
    ws = get_or_create_worksheet(spreadsheet, ITEMS_SHEET, ITEMS_HEADERS)
    return ws.get_all_records()


def load_items_df() -> pd.DataFrame:
    records = _cached_load_items()
    if not records:
        return pd.DataFrame(columns=ITEMS_HEADERS)

    df = pd.DataFrame(records)
    for col in ITEMS_HEADERS:
        if col not in df.columns:
            df[col] = None

    if not df.empty:
        for col in ["amount_total", "amount_owed"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        # Don't convert paid to bool here - let the UI handle it properly

    return df[ITEMS_HEADERS]


@st.cache_data(ttl=30, show_spinner=False)
def _cached_load_archive() -> List[Dict]:
    """Cached version of archive loading - returns raw records."""
    _, _, spreadsheet = get_clients()
    ws = get_or_create_worksheet(spreadsheet, ARCHIVE_SHEET, ARCHIVE_HEADERS)
    return ws.get_all_records()


def load_archive_df() -> pd.DataFrame:
    records = _cached_load_archive()
    if not records:
        return pd.DataFrame(columns=ARCHIVE_HEADERS)

    df = pd.DataFrame(records)
    for col in ARCHIVE_HEADERS:
        if col not in df.columns:
            df[col] = None

    if not df.empty:
        # Don't convert paid/approved to bool here - let the UI handle it properly
        for col in ["amount_total", "amount_owed"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df[ARCHIVE_HEADERS]


# -------------------------------------------------------------------
# FILE UPLOAD → GOOGLE DRIVE
# -------------------------------------------------------------------

def upload_receipt_file(uploaded_file, purchase_id: str) -> Optional[str]:
    """
    Upload receipt to Drive, return a sharing URL (or None).

    If drive_receipts_folder_id is not configured or upload fails,
    returns None instead of crashing.
    """
    if uploaded_file is None:
        return None

    # Get folder ID from secrets
    folder_id = st.secrets["app"].get("drive_receipts_folder_id", "").strip()
    if not folder_id:
        # Attachments disabled – just skip silently
        return None

    _, drive_service, _ = get_clients()

    # Read file into memory
    file_bytes = uploaded_file.read()
    if not file_bytes:
        return None

    media = MediaIoBaseUpload(
        io.BytesIO(file_bytes),
        mimetype=uploaded_file.type or "application/octet-stream",
        resumable=False,
    )
    metadata = {
        "name": f"{purchase_id}_{uploaded_file.name}",
        "parents": [folder_id],
    }

    try:
        file = (
            drive_service.files()
            .create(
                body=metadata,
                media_body=media,
                fields="id,webViewLink,webContentLink",
            )
            .execute()
        )
    except HttpError as e:
        # 403 with 'Service Accounts do not have storage quota' is common here
        st.warning(
            "Could not upload receipt to Google Drive. "
            "The expense was created without a stored receipt.\n\n"
            f"Drive API error: {e}"
        )
        return None

    return file.get("webViewLink") or file.get("webContentLink")

# -------------------------------------------------------------------
# BUSINESS LOGIC
# -------------------------------------------------------------------

def add_expense_and_create_debts(
    uploader: str,
    description: str,
    total_amount: float,
    share_type: str,
    purchase_date: Optional[date] = None,
    uploaded_file=None,
) -> None:
    """
    Create one 'debt' row per debtor in the items sheet.
    For single expense - calls the batch function with a single expense.
    """
    expenses = [{
        "description": description,
        "amount": total_amount,
        "date": purchase_date,
        "file": uploaded_file,
    }]
    batch_add_expenses_and_create_debts(uploader, expenses, share_type)


def batch_add_expenses_and_create_debts(
    uploader: str,
    expenses: List[Dict],
    share_type: str,
) -> int:
    """
    Create debt rows for multiple expenses in a single batch operation.
    This is much more efficient than calling add_expense_and_create_debts for each expense.

    Args:
        uploader: Username of the person who paid
        expenses: List of dicts with keys: description, amount, date, file (optional)
        share_type: How to split the expense

    Returns:
        Number of successfully created expenses

    Semantics:
      - share_type == "self":
          Only uploader pays. No rows created (no one owes anyone).
      - share_type == "relative_all":
          All users with paycheck data share cost relative to their average income.
          Uploader's own share is implicit and NOT stored as a debt row.
          Everyone else gets a row (debtor -> uploader).
      - share_type == "relative_others":
          Only other users (excluding uploader) share cost relative to their incomes.
          Uploader is fully reimbursed (cost is fully split among others).
    """
    if share_type == "self":
        # Only uploader – nothing to record for debts.
        return 0

    # Validate all expenses first
    valid_expenses = []
    for exp in expenses:
        amount = exp.get("amount", 0)
        desc = exp.get("description", "").strip()
        if amount > 0 and desc:
            valid_expenses.append(exp)
    
    if not valid_expenses:
        raise ValueError("No valid expenses to create.")

    _, _, spreadsheet = get_clients()
    items_ws = get_or_create_worksheet(spreadsheet, ITEMS_SHEET, ITEMS_HEADERS)

    income_means = compute_income_means()
    if not income_means:
        raise ValueError("No paycheck data found. Ask all users to update their paychecks first.")

    all_usernames = list(st.secrets["users"].keys())
    participants = [u for u in all_usernames if u in income_means]

    if share_type == "relative_all":
        # participants already = everyone with income
        pass
    elif share_type == "relative_others":
        participants = [u for u in participants if u != uploader]
    else:
        raise ValueError(f"Unknown share_type: {share_type}")

    if not participants:
        raise ValueError("No participants found for this share type (check paychecks).")

    denom = sum(income_means[u] for u in participants)
    if denom <= 0:
        raise ValueError("Participants must have positive average paychecks.")

    # Build all rows for batch append
    all_rows = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for idx, exp in enumerate(valid_expenses):
        total_amount = exp["amount"]
        description = exp["description"].strip()
        purchase_date = exp.get("date") or datetime.now().date()
        uploaded_file = exp.get("file") if idx == 0 else None  # Only first expense gets receipt

        purchase_id = str(uuid.uuid4())
        purchase_date_str = purchase_date.isoformat() if hasattr(purchase_date, 'isoformat') else str(purchase_date)

        receipt_url = upload_receipt_file(uploaded_file, purchase_id) if uploaded_file else None

        for debtor in participants:
            # Never create a row where someone "owes themselves"
            if debtor == uploader:
                continue

            share = float(total_amount) * float(income_means[debtor]) / float(denom)
            share = round(share, 2)

            row_id = str(uuid.uuid4())
            row = [
                row_id,
                purchase_id,
                now_iso,
                purchase_date_str,
                uploader,
                debtor,
                description,
                float(total_amount),
                share,
                share_type,
                receipt_url or "",
                False,  # paid
                "",     # paid_at
                "",     # paid_by
            ]
            all_rows.append(row)

    if not all_rows:
        raise ValueError(
            "No debt rows were created. This happens when there are no other users "
            "with paycheck data to share the expense with. Add more users' paychecks "
            "or use 'Only me (no sharing)' option."
        )

    # Use batch append - single API call for all rows!
    items_ws.append_rows(all_rows, value_input_option='USER_ENTERED')
    
    # Invalidate cache after modification
    invalidate_data_cache()
    
    return len(valid_expenses)


def delete_expense_debts(current_user: str, purchase_id: str) -> None:
    """Delete all debt rows associated with a purchase_id (only if current_user is the uploader)."""
    batch_delete_expense_debts(current_user, [purchase_id])


def batch_delete_expense_debts(current_user: str, purchase_ids: List[str]) -> int:
    """
    Delete all debt rows associated with multiple purchase_ids in a batch operation.
    Only deletes expenses where current_user is the uploader.
    
    Returns:
        Number of successfully deleted expenses
    """
    if not purchase_ids:
        return 0
        
    _, _, spreadsheet = get_clients()
    items_ws = get_or_create_worksheet(spreadsheet, ITEMS_SHEET, ITEMS_HEADERS)
    
    items_df = load_items_df()
    if items_df.empty:
        return 0
    
    # Get all rows matching any of the purchase_ids
    matching_rows = items_df[items_df["purchase_id"].isin(purchase_ids)]
    if matching_rows.empty:
        return 0
    
    # Filter to only rows where current_user is the uploader
    matching_rows = matching_rows[matching_rows["uploader"] == current_user]
    if matching_rows.empty:
        return 0
    
    # Collect info for email notifications (group by purchase_id)
    deleted_purchases = set()
    notifications_data = []
    for pid in matching_rows["purchase_id"].unique():
        pid_rows = matching_rows[matching_rows["purchase_id"] == pid]
        notifications_data.append({
            "description": str(pid_rows.iloc[0]["description"]),
            "total_amount": float(pid_rows.iloc[0]["amount_total"]),
            "affected_users": list(pid_rows["debtor"].unique())
        })
        deleted_purchases.add(pid)
    
    # Get all sheet row numbers (sorted in reverse to maintain indices when deleting)
    rows_to_delete = sorted([idx + 2 for idx in matching_rows.index], reverse=True)
    
    if rows_to_delete:
        # Use batch delete request via Sheets API for maximum efficiency
        # Group consecutive rows for batch deletion
        delete_requests = []
        i = 0
        while i < len(rows_to_delete):
            start_row = rows_to_delete[i]
            end_row = start_row
            
            # Find consecutive rows (remember we're going in reverse, so consecutive means -1)
            while i + 1 < len(rows_to_delete) and rows_to_delete[i + 1] == rows_to_delete[i] - 1:
                i += 1
                end_row = rows_to_delete[i]
            
            # end_row is the smaller number (earlier row), start_row is larger (later row)
            # Sheets API deleteDimension uses 0-indexed, exclusive end
            delete_requests.append({
                'deleteDimension': {
                    'range': {
                        'sheetId': items_ws.id,
                        'dimension': 'ROWS',
                        'startIndex': end_row - 1,  # Convert to 0-indexed
                        'endIndex': start_row  # Exclusive, so this is correct
                    }
                }
            })
            i += 1
        
        # Execute all deletes in a single batch request
        if delete_requests:
            spreadsheet.batch_update({'requests': delete_requests})
    
    # Invalidate cache after modification
    invalidate_data_cache()
    
    # Send email notifications
    for notif in notifications_data:
        if notif["affected_users"]:
            notify_expense_deleted(current_user, notif["description"], notif["total_amount"], notif["affected_users"])
    
    return len(deleted_purchases)

def mark_debts_as_paid(current_user: str, debt_ids: List[str]) -> None:
    if not debt_ids:
        return

    _, _, spreadsheet = get_clients()
    items_ws = get_or_create_worksheet(spreadsheet, ITEMS_SHEET, ITEMS_HEADERS)
    archive_ws = get_or_create_worksheet(spreadsheet, ARCHIVE_SHEET, ARCHIVE_HEADERS)

    items_df = load_items_df()
    if items_df.empty:
        return

    now_iso = datetime.now(timezone.utc).isoformat()

    # Collect all updates for batch processing
    items_batch_updates = []
    archive_rows = []
    notifications = []  # Store notification data for later

    for debt_id in debt_ids:
        if debt_id not in items_df["id"].values:
            continue

        row_idx_df = items_df.index[items_df["id"] == debt_id][0]
        # Only the debtor can mark their own debts as paid
        debtor_username = str(items_df.loc[row_idx_df, "debtor"])
        if debtor_username != current_user:
            continue

        sheet_row = row_idx_df + 2  # + header row

        # Collect updates for batch operation
        items_batch_updates.append({
            'range': f'{gspread.utils.rowcol_to_a1(sheet_row, ITEMS_COL_INDEX["paid"])}',
            'values': [[True]]
        })
        items_batch_updates.append({
            'range': f'{gspread.utils.rowcol_to_a1(sheet_row, ITEMS_COL_INDEX["paid_at"])}',
            'values': [[now_iso]]
        })
        items_batch_updates.append({
            'range': f'{gspread.utils.rowcol_to_a1(sheet_row, ITEMS_COL_INDEX["paid_by"])}',
            'values': [[current_user]]
        })

        # Prepare archive row
        row_dict = items_df.loc[row_idx_df].to_dict()
        row_dict["paid"] = True
        row_dict["paid_at"] = now_iso
        row_dict["paid_by"] = current_user
        row_dict["approved"] = False
        row_dict["approved_at"] = ""
        row_dict["approved_by"] = ""

        archive_row = [row_dict.get(col, "") for col in ARCHIVE_HEADERS]
        archive_rows.append(archive_row)
        
        # Store notification data
        notifications.append({
            "uploader": str(items_df.loc[row_idx_df, "uploader"]),
            "description": str(items_df.loc[row_idx_df, "description"]),
            "amount": float(items_df.loc[row_idx_df, "amount_owed"])
        })

    # Execute batch update for items sheet (single API call)
    if items_batch_updates:
        items_ws.batch_update(items_batch_updates, value_input_option='USER_ENTERED')

    # Execute batch append for archive sheet (single API call)
    if archive_rows:
        archive_ws.append_rows(archive_rows, value_input_option='USER_ENTERED')

    # Invalidate cache after modification
    invalidate_data_cache()

    # Send email notifications after successful updates
    for notif in notifications:
        notify_payment_marked(
            debtor=current_user,
            uploader=notif["uploader"],
            description=notif["description"],
            amount=notif["amount"]
        )


def approve_payments(current_user: str, archive_ids: List[str]) -> None:
    if not archive_ids:
        return

    _, _, spreadsheet = get_clients()
    archive_ws = get_or_create_worksheet(spreadsheet, ARCHIVE_SHEET, ARCHIVE_HEADERS)
    archive_df = load_archive_df()
    if archive_df.empty:
        return

    now_iso = datetime.now(timezone.utc).isoformat()

    # Collect all updates for batch processing
    batch_updates = []
    notifications = []

    for arc_id in archive_ids:
        if arc_id not in archive_df["id"].values:
            continue

        row_idx_df = archive_df.index[archive_df["id"] == arc_id][0]
        uploader = str(archive_df.loc[row_idx_df, "uploader"])

        # Only the uploader (who originally paid for the item) can approve
        if uploader != current_user:
            continue

        sheet_row = row_idx_df + 2
        
        # Collect updates for batch operation
        batch_updates.append({
            'range': f'{gspread.utils.rowcol_to_a1(sheet_row, ARCHIVE_COL_INDEX["approved"])}',
            'values': [[True]]
        })
        batch_updates.append({
            'range': f'{gspread.utils.rowcol_to_a1(sheet_row, ARCHIVE_COL_INDEX["approved_at"])}',
            'values': [[now_iso]]
        })
        batch_updates.append({
            'range': f'{gspread.utils.rowcol_to_a1(sheet_row, ARCHIVE_COL_INDEX["approved_by"])}',
            'values': [[current_user]]
        })
        
        # Store notification data
        notifications.append({
            "debtor": str(archive_df.loc[row_idx_df, "debtor"]),
            "description": str(archive_df.loc[row_idx_df, "description"]),
            "amount": float(archive_df.loc[row_idx_df, "amount_owed"])
        })

    # Execute batch update (single API call for all updates)
    if batch_updates:
        archive_ws.batch_update(batch_updates, value_input_option='USER_ENTERED')

    # Invalidate cache after modification
    invalidate_data_cache()

    # Send email notifications after successful updates
    for notif in notifications:
        notify_payment_approved(
            debtor=notif["debtor"],
            uploader=current_user,
            description=notif["description"],
            amount=notif["amount"]
        )


# -------------------------------------------------------------------
# AUTH & USER RESOLUTION
# -------------------------------------------------------------------

def require_login() -> str:
    """Ensure user is logged in and allowed. Returns username (email prefix)."""
    if not getattr(st.user, "is_logged_in", False):
        st.title("Household Expense Manager")
        st.info("Log in with Google to manage shared household expenses.")
        if st.button("Log in with Google"):
            st.login()  # uses [auth] settings in secrets.toml
        st.stop()

    email = getattr(st.user, "email", None)
    if not email:
        st.error("Logged in, but no email found in identity token.")
        st.stop()

    username = email.split("@")[0].lower()
    users_cfg = st.secrets.get("users", {})

    if username not in users_cfg:
        st.error(
            f"User '{username}' is not allowed to use this app.\n"
            "Ask the admin to add you under [users] in secrets.toml."
        )
        if st.button("Log out"):
            st.logout()
        st.stop()

    display_name = users_cfg[username]
    
    # Show personalized greeting in sidebar
    greeting = get_random_greeting(display_name)
    st.sidebar.markdown(f"### {greeting}")
    st.sidebar.markdown(f"**{display_name}**  \n`{email}`")
    st.sidebar.markdown("---")
    
    if st.sidebar.button("🚪 Log out"):
        st.logout()
        st.stop()

    return username


# -------------------------------------------------------------------
# UI PAGES
# -------------------------------------------------------------------

def page_dashboard(username: str):
    emoji = get_random_emoji(DASHBOARD_EMOJIS)
    st.header(f"{emoji} Dashboard")
    st.info(get_random_message(DASHBOARD_INTRO))

    income_means = compute_income_means()
    items_df = load_items_df()

    my_income = float(income_means.get(username, 0.0))

    if items_df.empty:
        my_debts = pd.DataFrame(columns=ITEMS_HEADERS)
        my_credits = pd.DataFrame(columns=ITEMS_HEADERS)
    else:
        # Handle various boolean representations from Google Sheets
        def is_unpaid(val):
            if pd.isna(val):
                return True
            if isinstance(val, bool):
                return not val
            if isinstance(val, str):
                return val.lower() not in ['true', '1', 'yes']
            return not bool(val)
        
        unpaid_mask = items_df["paid"].apply(is_unpaid)
        my_debts = items_df[
            (items_df["debtor"] == username) & unpaid_mask
        ]
        my_credits = items_df[
            (items_df["uploader"] == username) & unpaid_mask
        ]

    total_owe = float(my_debts["amount_owed"].sum()) if not my_debts.empty else 0.0
    total_owed_to_me = float(my_credits["amount_owed"].sum()) if not my_credits.empty else 0.0

    col1, col2, col3 = st.columns(3)
    col1.metric("My average income", f"{my_income:,.2f}")
    col2.metric("I owe others", f"{total_owe:,.2f}")
    col3.metric("Others owe me", f"{total_owed_to_me:,.2f}")

    st.subheader("💸 Debts I owe")

    if my_debts.empty:
        st.success(get_random_message(NO_DEBT_MESSAGES))
    else:
        st.dataframe(
            my_debts[["id", "uploader", "description", "amount_owed", "purchase_date", "timestamp"]],
            use_container_width=True,
        )

        with st.form("pay_debts_form"):
            labels = []
            values = []
            for _, row in my_debts.iterrows():
                label = (
                    f"{row['description']} — you owe {row['amount_owed']:.2f} "
                    f"to {row['uploader']} (id: {row['id']})"
                )
                labels.append(label)
                values.append(row["id"])

            selected_labels = st.multiselect(
                "Select items you are paying now:",
                labels,
            )
            selected_ids = [
                values[labels.index(lbl)] for lbl in selected_labels
            ] if selected_labels else []

            total_selected = float(
                my_debts[my_debts["id"].isin(selected_ids)]["amount_owed"].sum()
            ) if selected_ids else 0.0

            st.markdown(f"**Total to pay now:** {total_selected:,.2f}")
            submitted = st.form_submit_button("Mark selected as paid")

            if submitted:
                if not selected_ids:
                    st.warning("Select at least one item.")
                else:
                    mark_debts_as_paid(username, selected_ids)
                    st.success(get_random_message(PAYMENT_MARKED_MESSAGES))
                    st.info("Copied to archive and pending approval from the uploader.")
                    st.rerun()

    st.subheader("💰 Debts others owe me")

    if my_credits.empty:
        st.info(get_random_message(NO_CREDITS_MESSAGES))
    else:
        st.dataframe(
            my_credits[["id", "debtor", "description", "amount_owed", "purchase_date", "timestamp"]],
            use_container_width=True,
        )
    
    # Section to delete expenses
    st.subheader("🗑️ Manage My Expenses")
    
    # Get all unique expenses uploaded by this user (group by purchase_id)
    my_expenses = items_df[items_df["uploader"] == username] if not items_df.empty else pd.DataFrame()
    
    if my_expenses.empty:
        st.info(get_random_message(NO_EXPENSES_MESSAGES))
    else:
        # Group by purchase_id to show unique expenses
        expense_groups = my_expenses.groupby("purchase_id").agg({
            "description": "first",
            "amount_total": "first",
            "purchase_date": "first",
            "timestamp": "first",
            "debtor": lambda x: list(x)
        }).reset_index()
        
        # Bulk delete option
        with st.expander("🗑️ Bulk Delete Expenses", expanded=False):
            st.markdown("*Select multiple expenses to delete at once*")
            
            # Create checkboxes for each expense
            selected_for_deletion = []
            for _, expense in expense_groups.iterrows():
                debtors = ", ".join(expense["debtor"])
                label = f"**{expense['description']}** - ${expense['amount_total']:,.2f} (Shared with: {debtors})"
                if st.checkbox(label, key=f"bulk_select_{expense['purchase_id']}"):
                    selected_for_deletion.append(expense['purchase_id'])
            
            if selected_for_deletion:
                col1, col2 = st.columns([1, 1])
                with col1:
                    st.metric("📊 Selected", len(selected_for_deletion))
                with col2:
                    selected_total = expense_groups[expense_groups['purchase_id'].isin(selected_for_deletion)]['amount_total'].sum()
                    st.metric("💰 Total Amount", f"${selected_total:,.2f}")
                
                st.warning(f"⚠️ You are about to delete {len(selected_for_deletion)} expense(s). This action cannot be undone!")
                
                if st.button("🗑️ Delete Selected Expenses", type="primary", use_container_width=True):
                    try:
                        # Use batch delete - single API call for all deletions!
                        deleted_count = batch_delete_expense_debts(username, selected_for_deletion)
                        st.success(f"{get_random_message(DELETE_SUCCESS)} Deleted {deleted_count} expense(s)!")
                    except Exception as e:
                        st.error(f"Error deleting expenses: {str(e)}")
                    
                    st.rerun()
        
        st.markdown("---")
        
        # Individual delete options
        st.write("**Your expenses:**")
        for _, expense in expense_groups.iterrows():
            debtors = ", ".join(expense["debtor"])
            col1, col2 = st.columns([4, 1])
            with col1:
                st.write(f"**{expense['description']}** - ${expense['amount_total']:,.2f} (Shared with: {debtors})")
                st.caption(f"Created: {expense['timestamp']} | Purchase date: {expense['purchase_date']}")
            with col2:
                if st.button("🗑️", key=f"delete_{expense['purchase_id']}", help="Delete this expense"):
                    try:
                        delete_expense_debts(username, expense["purchase_id"])
                        st.success(f"{get_random_message(DELETE_SUCCESS)} ({expense['description']})")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error deleting expense: {str(e)}")
            st.divider()


def page_paychecks(username: str):
    emoji = get_random_emoji(PAYCHECK_EMOJIS)
    st.header(f"{emoji} My Paychecks")
    st.info(get_random_message(PAYCHECK_INTRO))

    df = load_paychecks_df()
    row = None
    if not df.empty and username in df["username"].values:
        row = df[df["username"] == username].iloc[0]

    def _default(val):
        try:
            return float(val)
        except Exception:
            return 0.0

    default_p1 = _default(row["pay1"]) if row is not None else 0.0
    default_p2 = _default(row["pay2"]) if row is not None else 0.0
    default_p3 = _default(row["pay3"]) if row is not None else 0.0

    with st.form("paychecks_form"):
        p1 = st.number_input(
            "Most recent paycheck",
            min_value=0.0,
            value=default_p1,
            step=100.0,
        )
        p2 = st.number_input(
            "Previous paycheck",
            min_value=0.0,
            value=default_p2,
            step=100.0,
        )
        p3 = st.number_input(
            "Oldest of the last 3 paychecks",
            min_value=0.0,
            value=default_p3,
            step=100.0,
        )
        submitted = st.form_submit_button("Save paychecks")

        if submitted:
            upsert_paychecks(username, p1, p2, p3)
            st.success("Paychecks saved.")
            st.rerun()

    income_means = compute_income_means()
    my_income = float(income_means.get(username, 0.0))
    st.markdown(f"**Current average used for sharing:** {my_income:,.2f}")


def page_add_expense(username: str):
    emoji = get_random_emoji(EXPENSE_EMOJIS)
    st.header(f"{emoji} Add New Expenses")
    st.info(get_random_message(EXPENSE_INTRO))
    
    # Initialize session state for expenses
    if "expenses" not in st.session_state:
        st.session_state.expenses = [{"description": "", "amount": 0.0, "date": datetime.now().date()}]
    
    # Shared settings for all expenses
    st.subheader("⚙️ Shared Settings")
    col1, col2 = st.columns(2)
    with col1:
        share_label = st.radio(
            "How should expenses be shared?",
            list(SHARE_TYPE_OPTIONS.keys()),
            index=1,
            help="This setting applies to all expenses below"
        )
    with col2:
        receipt_file = st.file_uploader(
            "Optional receipt (image/PDF)",
            type=["png", "jpg", "jpeg", "pdf"],
            help="Applies to the first expense only"
        )
    
    st.divider()
    
    # Dynamic expense entries
    st.subheader("📝 Expenses")
    
    expenses_to_remove = []
    for idx, expense in enumerate(st.session_state.expenses):
        col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
        
        with col1:
            expense["description"] = st.text_input(
                f"Description #{idx+1}",
                value=expense["description"],
                key=f"desc_{idx}",
                placeholder="e.g., Groceries, Utilities, etc."
            )
        
        with col2:
            expense["amount"] = st.number_input(
                f"Amount #{idx+1}",
                min_value=0.0,
                value=expense["amount"],
                step=1.0,
                key=f"amount_{idx}"
            )
        
        with col3:
            expense["date"] = st.date_input(
                f"Date #{idx+1}",
                value=expense["date"],
                key=f"date_{idx}"
            )
        
        with col4:
            st.write("")
            st.write("")
            if len(st.session_state.expenses) > 1:
                if st.button("🗑️", key=f"remove_{idx}", help="Remove this expense"):
                    expenses_to_remove.append(idx)
    
    # Remove expenses marked for deletion
    for idx in sorted(expenses_to_remove, reverse=True):
        st.session_state.expenses.pop(idx)
        st.rerun()
    
    # Add more expense button
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        if st.button("➕ Add Another Expense", use_container_width=True):
            st.session_state.expenses.append({"description": "", "amount": 0.0, "date": datetime.now().date()})
            st.rerun()
    
    with col2:
        if st.button("🔄 Clear All", use_container_width=True):
            st.session_state.expenses = [{"description": "", "amount": 0.0, "date": datetime.now().date()}]
            st.rerun()
    
    st.divider()
    
    # Summary
    valid_expenses = [e for e in st.session_state.expenses if e["description"].strip() and e["amount"] > 0]
    total_amount = sum(e["amount"] for e in valid_expenses)
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("📊 Total Expenses", len(valid_expenses))
    with col2:
        st.metric("💰 Total Amount", f"${total_amount:,.2f}")
    
    # Create debts button
    if st.button("✅ Create All Debts", type="primary", use_container_width=True):
        if not valid_expenses:
            st.error("Please add at least one expense with a description and positive amount.")
        else:
            share_type = SHARE_TYPE_OPTIONS[share_label]
            
            try:
                # Prepare expenses for batch processing
                expenses_for_batch = []
                for idx, expense in enumerate(valid_expenses):
                    expenses_for_batch.append({
                        "description": expense["description"].strip(),
                        "amount": expense["amount"],
                        "date": expense["date"],
                        "file": receipt_file if idx == 0 else None  # Only first expense gets receipt
                    })
                
                # Single batch call - much more efficient!
                success_count = batch_add_expenses_and_create_debts(
                    uploader=username,
                    expenses=expenses_for_batch,
                    share_type=share_type,
                )
                
                # Calculate affected users for email notification
                income_means = compute_income_means()
                all_usernames = list(st.secrets["users"].keys())
                participants = [u for u in all_usernames if u in income_means]
                
                if share_type == "relative_others":
                    participants = [u for u in participants if u != username]
                
                all_affected_users = [p for p in participants if p != username]
                
                # Build expense summaries for email
                expense_summaries = [
                    {"description": e["description"], "amount": e["amount"]}
                    for e in expenses_for_batch
                ]
                
                # Send email notification
                if all_affected_users:
                    if len(expense_summaries) > 1:
                        # Send consolidated email for multiple expenses
                        notify_multiple_expenses(username, expense_summaries, all_affected_users)
                    elif len(expense_summaries) == 1:
                        # Send single expense email
                        notify_new_expense(
                            username,
                            expense_summaries[0]["description"],
                            expense_summaries[0]["amount"],
                            all_affected_users
                        )
                
                st.success(f"{get_random_message(EXPENSE_SUCCESS)} Created {success_count} expense(s)!")
                
                # Reset form
                st.session_state.expenses = [{"description": "", "amount": 0.0, "date": datetime.now().date()}]
                st.rerun()
                
            except Exception as e:
                st.error(f"Error creating expenses: {str(e)}")
                import traceback
                st.code(traceback.format_exc(), language="python")


def page_history(username: str):
    emoji = get_random_emoji(HISTORY_EMOJIS)
    st.header(f"{emoji} Transaction History")
    st.info(get_random_message(HISTORY_INTRO))
    
    items_df = load_items_df()
    archive_df = load_archive_df()
    
    # Combine items and archive
    if not items_df.empty and not archive_df.empty:
        # Add source column
        items_df["source"] = "Active"
        archive_df["source"] = "Archive"
        # Combine, using only columns that exist in items
        combined_df = pd.concat([items_df, archive_df[ITEMS_HEADERS + ["source"]]], ignore_index=True)
    elif not items_df.empty:
        items_df["source"] = "Active"
        combined_df = items_df
    elif not archive_df.empty:
        archive_df["source"] = "Archive"
        combined_df = archive_df[ITEMS_HEADERS + ["source"]]
    else:
        st.info(get_random_message(NO_HISTORY_MESSAGES))
        return
    
    # Filter options
    col1, col2 = st.columns(2)
    with col1:
        filter_type = st.selectbox(
            "Filter by",
            ["All", "I uploaded", "I owe", "Others owe me"]
        )
    with col2:
        show_paid = st.checkbox("Include paid items", value=True)
    
    # Apply filters
    filtered_df = combined_df.copy()
    
    if filter_type == "I uploaded":
        filtered_df = filtered_df[filtered_df["uploader"] == username]
    elif filter_type == "I owe":
        filtered_df = filtered_df[filtered_df["debtor"] == username]
    elif filter_type == "Others owe me":
        filtered_df = filtered_df[
            (filtered_df["uploader"] == username) & 
            (filtered_df["debtor"] != username)
        ]
    
    if not show_paid:
        def is_unpaid(val):
            if pd.isna(val):
                return True
            if isinstance(val, bool):
                return not val
            if isinstance(val, str):
                return val.lower() not in ['true', '1', 'yes']
            return not bool(val)
        filtered_df = filtered_df[filtered_df["paid"].apply(is_unpaid)]
    
    # Display
    if filtered_df.empty:
        st.info("No items match the selected filters. Try adjusting your filters! 🔍")
    else:
        display_cols = [
            "source", "timestamp", "purchase_date", "uploader", "debtor",
            "description", "amount_total", "amount_owed", "paid", "share_type"
        ]
        st.dataframe(
            filtered_df[display_cols],
            use_container_width=True,
        )
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("📊 Total Items", len(filtered_df))
        with col2:
            total_amount = filtered_df["amount_owed"].sum()
            st.metric("💵 Total Amount", f"{total_amount:,.2f}")


def page_approve(username: str):
    emoji = get_random_emoji(APPROVE_EMOJIS)
    st.header(f"{emoji} Approve Payments")
    st.info(get_random_message(APPROVAL_INTRO))

    archive_df = load_archive_df()
    if archive_df.empty:
        st.success(get_random_message(NO_APPROVALS_MESSAGES))
        return
    
    # Handle boolean values properly
    def is_approved(val):
        if pd.isna(val):
            return False
        if isinstance(val, bool):
            return val
        if isinstance(val, str):
            return val.lower() in ['true', '1', 'yes']
        return bool(val)
    
    def is_paid(val):
        if pd.isna(val):
            return False
        if isinstance(val, bool):
            return val
        if isinstance(val, str):
            return val.lower() in ['true', '1', 'yes']
        return bool(val)

    pending = archive_df[
        (archive_df["uploader"] == username)
        & (~archive_df["approved"].apply(is_approved))
        & (archive_df["paid"].apply(is_paid))
    ]

    if pending.empty:
        st.success(get_random_message(NO_APPROVALS_MESSAGES))
        return

    st.info("💡 These payments were marked as paid by other users. Review and approve them below:")

    st.dataframe(
        pending[
            [
                "id",
                "debtor",
                "description",
                "amount_owed",
                "paid_by",
                "paid_at",
                "purchase_date",
            ]
        ],
        use_container_width=True,
    )

    with st.form("approve_payments_form"):
        labels = []
        values = []
        for _, row in pending.iterrows():
            label = (
                f"{row['debtor']} / {row['paid_by']} paid {row['amount_owed']:.2f} "
                f"for '{row['description']}' (id: {row['id']})"
            )
            labels.append(label)
            values.append(row["id"])

        selected_labels = st.multiselect(
            "Select payments to approve:",
            labels,
        )
        selected_ids = [
            values[labels.index(lbl)] for lbl in selected_labels
        ] if selected_labels else []

        submitted = st.form_submit_button("Approve selected")

        if submitted:
            if not selected_ids:
                st.warning("Select at least one row to approve.")
                return

            approve_payments(username, selected_ids)
            st.success(get_random_message(APPROVAL_SUCCESS))
            st.rerun()


# -------------------------------------------------------------------
# MAIN APP
# -------------------------------------------------------------------

def main():
    username = require_login()

    st.sidebar.header("Navigation")
    page = st.sidebar.radio(
        "Go to",
        ["Dashboard", "Update paychecks", "Add expense", "Approve payments", "History"],
    )
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("💡 **Tip:** You can delete expenses you created from the Dashboard!")

    if page == "Dashboard":
        page_dashboard(username)
    elif page == "Update paychecks":
        page_paychecks(username)
    elif page == "Add expense":
        page_add_expense(username)
    elif page == "Approve payments":
        page_approve(username)
    elif page == "History":
        page_history(username)


if __name__ == "__main__":
    main()
