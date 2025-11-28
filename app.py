import io
import uuid
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

    first_row = ws.row_values(1)
    if not first_row:
        ws.append_row(headers)
    return ws


# -------------------------------------------------------------------
# DATA ACCESS HELPERS
# -------------------------------------------------------------------

def load_paychecks_df() -> pd.DataFrame:
    _, _, spreadsheet = get_clients()
    ws = get_or_create_worksheet(spreadsheet, PAYCHECKS_SHEET, PAYCHECKS_HEADERS)
    records = ws.get_all_records()
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


def compute_income_means() -> Dict[str, float]:
    """Return {username: mean_of_last_3_paychecks}."""
    df = load_paychecks_df()
    if df.empty:
        return {}

    for col in ["pay1", "pay2", "pay3"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["average"] = df[["pay1", "pay2", "pay3"]].mean(axis=1)

    df = df.dropna(subset=["username", "average"])
    return dict(zip(df["username"], df["average"]))


def load_items_df() -> pd.DataFrame:
    _, _, spreadsheet = get_clients()
    ws = get_or_create_worksheet(spreadsheet, ITEMS_SHEET, ITEMS_HEADERS)
    records = ws.get_all_records()
    if not records:
        return pd.DataFrame(columns=ITEMS_HEADERS)

    df = pd.DataFrame(records)
    for col in ITEMS_HEADERS:
        if col not in df.columns:
            df[col] = None

    if not df.empty:
        for col in ["amount_total", "amount_owed"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["paid"] = df["paid"].astype(bool)

    return df[ITEMS_HEADERS]


def load_archive_df() -> pd.DataFrame:
    _, _, spreadsheet = get_clients()
    ws = get_or_create_worksheet(spreadsheet, ARCHIVE_SHEET, ARCHIVE_HEADERS)
    records = ws.get_all_records()
    if not records:
        return pd.DataFrame(columns=ARCHIVE_HEADERS)

    df = pd.DataFrame(records)
    for col in ARCHIVE_HEADERS:
        if col not in df.columns:
            df[col] = None

    if not df.empty:
        df["paid"] = df["paid"].astype(bool)
        df["approved"] = df["approved"].astype(bool)
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
    if total_amount <= 0:
        raise ValueError("Amount must be positive.")

    _, _, spreadsheet = get_clients()
    items_ws = get_or_create_worksheet(spreadsheet, ITEMS_SHEET, ITEMS_HEADERS)

    income_means = compute_income_means()
    if not income_means:
        raise ValueError("No paycheck data found. Ask all users to update their paychecks first.")

    all_usernames = list(st.secrets["users"].keys())
    participants = [u for u in all_usernames if u in income_means]

    if share_type == "self":
        # Only uploader – nothing to record for debts.
        return
    elif share_type == "relative_all":
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

    purchase_id = str(uuid.uuid4())
    now_iso = datetime.now(timezone.utc).isoformat()
    purchase_date_str = (purchase_date or datetime.now().date()).isoformat()

    receipt_url = upload_receipt_file(uploaded_file, purchase_id)

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
        items_ws.append_row(row)


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

    for debt_id in debt_ids:
        if debt_id not in items_df["id"].values:
            continue

        row_idx_df = items_df.index[items_df["id"] == debt_id][0]
        # Only the debtor can mark their own debts as paid
        debtor_username = str(items_df.loc[row_idx_df, "debtor"])
        if debtor_username != current_user:
            continue

        sheet_row = row_idx_df + 2  # + header row

        # Update items sheet
        items_ws.update_cell(sheet_row, ITEMS_COL_INDEX["paid"], True)
        items_ws.update_cell(sheet_row, ITEMS_COL_INDEX["paid_at"], now_iso)
        items_ws.update_cell(sheet_row, ITEMS_COL_INDEX["paid_by"], current_user)

        # Copy to archive with pending approval
        row_dict = items_df.loc[row_idx_df].to_dict()
        row_dict["paid"] = True
        row_dict["paid_at"] = now_iso
        row_dict["paid_by"] = current_user
        row_dict["approved"] = False
        row_dict["approved_at"] = ""
        row_dict["approved_by"] = ""

        archive_row = [row_dict.get(col, "") for col in ARCHIVE_HEADERS]
        archive_ws.append_row(archive_row)


def approve_payments(current_user: str, archive_ids: List[str]) -> None:
    if not archive_ids:
        return

    _, _, spreadsheet = get_clients()
    archive_ws = get_or_create_worksheet(spreadsheet, ARCHIVE_SHEET, ARCHIVE_HEADERS)
    archive_df = load_archive_df()
    if archive_df.empty:
        return

    now_iso = datetime.now(timezone.utc).isoformat()

    for arc_id in archive_ids:
        if arc_id not in archive_df["id"].values:
            continue

        row_idx_df = archive_df.index[archive_df["id"] == arc_id][0]
        uploader = str(archive_df.loc[row_idx_df, "uploader"])

        # Only the uploader (who originally paid for the item) can approve
        if uploader != current_user:
            continue

        sheet_row = row_idx_df + 2
        archive_ws.update_cell(sheet_row, ARCHIVE_COL_INDEX["approved"], True)
        archive_ws.update_cell(sheet_row, ARCHIVE_COL_INDEX["approved_at"], now_iso)
        archive_ws.update_cell(sheet_row, ARCHIVE_COL_INDEX["approved_by"], current_user)


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
    st.sidebar.markdown(f"**Logged in as** {display_name}  \n`{email}`")
    if st.sidebar.button("Log out"):
        st.logout()
        st.stop()

    return username


# -------------------------------------------------------------------
# UI PAGES
# -------------------------------------------------------------------

def page_dashboard(username: str):
    st.header("Dashboard")

    income_means = compute_income_means()
    items_df = load_items_df()

    my_income = float(income_means.get(username, 0.0))

    if items_df.empty:
        my_debts = pd.DataFrame(columns=ITEMS_HEADERS)
        my_credits = pd.DataFrame(columns=ITEMS_HEADERS)
    else:
        unpaid_mask = ~items_df["paid"].astype(bool)
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

    st.subheader("Debts I owe")

    if my_debts.empty:
        st.write("You don't currently owe anything 🎉")
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
                    st.success(
                        "Marked as paid and copied to the archive "
                        "(pending approval from the uploader)."
                    )
                    st.rerun()

    st.subheader("Debts others owe me")

    if my_credits.empty:
        st.write("No outstanding debts from others.")
    else:
        st.dataframe(
            my_credits[["id", "debtor", "description", "amount_owed", "purchase_date", "timestamp"]],
            use_container_width=True,
        )


def page_paychecks(username: str):
    st.header("My paychecks")

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
    st.header("Add a new expense")

    description = st.text_input("Description", "")
    total_amount = st.number_input(
        "Total amount",
        min_value=0.0,
        step=1.0,
    )
    purchase_date = st.date_input(
        "Purchase date",
        value=datetime.now().date(),
    )
    share_label = st.radio(
        "How should this expense be shared?",
        list(SHARE_TYPE_OPTIONS.keys()),
        index=1,
    )
    receipt_file = st.file_uploader(
        "Optional receipt (image/PDF)",
        type=["png", "jpg", "jpeg", "pdf"],
    )

    if st.button("Create debts"):
        if not description.strip():
            st.error("Please enter a description.")
        elif total_amount <= 0:
            st.error("Amount must be positive.")
        else:
            share_type = SHARE_TYPE_OPTIONS[share_label]
            try:
                add_expense_and_create_debts(
                    uploader=username,
                    description=description.strip(),
                    total_amount=total_amount,
                    share_type=share_type,
                    purchase_date=purchase_date,
                    uploaded_file=receipt_file,
                )
                st.success("Expense added and debts created.")
                st.rerun()
            except ValueError as e:
                st.error(str(e))


def page_approve(username: str):
    st.header("Approve payments for expenses you uploaded")

    archive_df = load_archive_df()
    if archive_df.empty:
        st.write("No payments waiting for approval.")
        return

    pending = archive_df[
        (archive_df["uploader"] == username)
        & (~archive_df["approved"].astype(bool))
        & (archive_df["paid"].astype(bool))
    ]

    if pending.empty:
        st.write("No payments waiting for your approval.")
        return

    st.write("These payments were marked as paid by other users. You can approve them:")

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
            st.success("Selected payments approved.")
            st.rerun()


# -------------------------------------------------------------------
# MAIN APP
# -------------------------------------------------------------------

def main():
    username = require_login()

    st.sidebar.header("Navigation")
    page = st.sidebar.radio(
        "Go to",
        ["Dashboard", "Update paychecks", "Add expense", "Approve payments"],
    )

    if page == "Dashboard":
        page_dashboard(username)
    elif page == "Update paychecks":
        page_paychecks(username)
    elif page == "Add expense":
        page_add_expense(username)
    elif page == "Approve payments":
        page_approve(username)


if __name__ == "__main__":
    main()
