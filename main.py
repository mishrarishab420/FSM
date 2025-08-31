import streamlit as st
import pandas as pd
import zipfile
import io
import re
from datetime import datetime
import time
from google.cloud import bigquery
import json

# ------------- Config -------------
st.set_page_config(
    page_title="Food Safety Management System", 
    page_icon="📊", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- BigQuery Integration ---
try:
    gcp_secret_str = st.secrets.get("GCP_SERVICE_ACCOUNT", None)
    if not gcp_secret_str:
        st.error("🚨 GCP_SERVICE_ACCOUNT secret not found. Please add it in Streamlit secrets.")
        st.stop()
    service_account_info = json.loads(gcp_secret_str)
    client = bigquery.Client.from_service_account_info(service_account_info)
except json.JSONDecodeError:
    st.error("🚨 GCP_SERVICE_ACCOUNT secret is not valid JSON. Check formatting and \\n in private_key.")
    st.stop()
except Exception as e:
    st.error(f"🚨 Error initializing BigQuery client: {e}")
    st.stop()

PROJECT_ID = st.secrets.get("BIGQUERY_PROJECT", "")
DATASET_ID = st.secrets.get("BIGQUERY_DATASET", "")
STATE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.state_licence"
REG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.registration"

# ------------- Schemas -------------
STATE_COLS = {
    "FBO NAME": "text", "ADDRESS": "text", "DISTT": "text", "STATE": "text", 
    "KOB": "text", "CONTACT": "text", "RESPONSIBLE MO": "text", "Y": "text", 
    "REF ID": "text", "AMOUNT": "numeric", "LICENSE": "text", "COMPLIANCE MO": "text", 
    "EXPIRY": "date", "source_filename": "text", "ingestion_timestamp": "datetime"
}

REG_COLS = {
    "refId": "text", "certificateNo": "text", "companyName": "text", 
    "addressPremises": "text", "premiseVillageName": "text", 
    "correspondenceDistrictName": "text", "stateName": "text", 
    "contactMobile": "text", "contactPerson": "text", "displayRefId": "text", 
    "kobNameDetails": "text", "productName": "text", "expiryDate": "date", 
    "issuedDate": "date", "talukName": "text", "pincodePremises": "text", 
    "applicantMobileNo": "text", "noOfYears": "numeric", "statusId": "text", 
    "appType": "text", "amount": "numeric", "source_filename": "text", 
    "ingestion_timestamp": "datetime"
}

# ------------- Authentication -------------
def authenticate(username, password):
    return username == "admin" and password == "admin123"

def login_page():
    st.markdown("""
    <div style='text-align: center; padding: 2rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 10px; color: white;'>
        <h1>🍽️ Food Safety Management System</h1>
        <p>Compliance Tracking & License Management</p>
    </div>
    """, unsafe_allow_html=True)
    
    with st.form("login", clear_on_submit=True):
        col1, col2, col3 = st.columns([1,2,1])
        with col2:
            st.subheader("🔐 Administrator Login")
            u = st.text_input("Username", placeholder="Enter your username")
            p = st.text_input("Password", type="password", placeholder="Enter your password")
            submitted = st.form_submit_button("Login", use_container_width=True)
            
            if submitted:
                if authenticate(u, p):
                    st.session_state.authenticated = True
                    st.session_state.username = u
                    st.session_state.login_time = datetime.now()
                    st.success("Login successful! Redirecting...")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Invalid credentials. Please try again.")

def logout():
    st.session_state.authenticated = False
    st.session_state.username = None
    st.session_state.login_time = None
    st.rerun()

# ------------- Helpers -------------
def smart_column_mapping(df_columns, expected_columns):
    mapping = {}
    df_cols_lower = [col.strip().lower() for col in df_columns]
    
    for expected_col in expected_columns:
        expected_lower = expected_col.strip().lower()
        if expected_col in df_columns:
            mapping[expected_col] = expected_col
            continue
        if expected_lower in df_cols_lower:
            idx = df_cols_lower.index(expected_lower)
            mapping[expected_col] = df_columns[idx]
            continue
        variations = [
            expected_lower,
            expected_lower.replace(" ", "_"),
            expected_lower.replace("_", " "),
            re.sub(r'[^a-z0-9]', '', expected_lower),
            expected_lower.replace("name", "").strip(),
            expected_lower.replace("no", "number").replace("num", "number"),
        ]
        for var in variations:
            if var in df_cols_lower:
                idx = df_cols_lower.index(var)
                mapping[expected_col] = df_columns[idx]
                break
        else:
            mapping[expected_col] = None
    return mapping

def ensure_columns(df: pd.DataFrame, expected_cols):
    mapping = smart_column_mapping(list(df.columns), list(expected_cols.keys()))
    df2 = pd.DataFrame()
    for target_col in expected_cols.keys():
        source_col = mapping.get(target_col)
        if source_col and source_col in df.columns:
            if expected_cols[target_col] == "numeric":
                df2[target_col] = pd.to_numeric(df[source_col], errors='coerce')
            elif expected_cols[target_col] in ["date", "datetime"]:
                df2[target_col] = pd.to_datetime(df[source_col], errors='coerce')
            else:
                df2[target_col] = df[source_col].astype(str).replace({"nan": None, "None": None})
        else:
            df2[target_col] = None
    return df2

def insert_df_to_table(df: pd.DataFrame, table_id: str):
    df_fixed = ensure_columns(df, STATE_COLS if "state" in table_id else REG_COLS)
    df_fixed["source_filename"] = df_fixed.get("source_filename", "manual_upload")
    df_fixed["ingestion_timestamp"] = datetime.utcnow()
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
    job = client.load_table_from_dataframe(df_fixed, table_id, job_config=job_config)
    job.result()
    return len(df_fixed)

def get_table_stats(table_id):
    try:
        query = f"SELECT COUNT(*) as count, MAX(ingestion_timestamp) as latest FROM `{table_id}`"
        df = client.query(query).to_dataframe()
        return df.iloc[0]["count"], df.iloc[0]["latest"]
    except Exception:
        return 0, None

# ------------- File Processing -------------
def process_uploaded_files(files, table_name):
    total_rows = 0
    successful_files = 0
    table_id = STATE_TABLE if table_name == "state_licence" else REG_TABLE
    progress_bar = st.progress(0)
    status_text = st.empty()
    for i, f in enumerate(files):
        try:
            status_text.text(f"Processing {f.name}...")
            progress_bar.progress((i) / len(files))
            if f.name.lower().endswith(".csv"):
                df = pd.read_csv(f, encoding='utf-8', on_bad_lines='skip')
            else:
                df = pd.read_excel(f, engine="openpyxl")
            rows = insert_df_to_table(df, table_id)
            total_rows += rows
            successful_files += 1
            st.success(f"✅ {f.name}: Inserted {rows} rows")
        except Exception as e:
            st.error(f"❌ Error processing {f.name}: {str(e)}")
    progress_bar.progress(1.0)
    status_text.empty()
    if successful_files > 0:
        st.balloons()
    return total_rows, successful_files

def process_zip_file(uploaded_zip, table_name):
    total = 0
    successful_files = 0
    table_id = STATE_TABLE if table_name == "state_licence" else REG_TABLE
    try:
        with zipfile.ZipFile(uploaded_zip, "r") as z:
            namelist = [f for f in z.namelist() if f.lower().endswith((".csv", ".xlsx", ".xls"))]
            progress_bar = st.progress(0)
            status_text = st.empty()
            for i, fname in enumerate(namelist):
                status_text.text(f"Processing {fname}...")
                progress_bar.progress((i) / len(namelist))
                with z.open(fname) as fh:
                    data = fh.read()
                    if fname.lower().endswith(".csv"):
                        df = pd.read_csv(io.BytesIO(data), encoding='utf-8', on_bad_lines='skip')
                    else:
                        df = pd.read_excel(io.BytesIO(data), engine="openpyxl")
                    if df is not None:
                        rows = insert_df_to_table(df, table_id)
                        total += rows
                        successful_files += 1
                        st.success(f"✅ {fname}: Inserted {rows} rows")
            progress_bar.progress(1.0)
            status_text.empty()
    except Exception as e:
        st.error(f"❌ ZIP processing error: {str(e)}")
    if successful_files > 0:
        st.balloons()
    return total, successful_files

# ------------- Pages -------------
def data_upload_page():
    st.header("📤 Data Upload Center")
    col1, col2 = st.columns(2)
    with col1:
        state_count, state_latest = get_table_stats(STATE_TABLE)
        st.metric("State Licence Records", state_count, f"Last update: {str(state_latest)[:10] if state_latest else 'Never'}")
    with col2:
        reg_count, reg_latest = get_table_stats(REG_TABLE)
        st.metric("Registration Records", reg_count, f"Last update: {str(reg_latest)[:10] if reg_latest else 'Never'}")
    
    st.markdown("---")
    
    tab1, tab2 = st.tabs(["📋 State Licence Data", "📝 Registration Data"])
    with tab1:
        st.subheader("State Licence Data Upload")
        col1, col2 = st.columns(2)
        with col1:
            zip1 = st.file_uploader("ZIP Archive", type=["zip"], key="zip_state")
            if zip1 and st.button("Process ZIP", key="btn_zip_state"):
                process_zip_file(zip1, "state_licence")
        with col2:
            files1 = st.file_uploader("Individual Files", type=["csv", "xlsx", "xls"], accept_multiple_files=True, key="files_state")
            if files1 and st.button("Process Files", key="btn_files_state"):
                process_uploaded_files(files1, "state_licence")
    with tab2:
        st.subheader("Registration Data Upload")
        col1, col2 = st.columns(2)
        with col1:
            zip2 = st.file_uploader("ZIP Archive", type=["zip"], key="zip_reg")
            if zip2 and st.button("Process ZIP", key="btn_zip_reg"):
                process_zip_file(zip2, "registration")
        with col2:
            files2 = st.file_uploader("Individual Files", type=["csv", "xlsx", "xls"], accept_multiple_files=True, key="files_reg")
            if files2 and st.button("Process Files", key="btn_files_reg"):
                process_uploaded_files(files2, "registration")

def search_page():
    st.header("🔍 Advanced Data Search")
    segment = st.radio("Select Data Segment", ["📋 State Licence", "📝 Registration"], horizontal=True)
    table = "state_licence" if "State Licence" in segment else "registration"
    table_id = STATE_TABLE if table == "state_licence" else REG_TABLE
    pk = ["REF ID", "LICENSE"] if table == "state_licence" else ["refId", "certificateNo"]
    sample = client.query(f"SELECT * FROM `{table_id}` LIMIT 1000").to_dataframe()
    if sample.empty:
        st.info("No data available. Please upload data first.")
        return
    st.success(f"💡 Searching {segment} data. Primary keys: {pk[0]} & {pk[1]}")
    col1, col2 = st.columns([2,1])
    with col1:
        term1 = st.text_input(f"Search by {pk[0]}")
        term2 = st.text_input(f"Search by {pk[1]}")
    with col2:
        show_expired = st.checkbox("Show expired only")
        show_recent = st.checkbox("Show recent uploads (last 7 days)")
    if st.button("Execute Search"):
        try:
            where = []
            if term1: where.append(f"CAST(`{pk[0]}` AS STRING) LIKE '%{term1}%'")
            if term2: where.append(f"CAST(`{pk[1]}` AS STRING) LIKE '%{term2}%'")
            if show_expired:
                exp_col = "EXPIRY" if table=="state_licence" else "expiryDate"
                where.append(f"CAST(`{exp_col}` AS DATE) < CURRENT_DATE()")
            if show_recent:
                where.append("ingestion_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)")
            where_clause = " AND ".join(where) if where else "1=1"
            df = client.query(f"SELECT * FROM `{table_id}` WHERE {where_clause} ORDER BY ingestion_timestamp DESC LIMIT 5000").to_dataframe()
            if df.empty:
                st.warning("No records found.")
            else:
                st.dataframe(df, height=400)
                csv = df.to_csv(index=False)
                st.download_button("Download CSV", csv, file_name=f"{table}_search_results.csv")
        except Exception as e:
            st.error(f"Search error: {e}")

# ------------- Main App -------------
def main():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
        st.session_state.login_time = None

    if not st.session_state.authenticated:
        login_page()
        return

    with st.sidebar:
        st.markdown(f"<h4>Welcome, {st.session_state.username}</h4>", unsafe_allow_html=True)
        page = st.radio("Go to", ["📤 Upload Data", "🔍 Search Data"])
        if st.button("Logout"):
            logout()
    
    if page == "📤 Upload Data":
        data_upload_page()
    elif page == "🔍 Search Data":
        search_page()

if __name__ == "__main__":
    main()