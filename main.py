import streamlit as st
import pandas as pd
import zipfile
import io
import re
from datetime import datetime
import hashlib
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
service_account_info = json.loads(st.secrets["GCP_SERVICE_ACCOUNT"])
client = bigquery.Client.from_service_account_info(service_account_info)
PROJECT_ID = st.secrets["BIGQUERY_PROJECT"]
DATASET_ID = st.secrets["BIGQUERY_DATASET"]
STATE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.state_licence"
REG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.registration"

# ------------- Enhanced Schemas with data types -------------
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

# ------------- DB Init REMOVED (handled by BigQuery schema management) -------------

# ------------- Enhanced Helpers -------------
# No connect() needed for BigQuery

def smart_column_mapping(df_columns, expected_columns):
    """Intelligent column name matching with fuzzy logic"""
    mapping = {}
    df_cols_lower = [col.strip().lower() for col in df_columns]
    
    for expected_col in expected_columns:
        expected_lower = expected_col.strip().lower()
        
        # Exact match
        if expected_col in df_columns:
            mapping[expected_col] = expected_col
            continue
            
        # Case-insensitive match
        if expected_lower in df_cols_lower:
            idx = df_cols_lower.index(expected_lower)
            mapping[expected_col] = df_columns[idx]
            continue
            
        # Fuzzy matching with common variations
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
    """Enhanced column alignment with intelligent mapping"""
    mapping = smart_column_mapping(list(df.columns), list(expected_cols.keys()))
    
    df2 = pd.DataFrame()
    for target_col in expected_cols.keys():
        source_col = mapping.get(target_col)
        
        if source_col and source_col in df.columns:
            # Convert data types based on schema
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

# ------------- Enhanced File Processing -------------
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

# ------------- Enhanced Data Upload Page -------------
def data_upload_page():
    st.header("📤 Data Upload Center")
    
    # Display current statistics
    col1, col2 = st.columns(2)
    with col1:
        state_count, state_latest = get_table_stats(STATE_TABLE)
        st.metric("State Licence Records", state_count, 
                 f"Last update: {str(state_latest)[:10] if state_latest else 'Never'}")
    with col2:
        reg_count, reg_latest = get_table_stats(REG_TABLE)
        st.metric("Registration Records", reg_count,
                 f"Last update: {str(reg_latest)[:10] if reg_latest else 'Never'}")
    
    st.markdown("---")
    
    # Upload sections with tabs for better organization
    tab1, tab2 = st.tabs(["📋 State Licence Data", "📝 Registration Data"])
    
    with tab1:
        st.subheader("State Licence Data Upload")
        
        col1, col2 = st.columns(2)
        with col1:
            st.info("💡 Upload a ZIP file containing multiple Excel/CSV files")
            zip1 = st.file_uploader("ZIP Archive", type=["zip"], key="zip_state", 
                                   help="Select a ZIP file containing state licence data files")
            if zip1 and st.button("Process ZIP", key="btn_zip_state", use_container_width=True):
                with st.spinner("Processing ZIP file..."):
                    total_rows, successful_files = process_zip_file(zip1, "state_licence")
                    if successful_files > 0:
                        st.success(f"Processed {successful_files} files with {total_rows} total rows")
        
        with col2:
            st.info("💡 Upload individual Excel or CSV files")
            files1 = st.file_uploader("Individual Files", type=["csv", "xlsx", "xls"], 
                                     accept_multiple_files=True, key="files_state",
                                     help="Select one or more state licence data files")
            if files1 and st.button("Process Files", key="btn_files_state", use_container_width=True):
                with st.spinner("Processing files..."):
                    total_rows, successful_files = process_uploaded_files(files1, "state_licence")
                    if successful_files > 0:
                        st.success(f"Processed {successful_files} files with {total_rows} total rows")
    
    with tab2:
        st.subheader("Registration Data Upload")
        
        col1, col2 = st.columns(2)
        with col1:
            st.info("💡 Upload a ZIP file containing multiple Excel/CSV files")
            zip2 = st.file_uploader("ZIP Archive", type=["zip"], key="zip_reg",
                                   help="Select a ZIP file containing registration data files")
            if zip2 and st.button("Process ZIP", key="btn_zip_reg", use_container_width=True):
                with st.spinner("Processing ZIP file..."):
                    total_rows, successful_files = process_zip_file(zip2, "registration")
                    if successful_files > 0:
                        st.success(f"Processed {successful_files} files with {total_rows} total rows")
        
        with col2:
            st.info("💡 Upload individual Excel or CSV files")
            files2 = st.file_uploader("Individual Files", type=["csv", "xlsx", "xls"], 
                                     accept_multiple_files=True, key="files_reg",
                                     help="Select one or more registration data files")
            if files2 and st.button("Process Files", key="btn_files_reg", use_container_width=True):
                with st.spinner("Processing files..."):
                    total_rows, successful_files = process_uploaded_files(files2, "registration")
                    if successful_files > 0:
                        st.success(f"Processed {successful_files} files with {total_rows} total rows")
    
    st.markdown("---")
    
    # Maintenance section with confirmation
    st.subheader("🛠️ Database Maintenance")

    col1, col2 = st.columns(2)

    # BigQuery does not support direct DELETE without advanced setup
    with col1:
        clear_state = st.button("🗑️ Clear State Licence Data", use_container_width=True)
        if clear_state:
            confirm_state = st.checkbox("Confirm deletion of ALL State Licence data", key="confirm_state")
            if confirm_state:
                st.warning("❌ Deletion not supported for BigQuery tables in this app. Please clear data manually in BigQuery.")

    with col2:
        clear_reg = st.button("🗑️ Clear Registration Data", use_container_width=True)
        if clear_reg:
            confirm_reg = st.checkbox("Confirm deletion of ALL Registration data", key="confirm_reg")
            if confirm_reg:
                st.warning("❌ Deletion not supported for BigQuery tables in this app. Please clear data manually in BigQuery.")

# ------------- Enhanced Search Page with Advanced Filters -------------
def search_page():
    st.header("🔍 Advanced Data Search")
    segment = st.radio("Select Data Segment", ["📋 State Licence", "📝 Registration"], horizontal=True)
    table = "state_licence" if "State Licence" in segment else "registration"
    table_id = STATE_TABLE if table == "state_licence" else REG_TABLE
    # Dual primary keys
    if table == "state_licence":
        primary_keys = ["REF ID", "LICENSE"]
    else:
        primary_keys = ["refId", "certificateNo"]
    # Get sample data for filter options
    sample_query = f"SELECT * FROM `{table_id}` LIMIT 1000"
    sample = client.query(sample_query).to_dataframe()
    if sample.empty:
        st.info("No data available. Please upload data first.")
        return
    st.success(
        f"💡 Searching {segment} data. Primary keys: **{primary_keys[0]}** and **{primary_keys[1]}**"
    )
    col1, col2 = st.columns([2, 1])
    with col1:
        search_terms = []
        search_terms.append(
            st.text_input(
                f"🔎 Search by {primary_keys[0]} (supports partial matching)",
                key=f"search_{primary_keys[0]}",
                help=f"Enter full or partial {primary_keys[0]} to search"
            )
        )
        search_terms.append(
            st.text_input(
                f"🔎 Search by {primary_keys[1]} (supports partial matching)",
                key=f"search_{primary_keys[1]}",
                help=f"Enter full or partial {primary_keys[1]} to search"
            )
        )
    with col2:
        st.markdown("**Quick Filters**")
        show_expired = st.checkbox("Show expired records only", value=False)
        show_recent = st.checkbox("Show recent uploads (last 7 days)", value=False)
    # Advanced filters in expander
    with st.expander("🧩 Advanced Filters", expanded=False):
        cols = st.columns(3)
        filter_options = {}
        exclude_cols = {"id", "source_filename", "ingestion_timestamp"}
        available_cols = [c for c in sample.columns if c not in exclude_cols and sample[c].notna().any()]
        for i, col in enumerate(available_cols[:9]):
            with cols[i % 3]:
                unique_vals = sample[col].dropna().unique()
                if len(unique_vals) < 50:
                    selected = st.selectbox(f"Filter by {col}", [""] + sorted(unique_vals.tolist()))
                    if selected:
                        filter_options[col] = selected
                else:
                    filter_text = st.text_input(f"Filter {col} (text contains)")
                    if filter_text:
                        filter_options[col] = filter_text
    expiry_col = "EXPIRY" if table == "state_licence" else "expiryDate"
    expiry_date_filter = st.date_input("Expiry", key="expiry_date_filter", value=None)
    col3, col4 = st.columns(2)
    with col3:
        source_filter = st.text_input("Source")
    with col4:
        date_filter = st.date_input("Date", key="ingestion_date", value=None)
    if st.button("🚀 Execute Search", use_container_width=True):
        with st.spinner("Searching..."):
            try:
                where_conditions = []
                # Dual primary search
                pk_conditions = []
                for idx, term in enumerate(search_terms):
                    if term:
                        pk_conditions.append(f"CAST(`{primary_keys[idx]}` AS STRING) LIKE '%{term}%'")
                if len(pk_conditions) == 1:
                    where_conditions.append(pk_conditions[0])
                elif len(pk_conditions) == 2:
                    where_conditions.append(f"({pk_conditions[0]} AND {pk_conditions[1]})")
                # Quick filters
                if show_expired:
                    expiry_col_bq = "EXPIRY" if table == "state_licence" else "expiryDate"
                    where_conditions.append(f"CAST(`{expiry_col_bq}` AS DATE) < CURRENT_DATE()")
                if show_recent:
                    where_conditions.append("ingestion_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)")
                # Advanced filters
                for col, value in filter_options.items():
                    if isinstance(value, str) and "%" in value:
                        where_conditions.append(f"CAST(`{col}` AS STRING) LIKE '{value}'")
                    elif isinstance(value, str):
                        where_conditions.append(f"CAST(`{col}` AS STRING) = '{value}'")
                    else:
                        where_conditions.append(f"`{col}` = {value}")
                if source_filter:
                    where_conditions.append(f"CAST(`source_filename` AS STRING) LIKE '%{source_filter}%'")
                if date_filter is not None:
                    where_conditions.append(f"CAST(ingestion_timestamp AS DATE) = '{date_filter.strftime('%Y-%m-%d')}'")
                if expiry_date_filter is not None:
                    expiry_col_bq = "EXPIRY" if table == "state_licence" else "expiryDate"
                    where_conditions.append(f"CAST(`{expiry_col_bq}` AS DATE) = '{expiry_date_filter.strftime('%Y-%m-%d')}'")
                where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
                query = f"SELECT * FROM `{table_id}` WHERE {where_clause} ORDER BY ingestion_timestamp DESC LIMIT 5000"
                df = client.query(query).to_dataframe()
                if df.empty:
                    st.warning("No records found matching your criteria.")
                else:
                    st.success(f"Found {len(df)} records")
                    tab1, tab2 = st.tabs(["📊 Data Table", "📈 Summary"])
                    with tab1:
                        st.dataframe(df, use_container_width=True, height=400)
                    with tab2:
                        st.subheader("Search Summary")
                        c1, c2, c3 = st.columns(3)
                        with c1:
                            st.metric("Total Records", len(df))
                        with c2:
                            st.metric("Columns", len(df.columns))
                        with c3:
                            latest = df["ingestion_timestamp"].max() if "ingestion_timestamp" in df.columns else "N/A"
                            st.metric("Latest Update", str(latest)[:10])
                    st.subheader("📤 Export Results")
                    export_col1, export_col2 = st.columns(2)
                    with export_col1:
                        csv = df.to_csv(index=False)
                        st.download_button("💾 Download CSV", csv,
                                         file_name=f"{table}_search_results.csv",
                                         mime="text/csv",
                                         use_container_width=True)
                    with export_col2:
                        try:
                            xlsx_bytes = io.BytesIO()
                            df.to_excel(xlsx_bytes, index=False, engine="openpyxl")
                            xlsx_bytes.seek(0)
                            st.download_button("📊 Download XLSX", xlsx_bytes,
                                             file_name=f"{table}_search_results.xlsx",
                                             mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                             use_container_width=True)
                        except Exception as e:
                            st.warning(f"XLSX export unavailable: {e}")
            except Exception as e:
                st.error(f"Search error: {str(e)}")


# ------------- Enhanced Main Application -------------
def main():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
        st.session_state.login_time = None

    if not st.session_state.authenticated:
        login_page()
        return

    # Sidebar with user info and navigation
    with st.sidebar:
        st.markdown(f"""
        <div style='background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                    padding: 1rem; border-radius: 10px; color: white; text-align: center;'>
            <h3>🍽️ FSM System</h3>
            <p>Welcome, <strong>{st.session_state.username}</strong></p>
            <p>Logged in: {st.session_state.login_time.strftime('%Y-%m-%d %H:%M')}</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Navigation
        st.subheader("Navigation")
        page = st.radio("Go to", 
                       ["📤 Upload Data", "🔍 Search Data"],
                       label_visibility="collapsed")
        
        st.markdown("---")
        
        # Logout button
        if st.button("🚪 Logout", use_container_width=True):
            logout()
    
    # Main content area
    if "Upload Data" in page:
        data_upload_page()
    elif "Search Data" in page:
        search_page()

if __name__ == "__main__":
    main()
