import streamlit as st
import requests
import time

# API Configuration
API_BASE_URL = "http://localhost:1226"

# ---------------------------
# Helper: API Calls
# ---------------------------
def get_batches_for_bureau(bureau_name):
    try:
        response = requests.get(f"{API_BASE_URL}/api/batches/bureau/{bureau_name}")
        response.raise_for_status()
        data = response.json()
        return data.get("batches", []), data.get("target_prefix", "")
    except Exception as e:
        st.error(f"Error fetching batches from API: {e}")
        return [], f"bureau-to-kft/{bureau_name}"

def get_files_for_batch(bureau_name, batch_name):
    try:
        response = requests.get(f"{API_BASE_URL}/files/{bureau_name}/{batch_name}")
        response.raise_for_status()
        data = response.json()
        return data.get("files", [])
    except Exception as e:
        st.error(f"Error fetching files from API: {e}")
        return []

def run_pipeline(bureau, batch, mode="Automatic", files=None):
    payload = {
        "bureau": bureau,
        "batch": batch,
        "mode": mode,
        "files": files
    }
    try:
        response = requests.post(f"{API_BASE_URL}/run-pipeline", json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        detail = e.response.json().get("detail", str(e))
        st.error(f"Pipeline Error: {detail}")
        return {"success": False, "message": detail}
    except Exception as e:
        st.error(f"Unexpected error calling API: {e}")
        return {"success": False, "message": str(e)}

# ---------------------------
# UI Layout
# ---------------------------
st.set_page_config(page_title="Bureau ETL Pipeline", layout="wide")

# Custom CSS
st.markdown("""
<style>
    .stButton>button {
        width: 100%;
        border-radius: 10px;
        height: 3em;
    }
    .big-font {
        font-size: 24px !important;
        font-weight: bold;
    }
    div[data-testid="stMetricValue"] {
        font-size: 18px;
    }
</style>
""", unsafe_allow_html=True)

st.title("📌 Bureau ETL Pipeline")
st.markdown("Advanced Bureau Data Processing & Management")

# Trigger Mode Selection    
col1, col2 = st.columns(2)
with col1:
    if st.button("⚡ Automatic Trigger", type="primary" if st.session_state.get("trigger_mode", "Automatic") == "Automatic" else "secondary"):
        st.session_state.trigger_mode = "Automatic"
        st.rerun()

with col2:
    if st.button("📄 Manual Trigger", type="primary" if st.session_state.get("trigger_mode") == "Manual" else "secondary"):
        st.session_state.trigger_mode = "Manual"
        st.rerun()

mode = st.session_state.get("trigger_mode", "Automatic")

# ---------------------------
# Automatic Trigger Section
# ---------------------------
if mode == "Automatic":
    st.markdown("### ⚡ Automatic Trigger")
    
    with st.container(border=True):
        # Bureau Selection
        bureau = st.selectbox("Select Bureau", ["equifax", "experian"], key="auto_bureau")
        
        # Batch Selection
        batches, _ = get_batches_for_bureau(bureau)
        batch = None
        if not batches:
            st.warning("No batches found.")
        else:
            batch = st.selectbox("Available Batch", batches, key="auto_batch")
            
        if st.button("🚀 Trigger Upload", type="primary", disabled=not batch):
            with st.status("Running Automatic Pipeline via API...", expanded=True) as status:
                st.write("📡 Calling API to start pipeline...")
                result = run_pipeline(bureau, batch, mode="Automatic")
                
                if result.get("success"):
                    st.write(result.get("message"))
                    status.update(label="Automatic Pipeline Completed!", state="complete", expanded=False)
                    st.success("✅ Uploaded Successfully")
                    st.balloons()
                else:
                    status.update(label="Failed", state="error")

# ---------------------------
# Manual Trigger Section
# ---------------------------
elif mode == "Manual":
    st.markdown("### 📄 Manual Trigger")
    st.info("Custom file selection & upload")
    
    with st.container(border=True):
        col_m1, col_m2 = st.columns(2)
        with col_m1:
            bureau = st.selectbox("Select Bureau", ["equifax", "experian"], key="manual_bureau")
        with col_m2:
            batches, _ = get_batches_for_bureau(bureau)
            batch = st.selectbox("Select Batch", batches if batches else [], key="manual_batch")
            
        # File Selection
        selected_files = []
        if batch:
            st.markdown("#### Available Files")
            files = get_files_for_batch(bureau, batch)
            if files:
                selected_files = st.multiselect("Select files to process", files, default=[])
                st.caption(f"{len(selected_files)} files selected")
            else:
                st.warning("No files found in this batch.")
        
        if st.button("🚀 Trigger Upload", type="primary", disabled=not selected_files):
             with st.status("Running Manual Pipeline via API...", expanded=True) as status:
                st.write(f"📂 Triggering processing for {len(selected_files)} files...")
                result = run_pipeline(bureau, batch, mode="Manual", files=selected_files)
                
                if result.get("success"):
                    st.write(result.get("message"))
                    status.update(label="Manual Extraction Completed!", state="complete", expanded=False)
                    st.success(f"✅ Successfully processed {len(selected_files)} files.")
                    st.snow()
                else:
                    status.update(label="Failed", state="error")
