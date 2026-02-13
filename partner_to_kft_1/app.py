import streamlit as st
import requests
import pandas as pd
from datetime import datetime
import os

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:1226")

st.set_page_config(page_title="Partner to KFT", page_icon="🔄", layout="wide")

# Custom CSS
st.markdown("""
    <style>
    .main-header { font-size: 2.5rem; font-weight: 700; color: #1f77b4; margin-bottom: 0.5rem; }
    .sub-header { font-size: 1.1rem; color: #666; margin-bottom: 2rem; }
    .log-entry { padding: 0.5rem; margin: 0.25rem 0; border-radius: 5px; background-color: #f8f9fa; border-left: 3px solid #667eea; }
    .stButton>button { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; font-weight: 600; }
    </style>
""", unsafe_allow_html=True)

st.markdown('<div class="main-header">🔄 Partner to KFT System</div>', unsafe_allow_html=True)

# Initialize session state
st.session_state.setdefault('logs', [])
st.session_state.setdefault('refresh', 0)
st.session_state.setdefault('last_results', [])

def add_log(message, level="info"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    st.session_state.logs.append(f"[{timestamp}] {message}")
    if level == "error": st.error(message)
    elif level == "success": st.success(message)
    elif level == "warning": st.warning(message)

def api_call(endpoint, method='GET', data=None, timeout=10):
    try:
        url = f"{API_BASE_URL}/{endpoint}"
        response = requests.request(method, url, json=data, timeout=timeout)
        return response.json() if response.status_code == 200 else []
    except Exception as e:
        st.error(f"❌ API Error: {e}")
        return [] if method == 'GET' else {"status": "failed", "message": str(e)}

# Create tabs
tab1, tab2 = st.tabs(["📥 Ingestion", "⚙️ Processing"])

# ===========================
# TAB 1: INGESTION
# ===========================
with tab1:
    st.markdown('<div class="sub-header">Upload files to Raw Table</div>', unsafe_allow_html=True)
    
    # File Upload
    uploaded_files = st.file_uploader("Choose files", accept_multiple_files=True)
    
    if uploaded_files and st.button("🚀 Upload and Save to Raw", use_container_width=True):
        progress_bar = st.progress(0)
        add_log(f"🚀 Uploading {len(uploaded_files)} file(s)...")
        
        try:
            files_data = [("files", (f.name, f.getvalue(), f.type)) for f in uploaded_files]
            response = requests.post(f"{API_BASE_URL}/files/upload", files=files_data, timeout=300)
            
            if response.status_code == 200:
                result = response.json()
                data = result.get("data", {})
                
                # Get duplicates
                dup_response = requests.get(f"{API_BASE_URL}/logs/duplicates")
                if dup_response.status_code == 200:
                    dup_logs = dup_response.json().get("logs", [])
                    if dup_logs:
                        st.markdown("---")
                        st.warning("⚠️ **Duplicate Records Detected**")
                        for log in dup_logs:
                            with st.expander(f"📊 Details for {log['filename']}"):
                                col1, col2, col3 = st.columns(3)
                                col1.metric("Original Count", log['original_count'])
                                col2.metric("In-file Duplicates", log['infile_dropped'], delta_color="inverse")
                                col3.metric("In-batch Duplicates", log['inbatch_dropped'], delta_color="inverse")
                                
                                st.write(f"**Duplicate Columns:** {', '.join(log['duplicate_columns'])}")
                                st.info(f"💡 Records saved to raw table: {log['original_count'] - log['infile_dropped'] - log['inbatch_dropped']}")
                
                st.balloons()
            else:
                add_log(f"❌ API error: {response.text}", "error")
        
        except Exception as e:
            add_log(f"❌ Error: {e}", "error")
        
        progress_bar.progress(1.0)
    
    # Logs
    st.markdown("---")
    st.markdown("### 📋 Logs")
    for log in reversed(st.session_state.logs):
        st.markdown(f'<div class="log-entry">{log}</div>', unsafe_allow_html=True)
    
    if st.session_state.logs and st.button("🗑️ Clear Logs"):
        st.session_state.logs = []
        try:
            requests.delete(f"{API_BASE_URL}/clear-logs")
        except:
            pass
        st.rerun()

# ===========================
# TAB 2: PROCESSING
# ===========================
with tab2:
    st.markdown('<div class="sub-header">Process batches from Raw to Cleaned</div>', unsafe_allow_html=True)
    
    # Show summary if exists
    if st.session_state.last_results:
        st.markdown("---\n### 📊 Last Processing Summary")
        for res in st.session_state.last_results:
            with st.expander(f"Batch {res.get('batch_no')}", expanded=True):
                cols = st.columns(4)
                cols[0].metric("Original", res.get('original_count', 0))
                cols[1].metric("Missing KFT ID", res.get('missing_kft_id_count', 0), delta_color="inverse")
                cols[2].metric("Duplicates", res.get('duplicates_found', 0), delta_color="inverse")
                cols[3].metric("Saved", res.get('cleaned_count', 0))
    
    pending = [b for b in api_call("batches/pending") if b['status'] == 'not processed']
    
    if not pending:
        st.success("✅ No pending batches!")
    else:
        df = pd.DataFrame(pending)
        
        # Sidebar filters
        st.sidebar.header("🔍 Filters")
        partners = ["All"] + sorted(df['partner_name'].unique())
        selected_partner = st.sidebar.selectbox("Partner", partners)
        
        batches = ["All"]
        if selected_partner != "All":
            batches += sorted(df[df['partner_name'] == selected_partner]['batch_no'].tolist())
        selected_batch = st.sidebar.selectbox("Batch Number", batches, disabled=selected_partner=="All")
        
        # Filter data
        filtered = df.copy()
        if selected_partner != "All":
            filtered = filtered[filtered['partner_name'] == selected_partner]
            if selected_batch != "All":
                filtered = filtered[filtered['batch_no'] == selected_batch]
        
        # File view or batch view
        is_file_view = selected_partner != "All" and selected_batch != "All"
        
        if is_file_view:
            st.markdown(f"### 📄 Files in {selected_partner} - Batch {selected_batch}")
            files = api_call(f"batches/{selected_partner}/{selected_batch}/files")
            if not files:
                st.warning("No files found")
            else:
                display = pd.DataFrame(files)[['filename', 'no_of_records', 'status']]
                st.dataframe(display, use_container_width=True)
                
                if st.button("🚀 Process Batch"):
                    result = api_call("batches/process", 'POST', 
                                     {"partner_name": selected_partner, "batch_no": int(selected_batch)}, 300)
                    if result.get("status") == "success":
                        st.success(f"✅ Processed!")
                        st.session_state.last_results = [result]
                        st.balloons()
                    else:
                        st.error(f"❌ Failed: {result.get('message')}")
                    st.rerun()
        else:
            st.markdown(f"### 📋 Batches ({len(filtered)})")
            filtered['Select'] = False
            
            edited = st.data_editor(
                filtered[['Select', 'partner_name', 'batch_no', 'no_of_records', 'status']],
                column_config={"Select": st.column_config.CheckboxColumn("Select", default=False)},
                disabled=["partner_name", "batch_no", "no_of_records", "status"],
                hide_index=True,
                use_container_width=True,
                key=f"editor_{st.session_state.refresh}"
            )
            
            if st.button("🚀 Process Selected"):
                selected = edited[edited['Select']]
                if selected.empty:
                    st.warning("Select at least one batch")
                else:
                    batches = [{"partner_name": r['partner_name'], "batch_no": int(r['batch_no'])} 
                              for _, r in selected.iterrows()]
                    result = api_call("batches/process-multiple", 'POST', {"batches": batches}, 600)
                    
                    if result:
                        st.session_state.last_results = [r['result'] for r in result['results'] 
                                                        if r['result'].get('status') == 'success']
                        st.success(f"✅ Processed {result['successful']}/{result['total']}")
                        st.balloons()
                    st.session_state.refresh += 1
                    st.rerun()