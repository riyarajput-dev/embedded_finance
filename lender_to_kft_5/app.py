import streamlit as st
import requests
import yaml
from box import ConfigBox

# API Configuration
API_BASE_URL = "http://localhost:1226"

st.set_page_config(page_title="Lender Offers to KFT", layout="wide")

# Helper Functions
def get_config():
    try:
        response = requests.get(f"{API_BASE_URL}/config")
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Failed to fetch config: {e}")
        return None

def update_config(cfg):
    try:
        response = requests.post(f"{API_BASE_URL}/config", json=cfg)
        response.raise_for_status()
        return True
    except Exception as e:
        st.error(f"Failed to update config: {e}")
        return False

def fetch_batches(lender_name):
    try:
        response = requests.get(f"{API_BASE_URL}/api/{lender_name}/fetch-batches")
        response.raise_for_status()
        return response.json().get("batches", [])
    except Exception as e:
        st.error(f"Failed to fetch batches: {e}")
        return []

def process_lender_batch(lender, batch):
    payload = {
        "lender_name": lender,
        "batch_name": batch
    }
    try:
        response = requests.post(f"{API_BASE_URL}/lenders-response", json=payload)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Processing Error: {e}")
        return {"status": "error", "message": str(e)}

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
</style>
""", unsafe_allow_html=True)

st.title("🏦 Lender Offers to KFT")
st.markdown("Process lender offer data from S3 to standard schema")

if 'available_batches' not in st.session_state:
    st.session_state.available_batches = []

# Main Flow Container
with st.container(border=True):
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("1. Select Lender")
        # Define available lenders - can be made dynamic if needed
        lenders = ["abfcl", "Prefr", "Zype"] 
        selected_lender = st.selectbox("Lender Name", lenders, key="lender_dropdown")
        
        if st.button("Update Config & Fetch Batches", type="secondary"):
            with st.spinner(f"Updating config for {selected_lender}..."):
                # Fetch current config, update lender, and save back
                current_cfg = get_config()
                if current_cfg:
                    current_cfg["location"]["lender_name"] = selected_lender
                    if update_config(current_cfg):
                        st.success(f"Config updated for {selected_lender}")
                        
                        # Fetch batches
                        batches = fetch_batches(selected_lender)
                        st.session_state.available_batches = batches
                        if not batches:
                            st.warning(f"No batches found for {selected_lender}")
                        else:
                            st.info(f"Found {len(batches)} batches")
    
    with col2:
        st.subheader("2. Select Batch & Process")
        if st.session_state.available_batches:
            selected_batch = st.selectbox("Select Batch", st.session_state.available_batches)
            
            if st.button("🚀 Process Batch", type="primary"):
                with st.status(f"Processing batch {selected_batch} for {selected_lender}...", expanded=True) as status:
                    st.write("📡 Calling API to process data...")
                    result = process_lender_batch(selected_lender, selected_batch)
                    
                    if result.get("status") == "success":
                        st.write(result.get("message"))
                        status.update(label="Processing Completed!", state="complete", expanded=False)
                        st.success(f"✅ {result.get('message')}")
                        st.balloons()
                    else:
                        status.update(label="Failed", state="error")
                        st.error(f"❌ {result.get('message')}")
        else:
            st.info("Please select a lender and click 'Update Config & Fetch Batches' to see available batches.")

# Footer or Information
st.divider()
config_data = get_config()
if config_data:
    st.expander("Current Configuration").json(config_data)

