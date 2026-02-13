import streamlit as st
import pandas as pd
import requests
from datetime import datetime

st.set_page_config(page_title="Lender Data Exchange", layout="wide")

st.title("🚀 Lender Data Exchange")

API_URL = "http://localhost:1226"

    
# Lender Mapping
LENDER_MAPPING = {
    "prefr": "prefr",
    "zype_experian": "zype_experian",
    "zype_equifax": "zype_equifax"
}

# Sidebar for Lender Selection
st.sidebar.header("Configuration")
lender_display_name = st.sidebar.selectbox(
    "Select Lender",
    options=list(LENDER_MAPPING.keys()),
    index=0
)
lender_name = LENDER_MAPPING[lender_display_name]

st.subheader(f"Batches for {lender_display_name}")

def get_batches(l_name):
    try:
        response = requests.get(f"{API_URL}/api/batches/lender/{l_name}")
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Error fetching batches: {e}")
        return None

batch_data = get_batches(lender_name)

if not batch_data or not batch_data.get("batches"):
    st.warning(f"No batches found for {lender_name}")
else:
    batches = batch_data["batches"]
    # Display batches in a table with checkboxes
    batch_df = pd.DataFrame({"Batch Name": batches})
    batch_df["Select"] = False
    
    edited_df = st.data_editor(
        batch_df,
        column_config={
            "Select": st.column_config.CheckboxColumn(
                "Select",
                help="Select batches to send",
                default=False,
            )
        },
        disabled=["Batch Name"],
        hide_index=True,
    )

    selected_batches = edited_df[edited_df["Select"]]["Batch Name"].tolist()

    if st.button("Send to Lender"):
        if not selected_batches:
            st.error("Please select at least one batch.")
        else:
            with st.spinner("Processing..."):
                try:
                    # 1. Data Preparation: Concat and upload
                    process_payload = {
                        "lender_name": lender_name,
                        "lender_display_name": lender_display_name,
                        "selected_batches": selected_batches
                    }
                    proc_resp = requests.post(f"{API_URL}/process", json=process_payload)
                    proc_resp.raise_for_status()
                    new_batch_folder = proc_resp.json().get("new_batch_folder")
                    
                    if new_batch_folder:
                        st.success(f"Successfully processed {len(selected_batches)} batches into S3")
                        
                        # Mapping for shortlisting
                        if lender_display_name == "prefr":
                            l_name, b_types = "prefr", ["experian"]
                        elif lender_display_name == "zype_experian":
                            l_name, b_types = "zype", ["experian"]
                        elif lender_display_name == "zype_equifax":
                            l_name, b_types = "zype", ["equifax"]
                        else:
                            l_name, b_types = lender_name, ["experian"]
                            
                        # 2. Data Ingestion & Shortlisting
                        st.info(f"Triggering shortlisting for {l_name} - {b_types}")
                        shortlist_payload = {
                            "new_batch_folder": new_batch_folder,
                            "l_name": l_name,
                            "b_types": b_types
                        }
                        sl_resp = requests.post(f"{API_URL}/shortlist", json=shortlist_payload)
                        sl_resp.raise_for_status()
                        
                        st.success("Shortlisting and Ingestion completed successfully!")
                    else:
                        st.error("No files were processed.")
                    
                except Exception as e:
                    st.error(f"Error during processing: {str(e)}")

