import streamlit as st
import requests
import pandas as pd
from datetime import datetime

# Configure page
st.set_page_config(page_title="Bureau Data Processing", layout="wide")

# API Configuration
API_BASE_URL = "http://localhost:1226"

# ==================== API CLIENT FUNCTIONS ====================

def api_get_summary(bureau_type="equifax"):
    """Fetch summary data from API"""
    try:
        response = requests.get(f"{API_BASE_URL}/api/summary/{bureau_type}", timeout=30)
        response.raise_for_status()
        data = response.json().get("data", [])
        return pd.DataFrame(data) if data else pd.DataFrame()
    except requests.exceptions.RequestException as e:
        st.error(f"Error connecting to API: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching summary: {e}")
        return pd.DataFrame()

def api_get_pending(bureau_type="equifax", days_threshold=90):
    """Fetch pending data from API"""
    try:
        response = requests.get(
            f"{API_BASE_URL}/api/pending/{bureau_type}", 
            params={"days_threshold": days_threshold},
            timeout=30
        )
        response.raise_for_status()
        data = response.json().get("data", [])
        return pd.DataFrame(data) if data else pd.DataFrame()
    except requests.exceptions.RequestException as e:
        st.error(f"Error connecting to API: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching pending: {e}")
        return pd.DataFrame()

def api_get_experian_tab(score_threshold=600, days_threshold=90):
    """Fetch Experian tab data from API"""
    try:
        response = requests.get(
            f"{API_BASE_URL}/api/experian-tab/{score_threshold}", 
            params={"days_threshold": days_threshold},
            timeout=30
        )
        response.raise_for_status()
        data = response.json().get("data", [])
        return pd.DataFrame(data) if data else pd.DataFrame()
    except requests.exceptions.RequestException as e:
        st.error(f"Error connecting to API: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching Experian data: {e}")
        return pd.DataFrame()

def api_get_experian_status(scrub_batch_no):
    """Check Equifax response status for a scrub batch"""
    try:
        response = requests.get(f"{API_BASE_URL}/api/experian-status/{scrub_batch_no}")
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Error checking status: {e}")
        return None

def api_process_equifax(batch_numbers, days_threshold=90):
    """Process and send batches to Equifax"""
    try:
        response = requests.post(
            f"{API_BASE_URL}/api/process/equifax",
            json={
                "batch_numbers": batch_numbers, 
                "bureau_type": "equifax",
                "days_threshold": days_threshold
            }
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Error processing batches: {e}")
        return None

def api_process_experian(batch_numbers, days_threshold=90):
    """Process and send batches to Experian"""
    try:
        response = requests.post(
            f"{API_BASE_URL}/api/process/experian",
            json={
                "batch_numbers": batch_numbers, 
                "bureau_type": "experian",
                "days_threshold": days_threshold
            }
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Error processing batches: {e}")
        return None

def api_send_to_experian(scrub_batches, send_type="all", score_threshold=600):
    """Send data to Experian"""
    try:
        response = requests.post(
            f"{API_BASE_URL}/api/send-to-experian",
            json={
                "scrub_batches": scrub_batches,
                "send_type": send_type,
                "score_threshold": score_threshold
            }
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Error sending to Experian: {e}")
        return None

# ==================== CACHING FUNCTIONS ====================

def load_summary_cached(bureau_type="equifax", force_refresh=False):
    """Cache summary data to avoid repeated queries"""
    key = f"summary_{bureau_type}"
    if not force_refresh and key in st.session_state:
        return st.session_state[key]
    df = api_get_summary(bureau_type=bureau_type)
    if df is not None and not df.empty:
        st.session_state[key] = df
    return df

def load_pending_cached(bureau_type="equifax", days_threshold=90, force_refresh=False):
    """Cache pending data separately"""
    key = f"pending_{bureau_type}_{days_threshold}"
    if not force_refresh and key in st.session_state:
        return st.session_state[key]
    df = api_get_pending(bureau_type=bureau_type, days_threshold=days_threshold)
    if df is not None:
        st.session_state[key] = df
    return df

def load_experian_tab_cached(input_score=600, days_threshold=90, force_refresh=False):
    """Cache Experian tab data"""
    key = "experian_tab_data"
    if (not force_refresh and key in st.session_state and
            st.session_state[key] is not None and
            st.session_state[key].get('input_score') == input_score and
            st.session_state[key].get('days_threshold') == days_threshold):
        return st.session_state[key]['df']
    df = api_get_experian_tab(input_score, days_threshold=days_threshold)
    if df is not None:
        st.session_state[key] = {
            'input_score': input_score, 
            'days_threshold': days_threshold,
            'df': df
        }
    return df

def invalidate_cache(keys):
    """Invalidate specific cache keys"""
    for k in keys:
        if k in st.session_state:
            del st.session_state[k]

# ==================== STATUS DETERMINATION ====================

def determine_status(row, case="Equifax Only"):
    """Determine the status of a batch based on processing state"""
    if case == "Equifax Only":
        if row['RESPONSE_COUNT'] > 0 and row['RESPONSE_COUNT'] >= row['SENT_COUNT']:
            return "Response Received"
        elif row['SENT_COUNT'] > 0:
            if row['SENT_COUNT'] >= row['TOTAL_RECORDS']:
                return "Sent (Awaiting Response)"
            else:
                return f"Partially Sent ({row['SENT_COUNT']}/{row['TOTAL_RECORDS']})"
        else:
            return "Pending"
    
    elif case == "Experian Only":
        if row['RESPONSE_COUNT'] > 0 and row['RESPONSE_COUNT'] >= row['SENT_COUNT']:
            return "Response Received"
        elif row['SENT_COUNT'] > 0:
            if row['SENT_COUNT'] >= row['TOTAL_RECORDS']:
                return "Sent (Awaiting Response)"
            else:
                return f"Partially Sent ({row['SENT_COUNT']}/{row['TOTAL_RECORDS']})"
        else:
            return "Pending"
    
    else:  # Equifax then Experian
        if row['EXPERIAN_RESPONSE_COUNT'] > 0:
            return "Response Received from Experian"
        elif row['SENT_TO_EXPERIAN_COUNT'] > 0:
            return "Sent to Experian"
        elif row['RESPONSE_COUNT'] > 0:
            return "Response Received from Equifax"
        elif row['SENT_COUNT'] > 0:
            if row['SENT_COUNT'] >= row['TOTAL_RECORDS']:
                return "Sent to Equifax"
            else:
                return f"Partially Sent to Equifax ({row['SENT_COUNT']}/{row['TOTAL_RECORDS']})"
        else:
            return "Pending"

# ==================== RENDERING FUNCTIONS ====================

def render_equifax_only():
    """Render Equifax Only processing view"""
    st.header("📊 Equifax Only Processing")
    
    if 'show_equifax_summary' not in st.session_state:
        st.session_state['show_equifax_summary'] = False

    if st.button("🔄 Load Data", key="show_eq_only"):
        st.session_state['show_equifax_summary'] = True

    if not st.session_state['show_equifax_summary']:
        return

    col1, col2 = st.columns([1, 2])
    with col1:
        scrub_threshold = st.selectbox(
            "Last Scrub Date Threshold",
            options=[60, 90, 120],
            index=1,
            format_func=lambda x: f">{x} days",
            key="eq_only_threshold"
        )
    
    summary_df = load_summary_cached(bureau_type="equifax")
    pending_df = load_pending_cached(bureau_type="equifax", days_threshold=scrub_threshold)
    
    if summary_df is None or summary_df.empty:
        st.info("No data available for Equifax processing")
        return
    
    # Filter to exclude Experian-only batches
    if 'SENT_TO_EXPERIAN_COUNT' in summary_df.columns and 'SENT_COUNT' in summary_df.columns:
        summary_df = summary_df[~((summary_df['SENT_TO_EXPERIAN_COUNT'] > 0) & (summary_df['SENT_COUNT'] == 0))]
    
    # Check if DataFrame is empty after filtering
    if summary_df.empty:
        st.info("No Equifax batches found after filtering")
        return
    
    summary_df['Status'] = summary_df.apply(lambda x: determine_status(x, "Equifax Only"), axis=1)
    
    st.subheader("📋 All Batches Summary")
    display_cols = ['PARTNER_NAME', 'BATCH_NO', 'RECEIVED_DATE', 'TOTAL_RECORDS', 'SENT_COUNT', 'RESPONSE_COUNT', 'SCRUB_BATCH_NO', 'Status']
    st.dataframe(
        summary_df[display_cols].rename(columns={
            'PARTNER_NAME': 'Partner Name',
            'BATCH_NO': 'Batch No',
            'RECEIVED_DATE': 'Received Date',
            'TOTAL_RECORDS': 'Total Records',
            'SENT_COUNT': 'Sent',
            'RESPONSE_COUNT': 'Responses',
            'SCRUB_BATCH_NO': 'Scrub Batch',
            'Status': 'Status'
        }),
        use_container_width=True,
        hide_index=True
    )
    
    if pending_df is not None and not pending_df.empty:
        st.subheader("⚠️ Batches with Pending Records")
        st.dataframe(
            pending_df.rename(columns={
                'PARTNER_NAME': 'Partner Name',
                'BATCH_NO': 'Batch No',
                'RECEIVED_DATE': 'Received Date',
                'PENDING_RECORDS': 'Pending Records',
                'LAST_SCRUB_BATCH': 'Last Scrub Batch'
            }),
            use_container_width=True,
            hide_index=True
        )
        
        st.subheader("🚀 Process Pending Records")
        selected_batches = st.multiselect(
            "Select batches to send to Equifax",
            pending_df['BATCH_NO'].tolist(),
            key="eq_only_select"
        )
        
        if selected_batches:
            st.info(f"Selected {len(selected_batches)} batch(es) for processing - Will be sent as a single combined batch")
            
            if st.button("✅ Send to Equifax", type="primary", key="eq_only_send"):
                with st.spinner(f"Processing {len(selected_batches)} batch(es)..."):
                    result = api_process_equifax(selected_batches, days_threshold=scrub_threshold)
                    
                    if result:
                        st.success(f"✓ {result['message']}: {result['total_records']} total records")
                        for warning in result.get('warnings', []):
                            st.warning(f"⚠ {warning}")
                        invalidate_cache(['summary_equifax', 'pending_equifax'])
                        st.balloons()
                        st.rerun()
    else:
        st.success("✅ No pending batches. All records have been processed!")

def render_experian_only():
    """Render Experian Only processing view"""
    st.header("📊 Experian Only Processing")
    
    if 'show_experian_summary' not in st.session_state:
        st.session_state['show_experian_summary'] = False

    if st.button("🔄 Load Data", key="show_exp_only"):
        st.session_state['show_experian_summary'] = True

    if not st.session_state['show_experian_summary']:
        return

    col1, col2 = st.columns([1, 2])
    with col1:
        scrub_threshold = st.selectbox(
            "Last Scrub Date Threshold",
            options=[60, 90, 120],
            index=1,
            format_func=lambda x: f">{x} days",
            key="exp_only_threshold"
        )

    summary_df = load_summary_cached(bureau_type="experian")
    pending_df = load_pending_cached(bureau_type="experian", days_threshold=scrub_threshold)
    
    if summary_df is None or summary_df.empty:
        st.info("No data available for Experian processing")
        return
    
    # Filter to exclude Equifax-only batches
    if 'SENT_COUNT' in summary_df.columns and 'SENT_TO_EXPERIAN_COUNT' in summary_df.columns:
        summary_df = summary_df[~((summary_df['SENT_COUNT'] > 0) & (summary_df['SENT_TO_EXPERIAN_COUNT'] == 0))]
    
    # Check if DataFrame is empty after filtering
    if summary_df.empty:
        st.info("No Experian batches found after filtering")
        return
    
    summary_df['Status'] = summary_df.apply(lambda x: determine_status(x, "Experian Only"), axis=1)
    
    st.subheader("📋 All Batches Summary")
    display_cols = ['PARTNER_NAME', 'BATCH_NO', 'RECEIVED_DATE', 'TOTAL_RECORDS', 'SENT_TO_EXPERIAN_COUNT', 'EXPERIAN_RESPONSE_COUNT', 'SCRUB_BATCH_NO', 'Status']
    st.dataframe(
        summary_df[display_cols].rename(columns={
            'PARTNER_NAME': 'Partner Name',
            'BATCH_NO': 'Batch No',
            'RECEIVED_DATE': 'Received Date',
            'TOTAL_RECORDS': 'Total Records',
            'SENT_TO_EXPERIAN_COUNT': 'Sent',
            'EXPERIAN_RESPONSE_COUNT': 'Responses',
            'SCRUB_BATCH_NO': 'Scrub Batch',
            'Status': 'Status'
        }),
        use_container_width=True,
        hide_index=True
    )
    
    if pending_df is not None and not pending_df.empty:
        st.subheader("⚠️ Batches with Pending Records")
        st.dataframe(
            pending_df.rename(columns={
                'PARTNER_NAME': 'Partner Name',
                'BATCH_NO': 'Batch No',
                'RECEIVED_DATE': 'Received Date',
                'PENDING_RECORDS': 'Pending Records',
                'LAST_SCRUB_BATCH': 'Last Scrub Batch'
            }),
            use_container_width=True,
            hide_index=True
        )
        
        st.subheader("🚀 Process Pending Records")
        selected_batches = st.multiselect(
            "Select batches to send to Experian",
            pending_df['BATCH_NO'].tolist(),
            key="exp_only_select"
        )
        
        if selected_batches:
            st.info(f"Selected {len(selected_batches)} batch(es) for processing - Will be sent as a single combined batch")
            
            if st.button("✅ Send to Experian", type="primary", key="exp_only_send"):
                with st.spinner(f"Processing {len(selected_batches)} batch(es)..."):
                    result = api_process_experian(selected_batches, days_threshold=scrub_threshold)
                    
                    if result:
                        st.success(f"✓ {result['message']}: {result['total_records']} total records")
                        for warning in result.get('warnings', []):
                            st.warning(f"⚠ {warning}")
                        invalidate_cache(['summary_experian', 'pending_experian'])
                        st.balloons()
                        st.rerun()
    else:
        st.success("✅ No pending batches. All records have been processed!")

def render_equifax_then_experian():
    """Render Equifax → Experian flow with two tabs"""
    st.header("📊 Equifax → Experian Processing Flow")
    
    tab1, tab2 = st.tabs(["📤 Equifax Processing", "📥 Experian Processing"])
    
    with tab1:
        st.subheader("Equifax Processing Stage")
        
        if 'show_eq_then_eq' not in st.session_state:
            st.session_state['show_eq_then_eq'] = False

        if st.button("🔄 Load Data", key="show_eq_then_tab1"):
            st.session_state['show_eq_then_eq'] = True

        if not st.session_state['show_eq_then_eq']:
            return

        col1, col2 = st.columns([1, 2])
        with col1:
            scrub_threshold = st.selectbox(
                "Last Scrub Date Threshold",
                options=[60, 90, 120],
                index=1,
                format_func=lambda x: f">{x} days",
                key="eq_then_threshold"
            )

        summary_df = load_summary_cached(bureau_type="equifax")
        pending_df = load_pending_cached(bureau_type="equifax", days_threshold=scrub_threshold)
        
        if summary_df is None or summary_df.empty:
            st.info("No data available for Equifax processing")
            return
        
        summary_df['Status'] = summary_df.apply(lambda x: determine_status(x, "Equifax then Experian"), axis=1)
        
        st.markdown("#### 📋 All Batches Summary")
        display_cols = ['PARTNER_NAME', 'BATCH_NO', 'RECEIVED_DATE', 'TOTAL_RECORDS', 'SENT_COUNT', 'RESPONSE_COUNT', 'SENT_TO_EXPERIAN_COUNT', 'SCRUB_BATCH_NO', 'Status']
        st.dataframe(
            summary_df[display_cols].rename(columns={
                'PARTNER_NAME': 'Partner Name',
                'BATCH_NO': 'Batch No',
                'RECEIVED_DATE': 'Received Date',
                'TOTAL_RECORDS': 'Total Records',
                'SENT_COUNT': 'Sent to Equifax',
                'RESPONSE_COUNT': 'Equifax Responses',
                'SENT_TO_EXPERIAN_COUNT': 'Sent to Experian',
                'SCRUB_BATCH_NO': 'Scrub Batch',
                'Status': 'Status'
            }),
            use_container_width=True,
            hide_index=True
        )
        
        if pending_df is not None and not pending_df.empty:
            st.markdown("#### ⚠️ Batches with Pending Records")
            st.dataframe(
                pending_df.rename(columns={
                    'PARTNER_NAME': 'Partner Name',
                    'BATCH_NO': 'Batch No',
                    'RECEIVED_DATE': 'Received Date',
                    'PENDING_RECORDS': 'Pending Records',
                    'LAST_SCRUB_BATCH': 'Last Scrub Batch'
                }),
                use_container_width=True,
                hide_index=True
            )
            
            st.markdown("#### 🚀 Process Pending Records")
            selected_batches = st.multiselect(
                "Select batches to send to Equifax",
                pending_df['BATCH_NO'].tolist(),
                key="eq_then_select"
            )
            
            if selected_batches:
                st.info(f"Selected {len(selected_batches)} batch(es) for processing - Will be sent as a single combined batch")
                
                if st.button("✅ Send to Equifax", type="primary", key="eq_then_send"):
                    with st.spinner(f"Processing {len(selected_batches)} batch(es)..."):
                        result = api_process_equifax(selected_batches, days_threshold=scrub_threshold)
                        
                        if result:
                            st.success(f"✓ {result['message']}: {result['total_records']} total records")
                            for warning in result.get('warnings', []):
                                st.warning(f"⚠ {warning}")
                            invalidate_cache(['summary_equifax', 'pending_equifax', 'experian_tab_data'])
                            st.balloons()
                            st.rerun()
        else:
            st.success("✅ No pending batches for Equifax. All records have been processed!")
    
    with tab2:
        st.subheader("Experian Processing Stage")
        
        col1, col2, col3 = st.columns([2, 2, 1])
        with col1:
            input_score = st.number_input(
                "Credit Score Threshold",
                min_value=300,
                max_value=900,
                value=600,
                step=50,
                help="Only records with Equifax score >= this value will be sent to Experian"
            )
        with col2:
            tab2_scrub_threshold = st.selectbox(
                "Equifax Scrub Date",
                options=[60, 90, 120],
                index=1,
                format_func=lambda x: f"Last {x} days",
                key="exp_tab2_threshold"
            )
        with col3:
            st.metric("Threshold", input_score)
        
        if 'show_eq_then_exp' not in st.session_state:
            st.session_state['show_eq_then_exp'] = False

        if st.button("🔄 Load Data", key="show_eq_then_tab2"):
            st.session_state['show_eq_then_exp'] = True

        if not st.session_state['show_eq_then_exp']:
            return

        exp_df = load_experian_tab_cached(input_score, days_threshold=tab2_scrub_threshold)
        
        if exp_df is None or exp_df.empty:
            st.info("No data available for Experian processing")
            return
        
        # Determine status for each scrub batch
        def determine_exp_status(r):
            status_result = api_get_experian_status(r['SCRUB_BATCH_NO'])
            if status_result and status_result.get('has_response'):
                return "Response Received from Equifax"
            
            if r['EXPERIAN_RESPONSE'] > 0:
                return "Response Received from Experian"
            elif r['SENT_TO_EXPERIAN'] > 0:
                return "Sent to Experian"
            else:
                return "Pending"
        
        exp_df['Status'] = exp_df.apply(determine_exp_status, axis=1)
        
        st.markdown("#### 📋 Scrub Batches Overview")
        display_cols = ['SCRUB_BATCH_NO', 'SCRUB_DATE', 'EQUIFAX_RESPONSE_DATE', 
                       'SENT_TO_EQUIFAX', 'EQUIFAX_HITS', 'NO_HITS', 'SCORE_GTE_INPUT', 
                       'SENT_TO_EXPERIAN', 'EXPERIAN_RESPONSE', 'Status']
        st.dataframe(
            exp_df[display_cols].rename(columns={
                'SCRUB_BATCH_NO': 'Scrub Batch',
                'SCRUB_DATE': 'Sent Date',
                'EQUIFAX_RESPONSE_DATE': 'Equifax Response Date',
                'SENT_TO_EQUIFAX': 'Sent to Equifax',
                'EQUIFAX_HITS': 'Equifax Hits',
                'NO_HITS': 'No Hits',
                'SCORE_GTE_INPUT': f'Score ≥ {input_score}',
                'SENT_TO_EXPERIAN': 'Sent to Experian',
                'EXPERIAN_RESPONSE': 'Experian Responses',
                'Status': 'Status'
            }),
            use_container_width=True,
            hide_index=True
        )
        
        ready_batches = exp_df[
            (exp_df['Status'] == 'Response Received from Equifax') |
            ((exp_df['Status'] == 'Pending') & (exp_df['EQUIFAX_HITS'] > 0))
        ]
        
        if not ready_batches.empty:
            st.markdown("#### 🚀 Send to Experian")
            selected_scrub_batches = st.multiselect(
                "Select scrub batches to send to Experian",
                ready_batches['SCRUB_BATCH_NO'].tolist(),
                key="exp_scrub_select"
            )
            
            if selected_scrub_batches:
                st.info(f"Selected {len(selected_scrub_batches)} scrub batch(es) for Experian processing")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("📤 Send All Data", type="secondary", key="exp_send_all"):
                        with st.spinner("Sending all data to Experian..."):
                            result = api_send_to_experian(selected_scrub_batches, send_type="all")
                            
                            if result:
                                for batch_info in result.get('success_batches', []):
                                    st.success(f"✓ Scrub Batch {batch_info['batch']}: Sent to Experian")
                                for batch_info in result.get('failed_batches', []):
                                    st.error(f"✗ Scrub Batch {batch_info['batch']}: {batch_info['error']}")
                                
                                if result['success_count'] > 0:
                                    invalidate_cache(['summary_experian', 'experian_tab_data'])
                                    st.balloons()
                                    st.rerun()
                
                with col2:
                    if st.button(f"🎯 Send Score ≥ {input_score}", type="primary", key="exp_send_filtered"):
                        with st.spinner(f"Sending records with score ≥ {input_score} to Experian..."):
                            result = api_send_to_experian(
                                selected_scrub_batches, 
                                send_type="filtered", 
                                score_threshold=input_score
                            )
                            
                            if result:
                                for batch_info in result.get('success_batches', []):
                                    st.success(f"✓ Scrub Batch {batch_info['batch']}: Filtered records sent to Experian")
                                for batch_info in result.get('failed_batches', []):
                                    st.error(f"✗ Scrub Batch {batch_info['batch']}: {batch_info['error']}")
                                
                                if result['success_count'] > 0:
                                    invalidate_cache(['summary_experian', 'experian_tab_data'])
                                    st.balloons()
                                    st.rerun()
        else:
            st.info("ℹ️ No scrub batches ready for Experian processing. Wait for Equifax responses.")

# ==================== MAIN APP ====================

def check_api_connection():
    """Check if API is reachable"""
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=5)
        return response.status_code == 200
    except:
        return False

def main():
    st.sidebar.title("🏢 Bureau Processing System")
    st.sidebar.markdown("---")
    
    # Check API connection
    if not check_api_connection():
        st.error("⚠️ Cannot connect to API server. Please ensure the API is running at " + API_BASE_URL)
        st.info("Start the API with: `python api.py`")
        st.stop()
    
    st.sidebar.success("✅ API Connected")
    st.sidebar.markdown("---")
    
    case = st.sidebar.radio(
        "**Choose Processing Flow**",
        ["Equifax Only", "Experian Only", "Equifax → Experian"],
        index=2,
        help="Select the bureau processing workflow"
    )
    
    st.sidebar.markdown("---")
    
    if st.sidebar.button("🔄 Refresh All Data", use_container_width=True):
        invalidate_cache(['summary_equifax', 'summary_experian', 'pending_equifax', 
                         'pending_experian', 'experian_tab_data'])
        st.cache_data.clear()
        st.success("✅ Cache cleared!")
        st.rerun()
    
    st.sidebar.markdown("---")
    
    if case == "Equifax Only":
        render_equifax_only()
    elif case == "Experian Only":
        render_experian_only()
    else:
        render_equifax_then_experian()

if __name__ == "__main__":
    main()