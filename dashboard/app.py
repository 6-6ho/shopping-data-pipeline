import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
import time
import os

# --- Configurations ---
# Database Connection (Assuming Postgres is accessible)
# In Docker: host='postgres'
# In Local Dev: host='localhost'
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_USER = os.getenv("POSTGRES_USER", "airflow")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "airflow")
DB_NAME = os.getenv("POSTGRES_DB", "airflow")
DB_PORT = os.getenv("DB_PORT", "5432")

# Page Config
st.set_page_config(
    page_title="Shopping Analytics",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Database Connection ---
@st.cache_resource
def get_db_connection():
    db_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url)

# --- Data Loading ---
def load_cohort_data(engine):
    try:
        # Assuming we will create this table in Spark Job
        query = "SELECT * FROM analytics_cohort_retention ORDER BY cohort_week DESC"
        return pd.read_sql(query, engine)
    except Exception as e:
        return pd.DataFrame() # Return empty if table doesn't exist yet

def load_anomaly_alerts(engine):
    try:
        query = "SELECT * FROM analytics_anomaly_alerts ORDER BY detection_time DESC LIMIT 100"
        return pd.read_sql(query, engine)
    except Exception as e:
        return pd.DataFrame()

# --- UI Layout ---
st.title("🛍️ Shopping User Analytics")

st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)

# Sidebar
st.sidebar.header("Filter Options")
refresh_interval = st.sidebar.slider("Refresh Interval (s)", 5, 60, 30)

# Main Content
tab1, tab2, tab3 = st.tabs(["📊 Dashboard", "👥 Cohort Analysis", "🚨 Anomaly Detection"])

engine = get_db_connection()

with tab1:
    st.subheader("Real-time Overview")
    
    col1, col2, col3 = st.columns(3)
    
    # Placeholder metrics (In real app, query these)
    with col1:
        st.metric("Active Users (Real-time)", "1,234", "+15%")
    with col2:
        st.metric("Total Orders (Today)", "450", "+5%")
    with col3:
        st.metric("Conversion Rate", "3.2%", "-0.1%")
        
    st.divider()
    
    # Check if we have anomaly data for a quickly accessible chart
    alerts_df = load_anomaly_alerts(engine)
    if not alerts_df.empty:
        st.subheader("Recent Alert Trends")
        alert_counts = alerts_df.groupby("anomaly_type").size().reset_index(name="count")
        fig = px.bar(alert_counts, x="anomaly_type", y="count", color="anomaly_type", title="Anomalies by Type")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No anomaly data available yet.")

with tab2:
    st.header("Cohort Retention Analysis")
    cohort_df = load_cohort_data(engine)
    
    if not cohort_df.empty:
        # Pivot for Heatmap
        # Assuming columns: cohort_week, period_week, retention_rate
        heatmap_data = cohort_df.pivot(index="cohort_week", columns="period_week", values="retention_rate")
        
        fig = px.imshow(
            heatmap_data,
            labels=dict(x="Weeks Since First Visit", y="Cohort Week", color="Retention Rate"),
            x=heatmap_data.columns,
            y=heatmap_data.index,
            text_auto='.1%',
            color_continuous_scale="Blues"
        )
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(cohort_df)
    else:
        st.warning("Cohort data not found. Run the batch job first.")

with tab3:
    st.header("Real-time Anomaly Alerts")
    
    if not alerts_df.empty:
        # Highlight high severity
        st.dataframe(
            alerts_df.style.apply(
                lambda x: ['background-color: #ffcdd2' if v == 'CRITICAL' else '' for v in x], 
                subset=['severity']
            ),
            use_container_width=True
        )
    else:
        st.info("No anomalies detected. System is running normally.")

# Auto-refresh logic (using st.empty() or rerun workaround if needed, 
# typically users manually refresh or use st.status in newer Streamlit)
if st.button("Refresh Data"):
    st.rerun()
