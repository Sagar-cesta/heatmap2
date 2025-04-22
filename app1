import streamlit as st
import pandas as pd
import plotly.express as px
import snowflake.connector
from cryptography.hazmat.primitives import serialization

st.set_page_config(page_title="Kaiser Permanente Cost Analysis Dashboard", layout="wide")

# Sidebar navigation
st.sidebar.title("🔎 Navigation")
section = st.sidebar.radio("Go to:", ["Home", "Heatmap Overview", "Category Analytics", "Negotiated Type Breakdown"])

# Map full state names → 2-letter codes
us_state_abbr = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
    'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
    'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID',
    'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS',
    'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
    'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
    'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
    'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY',
    'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
    'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
    'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT',
    'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV',
    'Wisconsin': 'WI', 'Wyoming': 'WY'
}

# Load and convert private key
private_key_pem = st.secrets["private_key"].encode()
private_key = serialization.load_pem_private_key(private_key_pem, password=None)
private_key_der = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# DB connection function
def get_connection():
    return snowflake.connector.connect(
        user=st.secrets["user"],
        private_key=private_key_der,
        account=st.secrets["account"],
        warehouse=st.secrets["warehouse"],
        database=st.secrets["database"],
        schema="ALL_STATES"
    )

# --- HOME PAGE ---
if section == "Home":
    st.title("🏥 Kaiser Permanente Cost Analysis")
    st.success("✅ Data successfully imported and analyzed.")
    st.markdown("""
        This app provides an interactive breakdown of testosterone-related negotiated rates by:

        - 📍 **State**  
        - 📦 **Drug Category** (Gel, Injection, Patch, etc.)  
        - 💰 **Negotiated Rate Type** (Fixed, Percentage, etc.)

        
        👉 Use the sidebar to explore the full analytics.
    """)
    st.markdown("---")

# --- TOTAL TESTOSTERONE RECORDS ---
elif section == "Heatmap Overview":
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT STATE, COUNT(*) AS ENTRY_COUNT
        FROM ALL_STATE_COMBINED
        WHERE STATE IS NOT NULL
        GROUP BY STATE
    """)
    df = pd.DataFrame(cur.fetchall(), columns=["STATE", "ENTRY_COUNT"])
    df["STATE_CODE"] = df["STATE"].map(us_state_abbr)
    df = df.dropna(subset=["STATE_CODE"])

    st.title("📊 Total Testosterone Records Across the U.S.")
    st.markdown("""
    This heatmap shows how many testosterone-related entries we’ve collected in each U.S. state.
    """)
    fig = px.choropleth(
        df,
        locations="STATE_CODE",
        locationmode="USA-states",
        color="ENTRY_COUNT",
        hover_name="STATE",
        hover_data={"STATE_CODE": False, "ENTRY_COUNT": True},
        scope="usa",
        color_continuous_scale="Turbo",
        title="📍 Total Testosterone-Related Entries by State"
    )
    st.plotly_chart(fig, use_container_width=True)

# --- CATEGORY ANALYTICS ---
elif section == "Category Analytics":
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT STATE, CATEGORY, COUNT(*) AS CATEGORY_COUNT
        FROM ALL_STATE_COMBINED
        WHERE STATE IS NOT NULL AND CATEGORY IS NOT NULL
        GROUP BY STATE, CATEGORY
    """)
    cat_data = pd.DataFrame(cur.fetchall(), columns=["STATE", "CATEGORY", "CATEGORY_COUNT"])

    state_summary = cat_data.groupby("STATE")["CATEGORY_COUNT"].sum().reset_index()
    state_summary["STATE_CODE"] = state_summary["STATE"].map(us_state_abbr)
    state_summary = state_summary.dropna(subset=["STATE_CODE"])

    st.title("📦 Category Analytics - Nationwide View")
    fig = px.choropleth(
        state_summary,
        locations="STATE_CODE",
        locationmode="USA-states",
        color="CATEGORY_COUNT",
        color_continuous_scale="Blues",
        hover_name="STATE",
        hover_data={"STATE_CODE": False, "CATEGORY_COUNT": True},
        scope="usa",
        title="📍 Total Category Subscriptions by State"
    )
    st.plotly_chart(fig, use_container_width=True)

    states = pd.read_sql("SELECT DISTINCT STATE FROM ALL_STATE_COMBINED WHERE STATE IS NOT NULL ORDER BY STATE", conn)
    selected_state = st.selectbox("👇 Select a state to view detailed CATEGORY breakdown:", states["STATE"])

    cur.execute(f"""
        SELECT CATEGORY, COUNT(*) AS CATEGORY_COUNT
        FROM ALL_STATE_COMBINED
        WHERE STATE = '{selected_state}' AND CATEGORY IS NOT NULL
        GROUP BY CATEGORY
        ORDER BY CATEGORY_COUNT DESC
    """)
    category_data = pd.DataFrame(cur.fetchall(), columns=["CATEGORY", "CATEGORY_COUNT"])

    st.markdown(f"📌 **Detailed breakdown for `{selected_state}`**")
    st.dataframe(category_data, use_container_width=True)

# --- NEGOTIATED TYPE BREAKDOWN ---
elif section == "Negotiated Type Breakdown":
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT STATE, CATEGORY, NEGOTIATED_TYPE, COUNT(*) AS TYPE_COUNT
        FROM ALL_STATE_COMBINED
        WHERE STATE IS NOT NULL AND CATEGORY IS NOT NULL AND NEGOTIATED_TYPE IS NOT NULL
        GROUP BY STATE, CATEGORY, NEGOTIATED_TYPE
    """)
    type_df = pd.DataFrame(cur.fetchall(), columns=["STATE", "CATEGORY", "NEGOTIATED_TYPE", "TYPE_COUNT"])

    # Pivot for hover info
    hover_info = type_df.pivot_table(
        index="STATE",
        columns="NEGOTIATED_TYPE",
        values="TYPE_COUNT",
        aggfunc="sum"
    ).fillna(0).astype(int).reset_index()

    # Add total column
    hover_info["TOTAL_NEGOTIATED_TYPE"] = hover_info.drop("STATE", axis=1).sum(axis=1)

    # Map state codes
    hover_info["STATE_CODE"] = hover_info["STATE"].map(us_state_abbr)
    hover_info = hover_info.dropna(subset=["STATE_CODE"])

    # Plot
    st.title("💰 Negotiated Type Breakdown")
    fig = px.choropleth(
        hover_info,
        locations="STATE_CODE",
        locationmode="USA-states",
        color="TOTAL_NEGOTIATED_TYPE",
        scope="usa",
        color_continuous_scale="Purples",
        hover_name="STATE",
        hover_data={
            "STATE_CODE": False,
            "TOTAL_NEGOTIATED_TYPE": True,
            "derived": True if "derived" in hover_info.columns else False,
            "negotiated": True if "negotiated" in hover_info.columns else False,
            "percentage": True if "percentage" in hover_info.columns else False,
            "per diem": True if "per diem" in hover_info.columns else False
        },
        title="📍 Total NEGOTIATED_TYPE Entries by State"
    )
    st.plotly_chart(fig, use_container_width=True)

    # State selector for detailed breakdown
    states = pd.read_sql("SELECT DISTINCT STATE FROM ALL_STATE_COMBINED WHERE STATE IS NOT NULL ORDER BY STATE", conn)
    selected_state = st.selectbox("Select a state:", states["STATE"])

    cur.execute(f"""
        SELECT CATEGORY, NEGOTIATED_TYPE, COUNT(*) AS TYPE_COUNT
        FROM ALL_STATE_COMBINED
        WHERE STATE = '{selected_state}' AND CATEGORY IS NOT NULL AND NEGOTIATED_TYPE IS NOT NULL
        GROUP BY CATEGORY, NEGOTIATED_TYPE
        ORDER BY CATEGORY, NEGOTIATED_TYPE
    """)
    type_breakdown = pd.DataFrame(cur.fetchall(), columns=["CATEGORY", "NEGOTIATED_TYPE", "TYPE_COUNT"])
    type_pivot = type_breakdown.pivot(index="CATEGORY", columns="NEGOTIATED_TYPE", values="TYPE_COUNT").fillna(0).astype(int)

    st.markdown(f"### 🔍 Negotiated Type Breakdown for `{selected_state}`")
    st.dataframe(type_pivot.reset_index(), use_container_width=True)
    st.markdown("""
    - **negotiated**: A fixed, direct amount agreed upon (e.g., $53.25)  
    - **percentage**: A percentage of billed charges (e.g., 80%)  
    - **per diem**: A daily rate (e.g., $500 per day)  
    - **derived**: Estimated from other values  
    """)
