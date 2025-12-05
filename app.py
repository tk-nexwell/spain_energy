"""
Main entry point for the Spain Energy Streamlit application.

This file serves as the home page. All other pages are in the pages/ folder.
"""

import streamlit as st

st.set_page_config(
    page_title="Overview",
    page_icon=":chart_with_upwards_trend:",
    layout="wide"
)

# Display logo in sidebar
try:
    st.sidebar.image("NP_logo.svg", use_container_width=True)
except Exception:
    pass  # Logo file not found, continue without it

# Apply brand styling
from style_config import apply_brand_styling
apply_brand_styling()

st.title("Overview")
st.markdown("""
Welcome to the Spain Energy Dashboard!

Navigate to different pages:
""")

# Create clickable links to pages in order
pages = [
    ("Electricity Prices", "pages/01_Electricity_Prices.py"),
    ("Prices Distribution", "pages/02_Price_Distribution.py"),
    ("PV Production", "pages/03_PV_Production.py"),
    ("PV Captured Prices", "pages/04_PV_Captured_Prices.py"),
    ("PV Captured Factor", "pages/05_PV_Captured_Factor.py"),
    ("PPA Effective Prices", "pages/06_PPA_Effective_Price.py"),
    ("BESS Spreads", "pages/07_BESS_Spreads.py"),
]

# Create clickable links to pages using st.page_link
for page_name, page_path in pages:
    st.page_link(page_path, label=page_name)

st.info("💡 **Tip**: Click on any page name above or select a page from the sidebar to get started!")
