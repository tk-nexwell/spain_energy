"""
Centralized styling configuration for Streamlit app.
"""
import streamlit as st

# Brand color: RGB(67, 150, 68) = #439644
BRAND_COLOR = "#439644"

def apply_brand_styling():
    """Apply brand color styling to Streamlit widgets via CSS only.
    
    This function injects pure CSS without creating any visible HTML elements.
    The CSS is scoped to slider components only to avoid unintended side effects.
    """
    # Inject CSS using st.markdown with unsafe_allow_html=True
    # Only <style> tags are used - no visible HTML elements should be created
    # The CSS is scoped to .stSlider to avoid affecting sidebar structure
    # All selectors are specific to slider components to prevent unintended side effects
    st.markdown(
        f"""<style>
        /* Slider styling - only thumb and active track, no background rectangles */
        /* Remove any background styling that creates rectangles */
        .stSlider [data-baseweb="slider"] {{
            background-color: transparent !important;
        }}
        .stSlider > div > div > div {{
            background-color: transparent !important;
        }}
        /* Thumb (the draggable button at extremes) */
        .stSlider [data-baseweb="slider"] [data-baseweb="thumb"] {{
            background-color: {BRAND_COLOR} !important;
            border-color: {BRAND_COLOR} !important;
        }}
        .stSlider > div > div > div[data-testid="stSliderThumb"] {{
            background-color: {BRAND_COLOR} !important;
            border-color: {BRAND_COLOR} !important;
        }}
        /* Active track portion (the thin line between values) */
        .stSlider [data-baseweb="slider"] [data-baseweb="track"] > div[data-index="1"] {{
            background-color: {BRAND_COLOR} !important;
        }}
        </style>""",
        unsafe_allow_html=True
    )

