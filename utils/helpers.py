import pandas as pd
import numpy as np
from datetime import datetime
import streamlit as st


def format_number(num):
    if num >= 1_000_000:
        return f"{num / 1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num / 1_000:.1f}K"
    else:
        return str(int(num))


def format_currency(amount):
    return f"₹{amount:,.0f}"


def get_state_coordinates():
    return {
        'Andhra Pradesh': [15.9129, 79.7400],
        'Arunachal Pradesh': [28.2180, 94.7278],
        'Assam': [26.2006, 92.9376],
        'Bihar': [25.0961, 85.3131],
        'Chhattisgarh': [21.2787, 81.8661],
        'Goa': [15.2993, 74.1240],
        'Gujarat': [22.2587, 71.1924],
        'Haryana': [29.0588, 76.0856],
        'Himachal Pradesh': [31.1048, 77.1734],
        'Jharkhand': [23.6102, 85.2799],
        'Karnataka': [15.3173, 75.7139],
        'Kerala': [10.8505, 76.2711],
        'Madhya Pradesh': [22.9734, 78.6569],
        'Maharashtra': [19.7515, 75.7139],
        'Manipur': [24.6637, 93.9063],
        'Meghalaya': [25.4670, 91.3662],
        'Mizoram': [23.1645, 92.9376],
        'Nagaland': [26.1584, 94.5624],
        'Odisha': [20.9517, 85.0985],
        'Punjab': [31.1471, 75.3412],
        'Rajasthan': [27.0238, 74.2179],
        'Sikkim': [27.5330, 88.5122],
        'Tamil Nadu': [11.1271, 78.6569],
        'Telangana': [18.1124, 79.0193],
        'Tripura': [23.9408, 91.9882],
        'Uttar Pradesh': [26.8467, 80.9462],
        'Uttarakhand': [30.0668, 79.0193],
        'West Bengal': [22.9868, 87.8550],
        'Delhi': [28.7041, 77.1025]
    }


def calculate_distance(coord1, coord2):
    from math import radians, sin, cos, sqrt, atan2

    R = 6371

    lat1, lon1 = radians(coord1[0]), radians(coord1[1])
    lat2, lon2 = radians(coord2[0]), radians(coord2[1])

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c


def create_download_link(df, filename):
    csv = df.to_csv(index=False)
    return st.download_button(
        label="Download Data",
        data=csv,
        file_name=filename,
        mime="text/csv"
    )


def apply_custom_css():
    st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }

    .sub-header {
        font-size: 1.5rem;
        color: #ff7f0e;
        margin-bottom: 1rem;
    }

    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        text-align: center;
    }

    .recommendation-card {
        background-color: #e8f4f8;
        padding: 1.5rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }

    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }

    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding-left: 20px;
        padding-right: 20px;
    }
    </style>
    """, unsafe_allow_html=True)


def get_season_emoji(season):
    season_emojis = {
        'Winter': '❄️',
        'Spring': '🌸',
        'Monsoon': '🌧️',
        'Autumn': '🍂'
    }
    return season_emojis.get(season, '🌞')


def validate_data(df, required_columns):
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        st.error(f"Missing required columns: {missing_columns}")
        return False
    return True


def create_info_box(title, content, color="#1f77b4"):
    st.markdown(f"""
    <div style="background-color: {color}20; padding: 1rem; border-radius: 0.5rem; border-left: 4px solid {color};">
        <h4 style="color: {color}; margin: 0;">{title}</h4>
        <p style="margin: 0.5rem 0 0 0;">{content}</p>
    </div>
    """, unsafe_allow_html=True)


def calculate_growth_rate(current, previous):
    if previous == 0:
        return 0
    return ((current - previous) / previous) * 100