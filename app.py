import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import plotly.express as px
from streamlit_folium import st_folium

from config import APP_CONFIG
from data.data_loader import DataLoader
from data.data_processor import DataProcessor
from components.maps import MapVisualizer
from components.analytics import AnalyticsVisualizer
from components.recommendations import RecommendationEngine
from utils.helpers import *

st.set_page_config(
    page_title=APP_CONFIG['title'],
    page_icon=APP_CONFIG['page_icon'],
    layout=APP_CONFIG['layout'],
    initial_sidebar_state=APP_CONFIG['initial_sidebar_state']
)

apply_custom_css()


@st.cache_data
def load_all_data(use_snowflake=False):
    loader = DataLoader(use_snowflake=use_snowflake)

    df_arts = loader.load_art_forms_data()
    df_tourism = loader.load_tourism_data()
    df_sites = loader.load_cultural_sites_data()
    df_festivals = loader.load_festival_data()

    state_coords = get_state_coordinates()

    # Ensure all dataframes have required columns
    if 'latitude' not in df_arts.columns or 'longitude' not in df_arts.columns:
        for df in [df_arts, df_sites]:
            if 'state' in df.columns:
                df['latitude'] = df['state'].map(
                    lambda x: state_coords.get(x, [20.5937, 78.9629])[0] + np.random.uniform(-1, 1))
                df['longitude'] = df['state'].map(
                    lambda x: state_coords.get(x, [20.5937, 78.9629])[1] + np.random.uniform(-1, 1))

    if 'latitude' not in df_tourism.columns or 'longitude' not in df_tourism.columns:
        if 'state' in df_tourism.columns:
            df_tourism['latitude'] = df_tourism['state'].map(lambda x: state_coords.get(x, [20.5937, 78.9629])[0])
            df_tourism['longitude'] = df_tourism['state'].map(lambda x: state_coords.get(x, [20.5937, 78.9629])[1])

    if 'total_visitors' not in df_tourism.columns:
        df_tourism['total_visitors'] = df_tourism['domestic_visitors'] + df_tourism['international_visitors']

    processor = DataProcessor()
    map_viz = MapVisualizer()
    analytics_viz = AnalyticsVisualizer()
    recommender = RecommendationEngine()

    return df_arts, df_tourism, df_sites, df_festivals


def main():
    st.markdown('<h1 class="main-header">🎭 India Cultural Heritage Explorer</h1>', unsafe_allow_html=True)
    st.markdown(
        '<p style="text-align: center; font-size: 1.2rem;">Discover, Analyze, and Preserve India\'s Rich Cultural Heritage</p>',
        unsafe_allow_html=True)

    with st.sidebar:
        try:
            st.image(
                "/Users/gowthamram/PycharmProjects/india-cultural-heritage-explorer/assets/peacock.svg",
                width=150, caption="India Cultural Heritage")
        except:
            st.markdown("### 🇮🇳 India Cultural Heritage")

        st.markdown("### 🎯 Navigation")

        page = st.radio(
            "Select Page",
            ["🏠 Dashboard", "🗺️ Interactive Maps", "📊 Analytics", "🎯 Recommendations", "📈 Insights"]
        )

        st.markdown("---")
        use_snowflake = st.checkbox("Use Snowflake Data", value=False)

        st.markdown("---")
        st.markdown("### 📊 Quick Stats")

    df_arts, df_tourism, df_sites, df_festivals = load_all_data(use_snowflake)

    state_coords = get_state_coordinates()

    # Ensure all dataframes have required columns
    if 'latitude' not in df_arts.columns or 'longitude' not in df_arts.columns:
        for df in [df_arts, df_sites]:
            if 'state' in df.columns:
                df['latitude'] = df['state'].map(
                    lambda x: state_coords.get(x, [20.5937, 78.9629])[0] + np.random.uniform(-1, 1))
                df['longitude'] = df['state'].map(
                    lambda x: state_coords.get(x, [20.5937, 78.9629])[1] + np.random.uniform(-1, 1))

    if 'latitude' not in df_tourism.columns or 'longitude' not in df_tourism.columns:
        if 'state' in df_tourism.columns:
            df_tourism['latitude'] = df_tourism['state'].map(lambda x: state_coords.get(x, [20.5937, 78.9629])[0])
            df_tourism['longitude'] = df_tourism['state'].map(lambda x: state_coords.get(x, [20.5937, 78.9629])[1])

    if 'total_visitors' not in df_tourism.columns:
        df_tourism['total_visitors'] = df_tourism['domestic_visitors'] + df_tourism['international_visitors']

    processor = DataProcessor()
    map_viz = MapVisualizer()
    analytics_viz = AnalyticsVisualizer()
    recommender = RecommendationEngine()

    with st.sidebar:
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Art Forms", len(df_arts))
            st.metric("Heritage Sites", len(df_sites))
        with col2:
            st.metric("Annual Visitors", f"{format_number(df_tourism['total_visitors'].sum())}")
            st.metric("Festivals",
                      len(df_festivals['festival'].unique()) if 'festival' in df_festivals.columns else len(
                          df_festivals))

    if page == "🏠 Dashboard":
        show_dashboard(df_arts, df_tourism, df_sites, df_festivals, processor, analytics_viz)

    elif page == "🗺️ Interactive Maps":
        show_maps(df_arts, df_tourism, df_sites, processor, map_viz)

    elif page == "📊 Analytics":
        show_analytics(df_arts, df_tourism, df_sites, df_festivals, processor, analytics_viz)

    elif page == "🎯 Recommendations":
        show_recommendations(df_arts, df_sites, df_tourism, processor, recommender, analytics_viz)

    elif page == "📈 Insights":
        show_insights(df_arts, df_tourism, df_sites, df_festivals, processor)


def show_dashboard(df_arts, df_tourism, df_sites, df_festivals, processor, analytics_viz):
    st.markdown('<h2 class="sub-header">Welcome to India\'s Cultural Heritage Platform</h2>', unsafe_allow_html=True)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        endangered_count = len(df_arts[df_arts['risk_level'] == 'Endangered'])
        st.markdown(f"""
        <div class="metric-card">
            <h3 style="color: #e74c3c;">🚨 {endangered_count}</h3>
            <p>Endangered Art Forms</p>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        unesco_count = len(df_sites[df_sites['unesco_status'] == 'Inscribed'])
        st.markdown(f"""
        <div class="metric-card">
            <h3 style="color: #f39c12;">🏛️ {unesco_count}</h3>
            <p>UNESCO Sites</p>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        avg_sustainability = df_tourism.groupby('site')['sustainability_score'].mean().mean()
        st.markdown(f"""
        <div class="metric-card">
            <h3 style="color: #27ae60;">🌿 {avg_sustainability:.2f}</h3>
            <p>Avg Sustainability Score</p>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        total_revenue = df_tourism['revenue'].sum()
        st.markdown(f"""
        <div class="metric-card">
            <h3 style="color: #3498db;">💰 {format_currency(total_revenue)}</h3>
            <p>Total Tourism Revenue</p>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.plotly_chart(analytics_viz.create_art_forms_distribution(df_arts), use_container_width=True)

    with col2:
        st.plotly_chart(analytics_viz.create_risk_assessment_chart(df_arts), use_container_width=True)

    heritage_index = processor.calculate_heritage_index(df_arts, df_sites, df_festivals)
    st.plotly_chart(analytics_viz.create_heritage_index_chart(heritage_index), use_container_width=True)


def show_maps(df_arts, df_tourism, df_sites, processor, map_viz):
    st.markdown('<h2 class="sub-header">🗺️ Interactive Cultural Maps</h2>', unsafe_allow_html=True)

    map_type = st.selectbox(
        "Select Map View",
        ["Traditional Art Forms", "Heritage Sites", "Tourism Heatmap", "Cultural Routes"]
    )

    if map_type == "Traditional Art Forms":
        create_info_box(
            "Traditional Art Forms Map",
            "Explore the distribution of traditional art forms across India. Colors indicate risk levels: 🟢 Safe, 🟠 Vulnerable, 🔴 Endangered",
            "#1f77b4"
        )

        category_filter = st.multiselect(
            "Filter by Category",
            df_arts['category'].unique(),
            default=df_arts['category'].unique()
        )

        filtered_arts = df_arts[df_arts['category'].isin(category_filter)]
        m = map_viz.create_art_forms_map(filtered_arts)
        st_folium(m, height=600, width=1000)

    elif map_type == "Heritage Sites":
        create_info_box(
            "Heritage Sites Map",
            "Discover UNESCO World Heritage Sites and other cultural landmarks. Gold markers indicate UNESCO inscribed sites.",
            "#f39c12"
        )

        unesco_filter = st.selectbox(
            "Filter by UNESCO Status",
            ["All", "Inscribed", "Tentative", "None"]
        )

        if unesco_filter != "All":
            filtered_sites = df_sites[df_sites['unesco_status'] == unesco_filter]
        else:
            filtered_sites = df_sites

        m = map_viz.create_heritage_sites_map(filtered_sites)
        st_folium(m, height=600, width=1000)

    elif map_type == "Tourism Heatmap":
        create_info_box(
            "Tourism Density Heatmap",
            "Visualize tourism concentration across cultural sites. Warmer colors indicate higher visitor density.",
            "#e74c3c"
        )

        m = map_viz.create_tourism_heatmap(df_tourism)
        st_folium(m, height=600, width=1000)

    else:
        create_info_box(
            "Recommended Cultural Routes",
            "Explore curated cultural circuits across different regions of India.",
            "#27ae60"
        )

        routes = processor.recommend_cultural_routes(df_sites, df_arts)
        m = map_viz.create_cultural_route_map(routes, df_sites)
        st_folium(m, height=600, width=1000)


def show_analytics(df_arts, df_tourism, df_sites, df_festivals, processor, analytics_viz):
    st.markdown('<h2 class="sub-header">📊 Cultural Heritage Analytics</h2>', unsafe_allow_html=True)

    tab1, tab2, tab3, tab4 = st.tabs(["Tourism Trends", "Sustainability", "Digital Presence", "Festival Impact"])

    with tab1:
        st.plotly_chart(analytics_viz.create_tourism_trends(df_tourism), use_container_width=True)

        seasonal_patterns, growth_trends = processor.identify_tourism_patterns(df_tourism)
        st.plotly_chart(analytics_viz.create_seasonal_patterns(seasonal_patterns), use_container_width=True)

    with tab2:
        sustainability_metrics = processor.calculate_sustainability_metrics(df_tourism, df_sites)
        st.plotly_chart(analytics_viz.create_sustainability_matrix(sustainability_metrics), use_container_width=True)

    with tab3:
        st.plotly_chart(analytics_viz.create_digital_presence_chart(df_sites), use_container_width=True)

    with tab4:
        st.plotly_chart(analytics_viz.create_festival_impact_chart(df_festivals), use_container_width=True)


def show_recommendations(df_arts, df_sites, df_tourism, processor, recommender, analytics_viz):
    st.markdown('<h2 class="sub-header">🎯 Personalized Recommendations</h2>', unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["Plan Your Journey", "Hidden Gems", "Sustainable Tourism"])

    with tab1:
        st.markdown("### 🗺️ Create Your Cultural Journey")

        col1, col2, col3 = st.columns(3)

        with col1:
            duration = st.slider("Trip Duration (days)", 1, 14, 5)
            budget = st.selectbox("Budget Level", ["Budget", "Mid-range", "Luxury"])

        with col2:
            interest = st.selectbox(
                "Primary Interest",
                ["All Heritage Sites", "UNESCO Sites", "Off-beat Locations", "Art & Craft", "Festivals"]
            )
            travel_style = st.selectbox("Travel Style", ["Cultural Immersion", "Photography", "Family", "Adventure"])

        with col3:
            season = st.selectbox("Travel Season", ["Winter", "Spring", "Monsoon", "Autumn"])
            group_size = st.number_input("Group Size", 1, 20, 2)

        user_preferences = {
            'duration': duration,
            'budget': budget,
            'interest': interest,
            'travel_style': travel_style,
            'season': season,
            'group_size': group_size
        }

        if st.button("Generate Itinerary", type="primary"):
            route = recommender.generate_personalized_route(user_preferences, df_sites, df_arts)

            st.markdown("### 🎒 Your Personalized Cultural Journey")

            col1, col2 = st.columns([2, 1])

            with col1:
                st.markdown(f"**Duration:** {route['total_duration']} days")
                st.markdown(f"**Estimated Cost:** {format_currency(route['estimated_cost'])} per person")
                st.markdown(f"**Best Time to Visit:** {route['best_time']}")

                st.markdown("#### 📍 Recommended Sites:")
                for i, site in enumerate(route['sites'], 1):
                    st.markdown(f"""
                    <div class="recommendation-card">
                        <h4>{i}. {site['site_name']}</h4>
                        <p><strong>Location:</strong> {site['state']}</p>
                        <p><strong>Type:</strong> {site['type']}</p>
                        <p><strong>Accessibility:</strong> {'⭐' * int(site['accessibility_score'] * 5)}</p>
                    </div>
                    """, unsafe_allow_html=True)

            with col2:
                st.markdown("#### 🎭 Special Experiences:")
                for exp in route['special_experiences']:
                    st.markdown(f"- {exp}")

    with tab2:
        st.markdown("### 💎 Discover Hidden Gems")

        hidden_gems = processor.find_hidden_gems(df_sites, df_tourism)

        col1, col2 = st.columns([3, 2])

        with col1:
            st.plotly_chart(analytics_viz.create_hidden_gems_radar(hidden_gems), use_container_width=True)

        with col2:
            st.markdown("#### Top Hidden Gems:")
            for _, gem in hidden_gems.head(5).iterrows():
                st.markdown(f"""
                <div class="recommendation-card">
                    <h5>{gem['site_name']}</h5>
                    <p><strong>State:</strong> {gem['state']}</p>
                    <p><strong>Potential Score:</strong> {gem['potential_score']:.2f}</p>
                </div>
                """, unsafe_allow_html=True)

    with tab3:
        st.markdown("### 🌿 Sustainable Tourism Recommendations")

        sustainability_metrics = processor.calculate_sustainability_metrics(df_tourism, df_sites)
        sustainable_sites = recommender.recommend_sustainable_sites(df_sites, sustainability_metrics)

        for _, site in sustainable_sites.head(5).iterrows():
            col1, col2, col3 = st.columns([3, 2, 1])

            with col1:
                st.markdown(f"**{site['site']}** - {site['state']}")
                st.markdown(f"*{site['key_features']}*")

            with col2:
                st.markdown(f"**Sustainability Score:** {site['sustainability_score']:.2f} / 1.0")
                st.markdown(f"💡 {site['visitor_tips']}")

            with col3:
                st.button("Details", key=f"detail_{site['site']}")


def show_insights(df_arts, df_tourism, df_sites, df_festivals, processor):
    st.markdown('<h2 class="sub-header">📈 Data-Driven Insights</h2>', unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["Key Findings", "Trend Analysis", "Action Items"])

    with tab1:
        st.markdown("### 🔍 Key Findings")

        col1, col2 = st.columns(2)

        with col1:
            endangered_arts = df_arts[df_arts['risk_level'] == 'Endangered']
            endangered_by_state = endangered_arts.groupby('state').size().sort_values(ascending=False).head(5)

            st.markdown("#### States with Most Endangered Art Forms")
            fig = px.bar(
                x=endangered_by_state.values,
                y=endangered_by_state.index,
                orientation='h',
                labels={'x': 'Number of Endangered Art Forms', 'y': 'State'},
                color_discrete_sequence=['#e74c3c']
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            low_digital = df_sites.groupby('state')['digital_presence_score'].mean().sort_values().head(5)

            st.markdown("#### States Needing Digital Transformation")
            fig = px.bar(
                x=low_digital.values,
                y=low_digital.index,
                orientation='h',
                labels={'x': 'Digital Presence Score', 'y': 'State'},
                color_discrete_sequence=['#3498db']
            )
            st.plotly_chart(fig, use_container_width=True)

    with tab2:
        st.markdown("### 📊 Trend Analysis")

        seasonal_patterns, growth_trends = processor.identify_tourism_patterns(df_tourism)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### Monthly Visitor Patterns")
            monthly_avg = df_tourism.groupby(df_tourism['date'].dt.month)['total_visitors'].mean()
            fig = px.area(
                x=monthly_avg.index,
                y=monthly_avg.values,
                labels={'x': 'Month', 'y': 'Average Visitors'},
                color_discrete_sequence=['#27ae60']
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("#### Year-over-Year Growth")
            yoy_growth = growth_trends.groupby('year')['yoy_growth'].mean().dropna()
            fig = px.line(
                x=yoy_growth.index,
                y=yoy_growth.values * 100,
                labels={'x': 'Year', 'y': 'Growth Rate (%)'},
                markers=True,
                color_discrete_sequence=['#f39c12']
            )
            st.plotly_chart(fig, use_container_width=True)

    with tab3:
        st.markdown("### 🎯 Recommended Actions")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("""
            <div class="recommendation-card">
                <h4>🚨 Urgent Conservation Needed</h4>
                <ul>
                    <li>Focus on endangered art forms in Maharashtra and Rajasthan</li>
                    <li>Establish practitioner support programs</li>
                    <li>Create digital archives for preservation</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)

            st.markdown("""
            <div class="recommendation-card">
                <h4>🌐 Digital Transformation</h4>
                <ul>
                    <li>Prioritize Northeast states for digital initiatives</li>
                    <li>Develop virtual tour capabilities</li>
                    <li>Create mobile apps for cultural sites</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)

        with col2:
            st.markdown("""
            <div class="recommendation-card">
                <h4>♻️ Sustainable Tourism</h4>
                <ul>
                    <li>Implement visitor caps at overcrowded sites</li>
                    <li>Promote off-season travel incentives</li>
                    <li>Develop eco-friendly infrastructure</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)

            st.markdown("""
            <div class="recommendation-card">
                <h4>📈 Revenue Optimization</h4>
                <ul>
                    <li>Create premium cultural experiences</li>
                    <li>Develop heritage stay programs</li>
                    <li>Partner with local artisan communities</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
