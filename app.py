import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import plotly.express as px
from streamlit_folium import st_folium
try:
    from config import APP_CONFIG
    from data.data_loader import DataLoader
    from data.data_processor import DataProcessor
    from components.maps import MapVisualizer
    from components.analytics import AnalyticsVisualizer
    from components.recommendations import RecommendationEngine
    from utils.helpers import *
except ImportError as ie:
    st.error(f"Failed to import a required module: {ie}. "
             f"Please ensure all components (`config.py`, `data/`, `components/`, `utils/`) are present.")
    st.stop()
def format_number(n):
    return f"{n:,}"
def format_currency(n):
    return f"₹{n:,.0f}"
def create_info_box(title, text, color):
    st.info(f"**{title}**: {text}")

def apply_custom_css():
    st.markdown("""
        <style>
        .main-header { text-align: center; color: #1f77b4; }
        .sub-header { color: #ff7f0e; border-bottom: 2px solid #ff7f0e; padding-bottom: 5px; margin-top: 20px;}
        .metric-card {
            background-color: #f9f9f9;
            border-radius: 8px;
            padding: 15px;
            text-align: center;
            box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
            margin-bottom: 10px;
        }
        .recommendation-card {
             border: 1px solid #ddd;
             border-radius: 5px;
             padding: 10px;
             margin-bottom: 10px;
        }
        </style>
    """, unsafe_allow_html=True)

def get_state_coordinates():
    return {
        'Delhi': [28.7041, 77.1025], 'Maharashtra': [19.7515, 75.7139],
        'Rajasthan': [27.0238, 74.2179], 'Uttar Pradesh': [26.8467, 80.9462],
        'Tamil Nadu': [11.1271, 78.6569], 'Karnataka': [15.3173, 75.7139],
        'Default': [20.5937, 78.9629]
    }

st.set_page_config(
    page_title=APP_CONFIG.get('title', "India Cultural Heritage Explorer"),
    page_icon=APP_CONFIG.get('page_icon', "🇮🇳"),
    layout=APP_CONFIG.get('layout', "wide"),
    initial_sidebar_state=APP_CONFIG.get('initial_sidebar_state', "expanded")
)

apply_custom_css()
state_coords = get_state_coordinates()


@st.cache_data
def load_all_data(use_snowflake=False):
    loader = DataLoader(use_snowflake=use_snowflake)
    df_arts = loader.load_art_forms_data()
    df_tourism = loader.load_tourism_data()
    df_sites = loader.load_cultural_sites_data()
    df_festivals = loader.load_festival_data()
    if 'latitude' not in df_arts.columns or 'longitude' not in df_arts.columns:
        if 'state' in df_arts.columns:
            df_arts['latitude'] = df_arts['state'].map(
                lambda x: state_coords.get(x, state_coords['Default'])[0] + np.random.uniform(-0.5, 0.5))
            df_arts['longitude'] = df_arts['state'].map(
                lambda x: state_coords.get(x, state_coords['Default'])[1] + np.random.uniform(-0.5, 0.5))
    if 'latitude' not in df_sites.columns or 'longitude' not in df_sites.columns:
        if 'state' in df_sites.columns:
            df_sites['latitude'] = df_sites['state'].map(
                lambda x: state_coords.get(x, state_coords['Default'])[0] + np.random.uniform(-0.5, 0.5))
            df_sites['longitude'] = df_sites['state'].map(
                lambda x: state_coords.get(x, state_coords['Default'])[1] + np.random.uniform(-0.5, 0.5))
    if 'latitude' not in df_tourism.columns or 'longitude' not in df_tourism.columns:
        if 'state' in df_tourism.columns:
            df_tourism['latitude'] = df_tourism['state'].map(lambda x: state_coords.get(x, state_coords['Default'])[0])
            df_tourism['longitude'] = df_tourism['state'].map(lambda x: state_coords.get(x, state_coords['Default'])[1])

    if 'total_visitors' not in df_tourism.columns and 'domestic_visitors' in df_tourism.columns and 'international_visitors' in df_tourism.columns:
        df_tourism['total_visitors'] = df_tourism['domestic_visitors'] + df_tourism['international_visitors']
    elif 'total_visitors' not in df_tourism.columns:
         df_tourism['total_visitors'] = 0
    if 'digital_presence_score' not in df_sites.columns:
        st.warning("`digital_presence_score` column was missing in `df_sites`. Added random default values.")
        df_sites['digital_presence_score'] = np.random.uniform(0.1, 0.9, len(df_sites))
    if 'risk_level' not in df_arts.columns:
        df_arts['risk_level'] = 'Unknown'
    if 'unesco_status' not in df_sites.columns:
        df_sites['unesco_status'] = 'None'
    if 'sustainability_score' not in df_tourism.columns:
        df_tourism['sustainability_score'] = 0.5
    if 'revenue' not in df_tourism.columns:
        df_tourism['revenue'] = 0
    if 'date' in df_tourism.columns:
        try:
            df_tourism['date'] = pd.to_datetime(df_tourism['date'])
        except Exception:
             st.warning("Could not convert 'date' column to datetime. Trend analysis might fail.")
             df_tourism = df_tourism.drop(columns=['date'])
    else:
        st.warning("Tourism data is missing 'date' column, trend analysis will be unavailable.")
    if 'site_name' not in df_sites.columns and 'site' in df_sites.columns:
         df_sites['site_name'] = df_sites['site']
    elif 'site_name' not in df_sites.columns:
         st.error("`df_sites` is missing 'site_name' or 'site' column. Recommendations might fail.")
         df_sites['site_name'] = 'Unknown Site'

    return df_arts, df_tourism, df_sites, df_festivals

def main():
    st.markdown('<h1 class="main-header">🎭 India Cultural Heritage Explorer</h1>', unsafe_allow_html=True)
    st.markdown(
        '<p style="text-align: center; font-size: 1.2rem;">Discover, Analyze, and Preserve India\'s Rich Cultural Heritage</p>',
        unsafe_allow_html=True)

    with st.sidebar:
        try:
            st.image("./assets/peacock.svg", width=150, caption="India Cultural Heritage")
        except Exception:
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
        avg_sustainability = df_tourism['sustainability_score'].mean() if 'sustainability_score' in df_tourism.columns else 0.0
        st.markdown(f"""
        <div class="metric-card">
            <h3 style="color: #27ae60;">🌿 {avg_sustainability:.2f}</h3>
            <p>Avg Sustainability Score</p>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        total_revenue = df_tourism['revenue'].sum() if 'revenue' in df_tourism.columns else 0
        formatted_revenue = format_currency(total_revenue)
        st.markdown(f"""
        <div class="metric-card">
            <h3 style="color: #3498db;">💰 {formatted_revenue}</h3>
            <p>Total Tourism Revenue</p>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.plotly_chart(analytics_viz.create_art_forms_distribution(df_arts), use_container_width=True)

    with col2:
        st.plotly_chart(analytics_viz.create_risk_assessment_chart(df_arts), use_container_width=True)

    try:
        heritage_index = processor.calculate_heritage_index(df_arts, df_sites, df_festivals)
        st.plotly_chart(analytics_viz.create_heritage_index_chart(heritage_index), use_container_width=True)
    except Exception as e:
        st.warning(f"Could not calculate or display Heritage Index: {e}")


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
        if 'category' in df_arts.columns:
            category_filter = st.multiselect(
                "Filter by Category",
                df_arts['category'].unique(),
                default=df_arts['category'].unique()
            )
            filtered_arts = df_arts[df_arts['category'].isin(category_filter)]
        else:
            filtered_arts = df_arts

        m = map_viz.create_art_forms_map(filtered_arts)
        st_folium(m, height=600, width=1000)

    elif map_type == "Heritage Sites":
        create_info_box(
            "Heritage Sites Map",
            "Discover UNESCO World Heritage Sites and other cultural landmarks. Gold markers indicate UNESCO inscribed sites.",
            "#f39c12"
        )
        if 'unesco_status' in df_sites.columns:
            unesco_filter = st.selectbox(
                "Filter by UNESCO Status",
                ["All", "Inscribed", "Tentative", "None"]
            )
            if unesco_filter != "All":
                filtered_sites = df_sites[df_sites['unesco_status'] == unesco_filter]
            else:
                filtered_sites = df_sites
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
        try:
            routes = processor.recommend_cultural_routes(df_sites, df_arts)
            m = map_viz.create_cultural_route_map(routes, df_sites)
            st_folium(m, height=600, width=1000)
        except Exception as e:
            st.error(f"Could not generate cultural routes: {e}")


def show_analytics(df_arts, df_tourism, df_sites, df_festivals, processor, analytics_viz):
    st.markdown('<h2 class="sub-header">📊 Cultural Heritage Analytics</h2>', unsafe_allow_html=True)

    tab1, tab2, tab3, tab4 = st.tabs(["Tourism Trends", "Sustainability", "Digital Presence", "Festival Impact"])

    with tab1:
        st.plotly_chart(analytics_viz.create_tourism_trends(df_tourism), use_container_width=True)
        if 'date' in df_tourism.columns:
            try:
                seasonal_patterns, growth_trends = processor.identify_tourism_patterns(df_tourism)
                st.plotly_chart(analytics_viz.create_seasonal_patterns(seasonal_patterns), use_container_width=True)
            except Exception as e:
                st.warning(f"Could not display tourism patterns: {e}")
        else:
            st.info("Tourism pattern analysis requires a 'date' column in the tourism data.")

    with tab2:
        try:
            sustainability_metrics = processor.calculate_sustainability_metrics(df_tourism, df_sites)
            st.plotly_chart(analytics_viz.create_sustainability_matrix(sustainability_metrics), use_container_width=True)
        except Exception as e:
            st.warning(f"Could not display sustainability metrics: {e}")

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
            'duration': duration, 'budget': budget, 'interest': interest,
            'travel_style': travel_style, 'season': season, 'group_size': group_size
        }
        if st.button("Generate Itinerary", type="primary"):
            try:
                route = recommender.generate_personalized_route(user_preferences, df_sites, df_arts)
                st.markdown("### 🎒 Your Personalized Cultural Journey")
                col1, col2 = st.columns([2, 1])
                with col1:
                    st.markdown(f"**Duration:** {route.get('total_duration', 'N/A')} days")
                    formatted_cost = format_currency(route.get('estimated_cost', 0))
                    st.markdown(f"**Estimated Cost:** {formatted_cost} per person")
                    st.markdown(f"**Best Time to Visit:** {route.get('best_time', 'N/A')}")
                    st.markdown("#### 📍 Recommended Sites:")
                    for i, site in enumerate(route.get('sites', []), 1):
                        st.markdown(f"""
                        <div class="recommendation-card">
                            <h4>{i}. {site.get('site_name', 'Unknown Site')}</h4>
                            <p><strong>Location:</strong> {site.get('state', 'N/A')}</p>
                            <p><strong>Type:</strong> {site.get('type', 'N/A')}</p>
                            <p><strong>Accessibility:</strong> {'⭐' * int(site.get('accessibility_score', 0) * 5)}</p>
                        </div>
                        """, unsafe_allow_html=True)
                with col2:
                    st.markdown("#### 🎭 Special Experiences:")
                    for exp in route.get('special_experiences', []):
                        st.markdown(f"- {exp}")
            except Exception as e:
                st.error(f"Could not generate itinerary: {e}")
                st.exception(e)

    with tab2:
        st.markdown("### 💎 Discover Hidden Gems")
        try:
            hidden_gems = processor.find_hidden_gems(df_sites, df_tourism)
            if 'digital_presence_score' not in hidden_gems.columns:
                if 'site_name' in hidden_gems.columns and 'site_name' in df_sites.columns:
                    hidden_gems = pd.merge(
                        hidden_gems,
                        df_sites[['site_name', 'digital_presence_score']],
                        on='site_name', how='left'
                    )
                    hidden_gems['digital_presence_score'].fillna(0.5, inplace=True)
                else:
                    hidden_gems['digital_presence_score'] = 0.5
            col1, col2 = st.columns([3, 2])
            with col1:
                if 'digital_presence_score' in hidden_gems.columns:
                    st.plotly_chart(analytics_viz.create_hidden_gems_radar(hidden_gems), use_container_width=True)
                else:
                    st.warning("Could not generate Hidden Gems Radar Chart (missing data).")
            with col2:
                st.markdown("#### Top Hidden Gems:")
                for _, gem in hidden_gems.head(5).iterrows():
                    st.markdown(f"""
                    <div class="recommendation-card">
                        <h5>{gem.get('site_name', 'Unknown')}</h5>
                        <p><strong>State:</strong> {gem.get('state', 'N/A')}</p>
                        <p><strong>Potential Score:</strong> {gem.get('potential_score', 0):.2f}</p>
                    </div>
                    """, unsafe_allow_html=True)
        except Exception as e:
            st.error(f"Could not find or display hidden gems: {e}")
            st.exception(e)

    with tab3:
        st.markdown("### 🌿 Sustainable Tourism Recommendations")
        try:
            sustainability_metrics = processor.calculate_sustainability_metrics(df_tourism, df_sites)
            sustainable_sites = recommender.recommend_sustainable_sites(df_sites, sustainability_metrics)
            if not sustainable_sites.empty:
                st.info(f"Found {len(sustainable_sites)} sustainable sites. Displaying top 5.")
                for _, site in sustainable_sites.head(5).iterrows():
                    col1, col2, col3 = st.columns([3, 2, 1])
                    with col1:
                        st.markdown(f"**{site.get('site', site.get('site_name', 'Unknown'))}** - {site.get('state', 'N/A')}")
                        st.markdown(f"*{site.get('key_features', 'No features listed')}*")
                    with col2:
                        st.markdown(f"**Sustainability Score:** {site.get('sustainability_score', 0):.2f} / 1.0")
                        st.markdown(f"💡 {site.get('visitor_tips', 'Be respectful.')}")
                    with col3:
                        st.button("Details", key=f"detail_{site.get('site', site.get('site_name', 'Unknown'))}")
            else:
                st.warning("No specific sustainable tourism recommendations could be generated based on the current data and criteria.")

        except Exception as e:
            st.error(f"Could not generate sustainable recommendations: {e}")
            st.exception(e)


def show_insights(df_arts, df_tourism, df_sites, df_festivals, processor):
    st.markdown('<h2 class="sub-header">📈 Data-Driven Insights</h2>', unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["Key Findings", "Trend Analysis", "Action Items"])

    with tab1:
        st.markdown("### 🔍 Key Findings")
        col1, col2 = st.columns(2)
        with col1:
            try:
                endangered_arts = df_arts[df_arts['risk_level'] == 'Endangered']
                endangered_by_state = endangered_arts.groupby('state').size().sort_values(ascending=False).head(5)
                fig = px.bar(x=endangered_by_state.values, y=endangered_by_state.index, orientation='h',
                             labels={'x': 'Number of Endangered Art Forms', 'y': 'State'}, color_discrete_sequence=['#e74c3c'])
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                 st.warning(f"Could not display endangered arts chart: {e}")
        with col2:
            try:
                low_digital = df_sites.groupby('state')['digital_presence_score'].mean().sort_values().head(5)
                fig = px.bar(x=low_digital.values, y=low_digital.index, orientation='h',
                             labels={'x': 'Digital Presence Score', 'y': 'State'}, color_discrete_sequence=['#3498db'])
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                 st.warning(f"Could not display digital presence chart: {e}")

    with tab2:
        st.markdown("### 📊 Trend Analysis")
        if 'date' in df_tourism.columns and pd.api.types.is_datetime64_any_dtype(df_tourism['date']):
            try:
                seasonal_patterns, growth_trends = processor.identify_tourism_patterns(df_tourism)
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("#### Monthly Visitor Patterns")
                    monthly_avg = df_tourism.groupby(df_tourism['date'].dt.month)['total_visitors'].mean()
                    fig = px.area(x=monthly_avg.index, y=monthly_avg.values,
                                  labels={'x': 'Month', 'y': 'Average Visitors'}, color_discrete_sequence=['#27ae60'])
                    st.plotly_chart(fig, use_container_width=True)
                with col2:
                    st.markdown("#### Year-over-Year Growth")
                    yoy_growth = growth_trends.groupby('year')['yoy_growth'].mean().dropna()
                    fig = px.line(x=yoy_growth.index, y=yoy_growth.values * 100,
                                  labels={'x': 'Year', 'y': 'Growth Rate (%)'}, markers=True, color_discrete_sequence=['#f39c12'])
                    st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Could not perform trend analysis: {e}")
        else:
            st.warning("Cannot perform trend analysis - 'date' column missing or not in datetime format in tourism data.")

    with tab3:
        st.markdown("### 🎯 Recommended Actions")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("""
            <div class="recommendation-card"><h4>🚨 Urgent Conservation Needed</h4><ul>
            <li>Focus on endangered art forms in key states</li>
            <li>Establish practitioner support programs</li><li>Create digital archives</li></ul></div>
            <div class="recommendation-card"><h4>🌐 Digital Transformation</h4><ul>
            <li>Prioritize states with low digital scores</li>
            <li>Develop virtual tour capabilities</li><li>Create mobile apps</li></ul></div>
            """, unsafe_allow_html=True)
        with col2:
            st.markdown("""
            <div class="recommendation-card"><h4>♻️ Sustainable Tourism</h4><ul>
            <li>Implement visitor caps at overcrowded sites</li>
            <li>Promote off-season travel incentives</li><li>Develop eco-friendly infrastructure</li></ul></div>
            <div class="recommendation-card"><h4>📈 Revenue Optimization</h4><ul>
            <li>Create premium cultural experiences</li><li>Develop heritage stay programs</li>
            <li>Partner with local artisans</li></ul></div>
            """, unsafe_allow_html=True)


if __name__ == "__main__":
    try:
        main()
    except NameError as ne:
        st.error(f"A required component or helper function is missing: {ne}. "
                 f"Please ensure `config.py`, `data_loader.py`, `data_processor.py`, "
                 f"`components/*.py`, and `utils/helpers.py` are present and correctly defined.")
    except FileNotFoundError as fnfe:
         st.error(f"A data file or asset could not be found: {fnfe}. "
                  f"Please ensure all data files and assets are in the correct paths.")
    except Exception as e:
        st.error(f"An unexpected error occurred during execution:")
        st.exception(e)
