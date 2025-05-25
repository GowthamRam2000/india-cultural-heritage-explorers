import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans


class DataProcessor:
    def __init__(self):
        self.scaler = StandardScaler()

    def calculate_heritage_index(self, df_arts, df_sites, df_festivals):
        state_heritage = {}

        for state in df_arts['state'].unique():
            art_score = len(df_arts[df_arts['state'] == state]) * 0.3
            site_score = len(df_sites[df_sites['state'] == state]) * 0.4
            festival_score = len(df_festivals[df_festivals['state'] == state]) * 0.3

            heritage_index = (art_score + site_score + festival_score) / 3
            state_heritage[state] = min(heritage_index, 100)

        return pd.DataFrame(list(state_heritage.items()), columns=['state', 'heritage_index'])

    def identify_tourism_patterns(self, df_tourism):
        df_tourism['total_visitors'] = df_tourism['domestic_visitors'] + df_tourism['international_visitors']
        df_tourism['year'] = df_tourism['date'].dt.year
        df_tourism['month'] = df_tourism['date'].dt.month

        seasonal_patterns = df_tourism.groupby('month')['total_visitors'].mean().reset_index()
        seasonal_patterns['season'] = seasonal_patterns['month'].apply(self._get_season)

        growth_trends = df_tourism.groupby(['site', 'year'])['total_visitors'].sum().reset_index()
        growth_trends['yoy_growth'] = growth_trends.groupby('site')['total_visitors'].pct_change()

        return seasonal_patterns, growth_trends

    def _get_season(self, month):
        if month in [12, 1, 2]:
            return 'Winter'
        elif month in [3, 4, 5]:
            return 'Spring'
        elif month in [6, 7, 8]:
            return 'Monsoon'
        else:
            return 'Autumn'

    def find_hidden_gems(self, df_sites, df_tourism):
        site_visits = df_tourism.groupby('site')['total_visitors'].sum().reset_index()
        site_visits['visit_rank'] = site_visits['total_visitors'].rank(method='dense', ascending=False)

        # Merge with site data to get actual site names
        site_data = pd.merge(df_sites, site_visits, left_on='site_name', right_on='site', how='left')

        # Find sites with good potential but low utilization
        hidden_gems = site_data[
            (site_data['conservation_status'].isin(['Excellent', 'Good'])) &
            (site_data['accessibility_score'] > 0.6) &
            (site_data['current_utilization'] < 0.5) &
            (site_data['site_name'].notna())
            ].copy()

        hidden_gems['potential_score'] = (
                hidden_gems['accessibility_score'] * 0.3 +
                (1 - hidden_gems['current_utilization']) * 0.4 +
                hidden_gems['digital_presence_score'] * 0.3
        )

        # Sort by potential score and return top 10 with meaningful names
        return hidden_gems.nlargest(10, 'potential_score')[['site_name', 'state', 'type', 'unesco_status',
                                                            'conservation_status', 'accessibility_score',
                                                            'current_utilization', 'potential_score']]

    def cluster_cultural_sites(self, df_sites):
        features = ['annual_maintenance_cost', 'visitor_capacity', 'accessibility_score', 'digital_presence_score']
        X = df_sites[features].fillna(0)
        X_scaled = self.scaler.fit_transform(X)

        kmeans = KMeans(n_clusters=4, random_state=42)
        df_sites['cluster'] = kmeans.fit_predict(X_scaled)

        cluster_names = {
            0: 'Premium Heritage Sites',
            1: 'Emerging Cultural Destinations',
            2: 'Traditional Local Sites',
            3: 'Underutilized Heritage'
        }

        df_sites['cluster_name'] = df_sites['cluster'].map(cluster_names)

        return df_sites

    def calculate_sustainability_metrics(self, df_tourism, df_sites):
        sustainability_metrics = df_tourism.groupby('site').agg({
            'sustainability_score': 'mean',
            'crowding_index': 'mean',
            'revenue': 'sum',
            'total_visitors': 'sum'
        }).reset_index()

        sustainability_metrics['revenue_per_visitor'] = (
                sustainability_metrics['revenue'] / sustainability_metrics['total_visitors']
        )

        sustainability_metrics['overall_sustainability'] = (
                sustainability_metrics['sustainability_score'] * 0.4 +
                (1 - sustainability_metrics['crowding_index']) * 0.3 +
                np.clip(sustainability_metrics['revenue_per_visitor'] / 200, 0, 1) * 0.3
        )

        return sustainability_metrics

    def recommend_cultural_routes(self, df_sites, df_arts):
        routes = []

        regions = {
            'North': ['Uttarakhand', 'Himachal Pradesh', 'Punjab', 'Haryana', 'Uttar Pradesh'],
            'South': ['Kerala', 'Tamil Nadu', 'Karnataka', 'Andhra Pradesh', 'Telangana'],
            'East': ['West Bengal', 'Odisha', 'Jharkhand', 'Bihar', 'Assam'],
            'West': ['Rajasthan', 'Gujarat', 'Maharashtra', 'Goa'],
            'Northeast': ['Assam', 'Meghalaya', 'Manipur', 'Mizoram', 'Nagaland', 'Tripura', 'Arunachal Pradesh',
                          'Sikkim']
        }

        for region, states in regions.items():
            region_sites = df_sites[df_sites['state'].isin(states)]
            region_arts = df_arts[df_arts['state'].isin(states)]

            if len(region_sites) >= 3:
                top_sites = region_sites.nlargest(5, 'accessibility_score')
                unique_arts = region_arts['art_form'].unique()[:3]

                routes.append({
                    'region': region,
                    'route_name': f'{region} Cultural Circuit',
                    'duration_days': len(states),
                    'key_sites': top_sites['site_name'].tolist()[:3],
                    'art_forms': list(unique_arts),
                    'best_season': 'October to March',
                    'difficulty': 'Moderate'
                })

        return pd.DataFrame(routes)