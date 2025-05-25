import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler


class RecommendationEngine:
    def __init__(self):
        self.scaler = StandardScaler()

    def generate_personalized_route(self, user_preferences, df_sites, df_arts):
        filtered_sites = df_sites.copy()

        if user_preferences['duration'] <= 3:
            filtered_sites = filtered_sites[filtered_sites['accessibility_score'] > 0.7]

        if user_preferences['interest'] == 'UNESCO Sites':
            filtered_sites = filtered_sites[filtered_sites['unesco_status'] == 'Inscribed']
        elif user_preferences['interest'] == 'Off-beat Locations':
            filtered_sites = filtered_sites[filtered_sites['current_utilization'] < 0.5]

        if user_preferences['budget'] == 'Budget':
            filtered_sites = filtered_sites[filtered_sites['annual_maintenance_cost'] < 1000000]

        selected_sites = filtered_sites.sample(min(user_preferences['duration'], len(filtered_sites)))

        route = {
            'sites': selected_sites[['site_name', 'state', 'type', 'accessibility_score']].to_dict('records'),
            'total_duration': user_preferences['duration'],
            'estimated_cost': self._calculate_route_cost(selected_sites, user_preferences),
            'best_time': self._get_best_travel_time(
                selected_sites['state'].iloc[0] if len(selected_sites) > 0 else 'Delhi'),
            'special_experiences': self._get_special_experiences(selected_sites, df_arts)
        }

        return route

    def _calculate_route_cost(self, sites, preferences):
        base_cost = len(sites) * 2000

        if preferences['budget'] == 'Luxury':
            base_cost *= 3
        elif preferences['budget'] == 'Mid-range':
            base_cost *= 1.5

        return base_cost

    def _get_best_travel_time(self, state):
        climate_zones = {
            'North': 'September to March',
            'South': 'October to February',
            'East': 'October to March',
            'West': 'October to March',
            'Northeast': 'October to April'
        }

        if state in ['Rajasthan', 'Gujarat', 'Maharashtra', 'Goa']:
            return climate_zones['West']
        elif state in ['Kerala', 'Tamil Nadu', 'Karnataka', 'Andhra Pradesh']:
            return climate_zones['South']
        else:
            return climate_zones['North']

    def _get_special_experiences(self, sites, df_arts):
        experiences = []
        states = sites['state'].unique()

        for state in states:
            state_arts = df_arts[df_arts['state'] == state]
            if len(state_arts) > 0:
                unique_art = state_arts.sample(1).iloc[0]
                experiences.append(f"{unique_art['art_form']} performance in {state}")

        return experiences[:3]

    def recommend_sustainable_sites(self, df_sites, sustainability_metrics):
        sustainable_sites = pd.merge(
            df_sites,
            sustainability_metrics[['site', 'overall_sustainability']],
            left_on='site_name',
            right_on='site',
            how='left'
        )

        sustainable_sites = sustainable_sites.dropna(subset=['overall_sustainability'])
        top_sustainable = sustainable_sites.nlargest(10, 'overall_sustainability')

        recommendations = []
        for _, site in top_sustainable.iterrows():
            recommendations.append({
                'site': site['site_name'],
                'state': site['state'],
                'sustainability_score': site['overall_sustainability'],
                'key_features': self._get_site_features(site),
                'visitor_tips': self._get_visitor_tips(site)
            })

        return pd.DataFrame(recommendations)

    def _get_site_features(self, site):
        features = []

        if site['unesco_status'] == 'Inscribed':
            features.append('UNESCO World Heritage Site')
        if site['accessibility_score'] > 0.8:
            features.append('Excellent Accessibility')
        if site['conservation_status'] in ['Excellent', 'Good']:
            features.append('Well Preserved')
        if site['digital_presence_score'] > 0.7:
            features.append('Strong Digital Presence')

        return ', '.join(features) if features else 'Historical Significance'

    def _get_visitor_tips(self, site):
        tips = []

        if site['current_utilization'] > 0.8:
            tips.append('Visit during weekdays to avoid crowds')
        if site['accessibility_score'] < 0.5:
            tips.append('Prepare for limited accessibility')
        if site['type'] == 'Temple':
            tips.append('Dress modestly and remove footwear')

        return '; '.join(tips) if tips else 'Check local guidelines before visiting'

    def find_similar_destinations(self, selected_site, df_sites, top_n=5):
        features = ['visitor_capacity', 'accessibility_score', 'digital_presence_score', 'current_utilization']

        site_features = df_sites[df_sites['site_name'] == selected_site][features]
        if site_features.empty:
            return pd.DataFrame()

        all_features = df_sites[features].fillna(0)
        scaled_features = self.scaler.fit_transform(all_features)

        site_idx = df_sites[df_sites['site_name'] == selected_site].index[0]
        site_vector = scaled_features[site_idx].reshape(1, -1)

        similarities = cosine_similarity(site_vector, scaled_features)[0]
        similar_indices = similarities.argsort()[-top_n - 1:-1][::-1]

        similar_sites = df_sites.iloc[similar_indices][['site_name', 'state', 'type', 'unesco_status']]
        similar_sites['similarity_score'] = similarities[similar_indices]

        return similar_sites