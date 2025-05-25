import folium
from folium import plugins
import pandas as pd
import streamlit as st
from streamlit_folium import st_folium


class MapVisualizer:
    def __init__(self):
        self.india_coords = [20.5937, 78.9629]
        self.zoom_start = 5

    def create_art_forms_map(self, df_arts):
        m = folium.Map(location=self.india_coords, zoom_start=self.zoom_start)

        marker_cluster = plugins.MarkerCluster().add_to(m)

        for idx, row in df_arts.iterrows():
            color = 'green' if row['risk_level'] == 'Safe' else 'orange' if row['risk_level'] == 'Vulnerable' else 'red'

            popup_html = f"""
            <div style="font-family: Arial, sans-serif;">
                <h4>{row['art_form']}</h4>
                <p><strong>Category:</strong> {row['category']}</p>
                <p><strong>State:</strong> {row['state']}</p>
                <p><strong>Practitioners:</strong> {row['practitioners']}</p>
                <p><strong>Risk Level:</strong> {row['risk_level']}</p>
                <p><strong>Age:</strong> {row['age_years']} years</p>
            </div>
            """

            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=folium.Popup(popup_html, max_width=300),
                icon=folium.Icon(color=color, icon='masks-theater', prefix='fa')
            ).add_to(marker_cluster)

        plugins.Fullscreen().add_to(m)

        return m

    def create_heritage_sites_map(self, df_sites):
        m = folium.Map(location=self.india_coords, zoom_start=self.zoom_start)

        for idx, row in df_sites.iterrows():
            if row['unesco_status'] == 'Inscribed':
                color = 'gold'
                icon = 'crown'
            elif row['unesco_status'] == 'Tentative':
                color = 'blue'
                icon = 'star'
            else:
                color = 'gray'
                icon = 'monument'

            size = 20 + (row['visitor_capacity'] / 1000)

            popup_html = f"""
            <div style="font-family: Arial, sans-serif;">
                <h4>{row['site_name']}</h4>
                <p><strong>Type:</strong> {row['type']}</p>
                <p><strong>State:</strong> {row['state']}</p>
                <p><strong>UNESCO Status:</strong> {row['unesco_status']}</p>
                <p><strong>Conservation:</strong> {row['conservation_status']}</p>
                <p><strong>Accessibility:</strong> {row['accessibility_score']:.2f}</p>
            </div>
            """

            folium.CircleMarker(
                location=[row['latitude'], row['longitude']],
                radius=size,
                color=color,
                fill=True,
                fillColor=color,
                fillOpacity=0.7,
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=row['site_name']
            ).add_to(m)

        legend_html = '''
        <div style="position: fixed; 
                    top: 10px; right: 10px; width: 200px; height: 120px; 
                    background-color: white; border:2px solid grey; z-index:9999; 
                    font-size:14px; padding: 10px">
        <p><strong>UNESCO Status</strong></p>
        <p><span style="color: gold;">●</span> Inscribed</p>
        <p><span style="color: blue;">●</span> Tentative</p>
        <p><span style="color: gray;">●</span> None</p>
        </div>
        '''
        m.get_root().html.add_child(folium.Element(legend_html))

        return m

    def create_tourism_heatmap(self, df_tourism):
        m = folium.Map(location=self.india_coords, zoom_start=self.zoom_start)

        site_coords = df_tourism.groupby('site').first()[['latitude', 'longitude']]
        site_visitors = df_tourism.groupby('site')['total_visitors'].sum()

        heat_data = []
        for site, visitors in site_visitors.items():
            if site in site_coords.index:
                lat = site_coords.loc[site, 'latitude']
                lon = site_coords.loc[site, 'longitude']
                heat_data.append([lat, lon, visitors])

        plugins.HeatMap(heat_data, radius=25).add_to(m)

        return m

    def create_cultural_route_map(self, route_data, df_sites):
        m = folium.Map(location=self.india_coords, zoom_start=self.zoom_start)

        colors = ['red', 'blue', 'green', 'purple', 'orange']

        for idx, route in route_data.iterrows():
            route_sites = df_sites[df_sites['site_name'].isin(route['key_sites'])]

            if len(route_sites) > 0:
                points = [[row['latitude'], row['longitude']] for _, row in route_sites.iterrows()]

                folium.PolyLine(
                    points,
                    color=colors[idx % len(colors)],
                    weight=3,
                    opacity=0.8,
                    popup=f"<b>{route['route_name']}</b><br>Duration: {route['duration_days']} days"
                ).add_to(m)

                for _, site in route_sites.iterrows():
                    folium.Marker(
                        location=[site['latitude'], site['longitude']],
                        popup=site['site_name'],
                        icon=folium.Icon(color=colors[idx % len(colors)], icon='info-sign')
                    ).add_to(m)

        return m