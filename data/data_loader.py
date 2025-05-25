import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os


class DataLoader:
    def __init__(self, use_snowflake=False):
        self.use_snowflake = use_snowflake
        self.states = [
            'Andhra Pradesh', 'Arunachal Pradesh', 'Assam', 'Bihar', 'Chhattisgarh',
            'Goa', 'Gujarat', 'Haryana', 'Himachal Pradesh', 'Jharkhand', 'Karnataka',
            'Kerala', 'Madhya Pradesh', 'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram',
            'Nagaland', 'Odisha', 'Punjab', 'Rajasthan', 'Sikkim', 'Tamil Nadu',
            'Telangana', 'Tripura', 'Uttar Pradesh', 'Uttarakhand', 'West Bengal'
        ]

        if self.use_snowflake:
            try:
                if os.path.exists('snowflake_connector.py'):
                    from snowflake_connector import SnowflakeConnection
                    self.snow = SnowflakeConnection()
                else:
                    self.use_snowflake = False
            except Exception as e:
                print(f"Could not connect to Snowflake: {e}")
                self.use_snowflake = False

    def load_art_forms_data(self):
        if self.use_snowflake:
            try:
                query = "SELECT * FROM ART_FORMS"
                df = self.snow.fetch_dataframe(query)
                if not df.empty:
                    return df
            except:
                pass

        art_forms = []
        art_categories = {
            'Dance': ['Kathakali', 'Bharatanatyam', 'Kathak', 'Odissi', 'Kuchipudi', 'Manipuri', 'Mohiniyattam',
                      'Sattriya', 'Bhangra', 'Garba', 'Ghoomar', 'Bihu'],
            'Music': ['Hindustani Classical', 'Carnatic', 'Folk Songs', 'Qawwali', 'Baul', 'Lavani',
                      'Rabindra Sangeet'],
            'Craft': ['Pottery', 'Weaving', 'Embroidery', 'Wood Carving', 'Metal Work', 'Jewelry Making', 'Painting'],
            'Painting': ['Madhubani', 'Warli', 'Pattachitra', 'Miniature', 'Tanjore', 'Kalamkari', 'Phad'],
            'Theatre': ['Yakshagana', 'Kathputli', 'Bhand Pather', 'Nautanki', 'Tamasha', 'Therukoothu']
        }

        for state in self.states:
            num_arts = random.randint(3, 8)
            for _ in range(num_arts):
                category = random.choice(list(art_categories.keys()))
                art_form = random.choice(art_categories[category])

                art_forms.append({
                    'state': state,
                    'art_form': art_form,
                    'category': category,
                    'practitioners': random.randint(100, 5000),
                    'unesco_recognized': random.choice([True, False]),
                    'risk_level': random.choice(['Safe', 'Vulnerable', 'Endangered']),
                    'age_years': random.randint(100, 2000),
                    'latitude': 20 + random.uniform(-10, 15),
                    'longitude': 78 + random.uniform(-15, 15)
                })

        return pd.DataFrame(art_forms)

    def load_tourism_data(self):
        if self.use_snowflake:
            try:
                query = "SELECT * FROM TOURISM_DATA"
                df = self.snow.fetch_dataframe(query)
                if not df.empty:
                    if 'total_visitors' not in df.columns:
                        df['total_visitors'] = df['domestic_visitors'] + df['international_visitors']
                    return df
            except:
                pass

        tourism_data = []
        sites = [
            'Taj Mahal', 'Red Fort', 'Qutub Minar', 'Gateway of India', 'Hawa Mahal',
            'Mysore Palace', 'Charminar', 'Victoria Memorial', 'Meenakshi Temple',
            'Golden Temple', 'Konark Sun Temple', 'Khajuraho Temples', 'Ajanta Caves',
            'Ellora Caves', 'Hampi', 'Fatehpur Sikri', 'Jaisalmer Fort', 'Udaipur City Palace'
        ]

        start_date = datetime(2020, 1, 1)
        end_date = datetime(2024, 12, 31)

        for site in sites:
            state = random.choice(self.states)
            base_visitors = random.randint(50000, 500000)

            for month in pd.date_range(start_date, end_date, freq='ME'):
                seasonal_factor = 1 + 0.3 * np.sin(2 * np.pi * month.month / 12)
                trend_factor = 1 + 0.02 * (month.year - 2020)

                visitors = int(base_visitors * seasonal_factor * trend_factor * random.uniform(0.8, 1.2) / 12)

                tourism_data.append({
                    'site': site,
                    'state': state,
                    'date': month,
                    'domestic_visitors': int(visitors * 0.7),
                    'international_visitors': int(visitors * 0.3),
                    'revenue': visitors * random.randint(50, 200),
                    'sustainability_score': random.uniform(0.5, 1.0),
                    'crowding_index': random.uniform(0.3, 0.9)
                })

        return pd.DataFrame(tourism_data)

    def load_cultural_sites_data(self):
        if self.use_snowflake:
            try:
                query = "SELECT * FROM CULTURAL_SITES"
                df = self.snow.fetch_dataframe(query)
                if not df.empty:
                    return df
            except:
                pass

        cultural_sites = []
        site_types = ['Temple', 'Monument', 'Palace', 'Fort', 'Museum', 'Heritage Village']

        for state in self.states:
            num_sites = random.randint(5, 15)
            for i in range(num_sites):
                cultural_sites.append({
                    'site_name': f'{state} Heritage Site {i + 1}',
                    'state': state,
                    'type': random.choice(site_types),
                    'establishment_year': random.randint(500, 1900),
                    'unesco_status': random.choice(['Inscribed', 'Tentative', 'None']),
                    'conservation_status': random.choice(['Excellent', 'Good', 'Fair', 'Poor']),
                    'annual_maintenance_cost': random.randint(100000, 5000000),
                    'visitor_capacity': random.randint(1000, 10000),
                    'current_utilization': random.uniform(0.3, 0.95),
                    'accessibility_score': random.uniform(0.4, 1.0),
                    'digital_presence_score': random.uniform(0.2, 1.0),
                    'latitude': 20 + random.uniform(-10, 15),
                    'longitude': 78 + random.uniform(-15, 15)
                })

        return pd.DataFrame(cultural_sites)

    def load_festival_data(self):
        if self.use_snowflake:
            try:
                query = "SELECT * FROM FESTIVALS"
                df = self.snow.fetch_dataframe(query)
                if not df.empty:
                    return df
            except:
                pass

        festivals = []
        festival_names = [
            'Diwali', 'Holi', 'Durga Puja', 'Ganesh Chaturthi', 'Onam', 'Pongal',
            'Bihu', 'Navratri', 'Baisakhi', 'Makar Sankranti', 'Rath Yatra',
            'Hornbill Festival', 'Pushkar Fair', 'Kumbh Mela', 'Desert Festival'
        ]

        for festival in festival_names:
            states_celebrating = random.sample(self.states, random.randint(1, 5))
            for state in states_celebrating:
                festivals.append({
                    'festival': festival,
                    'state': state,
                    'duration_days': random.randint(1, 10),
                    'expected_visitors': random.randint(10000, 1000000),
                    'economic_impact': random.randint(1000000, 50000000),
                    'cultural_significance_score': random.uniform(0.7, 1.0),
                    'tourism_potential_score': random.uniform(0.5, 1.0),
                    'month': random.randint(1, 12)
                })

        return pd.DataFrame(festivals)