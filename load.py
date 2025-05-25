import pandas as pd
import snowflake.connector
from datetime import datetime, timedelta
import random
from config import SNOWFLAKE_CONFIG


class EnhancedDataLoader:
    def __init__(self):
        self.conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        self.cursor = self.conn.cursor()

    def load_comprehensive_art_forms(self):
        art_forms_data = [
            ('Kerala', 'Kathakali', 'Dance', 2500, True, 'Safe', 500,
             'Classical dance-drama known for elaborate costumes and makeup'),
            ('Kerala', 'Mohiniyattam', 'Dance', 1500, True, 'Safe', 300, 'Graceful dance form performed by women'),
            ('Kerala', 'Theyyam', 'Ritual Dance', 800, False, 'Vulnerable', 800,
             'Ritualistic worship incorporating dance and music'),
            ('Kerala', 'Koodiyattam', 'Theatre', 300, True, 'Endangered', 2000,
             'UNESCO recognized Sanskrit theatre form'),
            ('Tamil Nadu', 'Bharatanatyam', 'Dance', 5000, True, 'Safe', 2000,
             'One of the oldest classical dance forms'),
            ('Tamil Nadu', 'Carnatic Music', 'Music', 10000, True, 'Safe', 500,
             'South Indian classical music tradition'),
            ('Tamil Nadu', 'Tanjore Painting', 'Painting', 1200, False, 'Vulnerable', 400,
             'Paintings with gold foil and precious stones'),
            ('Tamil Nadu', 'Villu Paatu', 'Music', 200, False, 'Endangered', 300, 'Bow song narrative performance'),
            ('Karnataka', 'Yakshagana', 'Theatre', 1500, False, 'Vulnerable', 400,
             'Traditional theatre with dance, music, and dialogue'),
            ('Karnataka', 'Mysore Painting', 'Painting', 800, False, 'Endangered', 500,
             'Paintings known for gold leaf work'),
            ('Karnataka', 'Dollu Kunitha', 'Dance', 1000, False, 'Safe', 300, 'Drum dance popular in Karnataka'),
            ('Karnataka', 'Bhoota Kola', 'Ritual Dance', 500, False, 'Vulnerable', 1000,
             'Spirit worship ritual performance'),
            ('Andhra Pradesh', 'Kuchipudi', 'Dance', 2000, True, 'Safe', 400,
             'Classical dance with origins in village traditions'),
            ('Andhra Pradesh', 'Kalamkari', 'Painting', 600, False, 'Endangered', 3000,
             'Hand-painted or block-printed cotton textile'),
            ('Andhra Pradesh', 'Burrakatha', 'Theatre', 400, False, 'Endangered', 200,
             'Storytelling with social messages'),
            ('Telangana', 'Perini Sivatandavam', 'Dance', 300, False, 'Endangered', 700, 'Ancient warrior dance form'),
            ('Telangana', 'Bonalu', 'Ritual Dance', 2000, False, 'Safe', 400,
             'Folk festival dance for Goddess Mahakali'),
            ('Odisha', 'Odissi', 'Dance', 1800, True, 'Safe', 2000, 'Classical dance with sculptural poses'),
            ('Odisha', 'Pattachitra', 'Painting', 1000, False, 'Vulnerable', 800,
             'Traditional cloth-based scroll painting'),
            ('Odisha', 'Gotipua', 'Dance', 400, False, 'Vulnerable', 500, 'Precursor to Odissi performed by boys'),
            ('Odisha', 'Chhau', 'Dance', 800, True, 'Vulnerable', 900, 'Semi-classical martial dance'),
            ('West Bengal', 'Kathak', 'Dance', 3000, True, 'Safe', 200, 'North Indian classical dance'),
            ('West Bengal', 'Rabindra Sangeet', 'Music', 5000, False, 'Safe', 150,
             'Songs written by Rabindranath Tagore'),
            ('West Bengal', 'Baul', 'Music', 2000, True, 'Vulnerable', 500, 'Mystic minstrel tradition'),
            ('West Bengal', 'Patachitra', 'Painting', 800, False, 'Vulnerable', 1000, 'Traditional scroll painting'),
            ('West Bengal', 'Gambhira', 'Theatre', 300, False, 'Endangered', 400, 'Traditional masked dance-drama'),
            ('Rajasthan', 'Ghoomar', 'Dance', 1500, False, 'Safe', 400, 'Traditional folk dance of Rajasthan'),
            ('Rajasthan', 'Kathputli', 'Theatre', 500, False, 'Endangered', 1000, 'String puppet theatre'),
            ('Rajasthan', 'Blue Pottery', 'Craft', 800, False, 'Vulnerable', 400, 'Distinctive blue glazed pottery'),
            ('Rajasthan', 'Phad Painting', 'Painting', 400, False, 'Endangered', 700,
             'Scroll paintings depicting folk deities'),
            ('Rajasthan', 'Kalbelia', 'Dance', 600, True, 'Vulnerable', 500, 'Snake charmer community dance'),
            ('Gujarat', 'Garba', 'Dance', 10000, True, 'Safe', 1000, 'Circular dance performed during Navratri'),
            ('Gujarat', 'Patola', 'Craft', 200, True, 'Endangered', 700, 'Double ikat woven sari'),
            ('Gujarat', 'Bhavai', 'Theatre', 300, False, 'Endangered', 600, 'Folk theatre with social commentary'),
            ('Maharashtra', 'Lavani', 'Dance', 1200, False, 'Vulnerable', 600,
             'Traditional dance with powerful rhythm'),
            ('Maharashtra', 'Warli Painting', 'Painting', 1500, False, 'Vulnerable', 3000,
             'Tribal art using geometric patterns'),
            ('Maharashtra', 'Tamasha', 'Theatre', 500, False, 'Vulnerable', 400,
             'Folk theatre combining dance and drama'),
            ('Maharashtra', 'Powada', 'Music', 400, False, 'Endangered', 500, 'Ballads of Maratha warriors'),
            ('Punjab', 'Bhangra', 'Dance', 5000, False, 'Safe', 500, 'Energetic harvest dance'),
            ('Punjab', 'Phulkari', 'Craft', 400, False, 'Endangered', 200, 'Traditional embroidery work'),
            ('Punjab', 'Giddha', 'Dance', 2000, False, 'Safe', 300, 'Womens folk dance'),
            ('Uttar Pradesh', 'Kathak', 'Dance', 2500, True, 'Safe', 500, 'Classical dance of North India'),
            ('Uttar Pradesh', 'Chikankari', 'Craft', 3000, False, 'Safe', 400, 'Traditional embroidery style'),
            ('Uttar Pradesh', 'Ramlila', 'Theatre', 1500, True, 'Safe', 500, 'Theatrical representation of Ramayana'),
            ('Madhya Pradesh', 'Gond Painting', 'Painting', 800, False, 'Vulnerable', 1000,
             'Tribal art with intricate patterns'),
            ('Madhya Pradesh', 'Bagh Print', 'Craft', 300, False, 'Endangered', 400, 'Traditional hand block printing'),
            ('Assam', 'Bihu', 'Dance', 8000, False, 'Safe', 600, 'Folk dance celebrating Assamese New Year'),
            ('Assam', 'Sattriya', 'Dance', 800, True, 'Vulnerable', 500,
             'Classical dance from Vaishnavite monasteries'),
            ('Assam', 'Assamese Silk', 'Craft', 1500, False, 'Vulnerable', 1000, 'Muga, Eri and Pat silk weaving'),
            ('Manipur', 'Manipuri', 'Dance', 1000, True, 'Safe', 1500, 'Classical dance known for graceful movements'),
            ('Manipur', 'Thang-Ta', 'Martial Art', 300, False, 'Endangered', 800, 'Sword and spear martial art'),
            ('Manipur', 'Pung Cholom', 'Dance', 400, False, 'Vulnerable', 400, 'Drum dance of Manipur'),
            ('Nagaland', 'Naga Folk Songs', 'Music', 1500, False, 'Vulnerable', 500,
             'Traditional songs of various Naga tribes'),
            ('Nagaland', 'War Dance', 'Dance', 800, False, 'Vulnerable', 600, 'Traditional warrior dances'),
            ('Mizoram', 'Cheraw', 'Dance', 600, False, 'Vulnerable', 400, 'Bamboo dance of Mizoram'),
            ('Tripura', 'Hojagiri', 'Dance', 300, False, 'Endangered', 500, 'Dance performed on earthen pitchers'),
            ('Sikkim', 'Mask Dance', 'Dance', 400, False, 'Vulnerable', 300, 'Buddhist ritual dance'),
            ('Himachal Pradesh', 'Nati', 'Dance', 2000, False, 'Safe', 1000,
             'Listed in Guinness Book as largest folk dance'),
            ('Himachal Pradesh', 'Chamba Rumal', 'Craft', 200, False, 'Endangered', 300, 'Embroidered handkerchief'),
            ('Uttarakhand', 'Aipan', 'Painting', 500, False, 'Endangered', 2000, 'Floor and wall paintings'),
            ('Uttarakhand', 'Langvir Nritya', 'Dance', 200, False, 'Endangered', 300, 'Acrobatic dance form'),
            ('Bihar', 'Madhubani', 'Painting', 2000, True, 'Safe', 2500, 'Mithila painting with natural dyes'),
            ('Bihar', 'Bidesia', 'Theatre', 400, False, 'Endangered', 200, 'Folk theatre about migration'),
            ('Jharkhand', 'Chhau', 'Dance', 800, True, 'Vulnerable', 1000, 'Martial dance with masks'),
            ('Jharkhand', 'Paika', 'Dance', 500, False, 'Vulnerable', 400, 'Martial dance with weapons'),
            ('Goa', 'Fugdi', 'Dance', 600, False, 'Safe', 400, 'Womens folk dance'),
            ('Goa', 'Dekhni', 'Dance', 300, False, 'Endangered', 500, 'Fusion of Indian and Western dance'),
            ('Goa', 'Kunbi Saree', 'Craft', 200, False, 'Endangered', 300, 'Traditional tribal weaving')
        ]

        state_coords = {
            'Kerala': (10.8505, 76.2711), 'Tamil Nadu': (11.1271, 78.6569),
            'Karnataka': (15.3173, 75.7139), 'Andhra Pradesh': (15.9129, 79.7400),
            'Telangana': (18.1124, 79.0193), 'Odisha': (20.9517, 85.0985),
            'West Bengal': (22.9868, 87.8550), 'Rajasthan': (27.0238, 74.2179),
            'Gujarat': (22.2587, 71.1924), 'Maharashtra': (19.7515, 75.7139),
            'Punjab': (31.1471, 75.3412), 'Uttar Pradesh': (26.8467, 80.9462),
            'Madhya Pradesh': (22.9734, 78.6569), 'Assam': (26.2006, 92.9376),
            'Manipur': (24.6637, 93.9063), 'Himachal Pradesh': (31.1048, 77.1734),
            'Uttarakhand': (30.0668, 79.0193), 'Bihar': (25.0961, 85.3131),
            'Jharkhand': (23.6102, 85.2799), 'Nagaland': (26.1584, 94.5624),
            'Sikkim': (27.5330, 88.5122), 'Goa': (15.2993, 74.1240),
            'Mizoram': (23.1645, 92.9376), 'Tripura': (23.9408, 91.9882)
        }

        print("Loading comprehensive Art Forms data...")
        self.cursor.execute("TRUNCATE TABLE ART_FORMS")
        for data in art_forms_data:
            state, art_form, category, practitioners, unesco, risk, age, description = data
            lat, lon = state_coords.get(state, (20.5937, 78.9629))
            lat += random.uniform(-0.5, 0.5)
            lon += random.uniform(-0.5, 0.5)
            query = """
                    INSERT INTO ART_FORMS (state, art_form, category, practitioners, unesco_recognized,
                                           risk_level, age_years, latitude, longitude)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) \
                    """
            self.cursor.execute(query, (state, art_form, category, practitioners, unesco,
                                        risk, age, lat, lon))
        self.conn.commit()
        print(f"Loaded {len(art_forms_data)} art forms")
    def load_comprehensive_cultural_sites(self):
        cultural_sites_data = [
            ('Taj Mahal', 'Uttar Pradesh', 'Monument', 1653, 'Inscribed', 'Excellent', 50000000, 70000, 0.85),
            ('Agra Fort', 'Uttar Pradesh', 'Fort', 1573, 'Inscribed', 'Good', 25000000, 40000, 0.75),
            ('Red Fort Complex', 'Delhi', 'Fort', 1648, 'Inscribed', 'Good', 30000000, 50000, 0.75),
            ('Qutub Minar and its Monuments', 'Delhi', 'Monument', 1193, 'Inscribed', 'Excellent', 15000000, 30000,
             0.65),
            ('Humayuns Tomb', 'Delhi', 'Monument', 1572, 'Inscribed', 'Excellent', 12000000, 25000, 0.70),
            ('Ajanta Caves', 'Maharashtra', 'Monument', -200, 'Inscribed', 'Good', 20000000, 20000, 0.55),
            ('Ellora Caves', 'Maharashtra', 'Monument', 600, 'Inscribed', 'Good', 18000000, 25000, 0.60),
            ('Elephanta Caves', 'Maharashtra', 'Monument', 600, 'Inscribed', 'Good', 10000000, 15000, 0.50),
            ('Chhatrapati Shivaji Terminus', 'Maharashtra', 'Monument', 1887, 'Inscribed', 'Good', 3000000, 50000,
             0.90),
            ('Group of Monuments at Hampi', 'Karnataka', 'Heritage Village', 1336, 'Inscribed', 'Fair', 25000000, 40000,
             0.70),
            ('Group of Monuments at Pattadakal', 'Karnataka', 'Temple', 750, 'Inscribed', 'Good', 5000000, 10000, 0.40),
            ('Khajuraho Group of Monuments', 'Madhya Pradesh', 'Temple', 950, 'Inscribed', 'Good', 15000000, 25000,
             0.50),
            ('Buddhist Monuments at Sanchi', 'Madhya Pradesh', 'Monument', -300, 'Inscribed', 'Good', 8000000, 15000,
             0.40),
            ('Rock Shelters of Bhimbetka', 'Madhya Pradesh', 'Heritage Village', -30000, 'Inscribed', 'Fair', 3000000,
             8000, 0.30),
            ('Sun Temple Konark', 'Odisha', 'Temple', 1250, 'Inscribed', 'Fair', 10000000, 20000, 0.45),
            ('Kaziranga National Park', 'Assam', 'Heritage Village', 1905, 'Inscribed', 'Excellent', 15000000, 20000,
             0.60),
            ('Manas Wildlife Sanctuary', 'Assam', 'Heritage Village', 1928, 'Inscribed', 'Good', 8000000, 10000, 0.40),
            ('Keoladeo National Park', 'Rajasthan', 'Heritage Village', 1971, 'Inscribed', 'Good', 6000000, 15000,
             0.50),
            ('Churches and Convents of Goa', 'Goa', 'Monument', 1594, 'Inscribed', 'Good', 12000000, 30000, 0.65),
            ('Fatehpur Sikri', 'Uttar Pradesh', 'Monument', 1571, 'Inscribed', 'Good', 12000000, 30000, 0.55),
            ('Group of Monuments at Mahabalipuram', 'Tamil Nadu', 'Monument', 700, 'Inscribed', 'Good', 12000000, 25000,
             0.60),
            ('Great Living Chola Temples', 'Tamil Nadu', 'Temple', 1010, 'Inscribed', 'Excellent', 8000000, 30000,
             0.65),
            ('Sundarbans National Park', 'West Bengal', 'Heritage Village', 1984, 'Inscribed', 'Good', 5000000, 10000,
             0.35),
            ('Nanda Devi and Valley of Flowers', 'Uttarakhand', 'Heritage Village', 1988, 'Inscribed', 'Excellent',
             4000000, 5000, 0.25),
            ('Mountain Railways of India', 'Multiple', 'Monument', 1881, 'Inscribed', 'Good', 10000000, 30000, 0.70),
            ('Mahabodhi Temple Complex', 'Bihar', 'Temple', 250, 'Inscribed', 'Good', 8000000, 30000, 0.70),
            ('Jantar Mantar Jaipur', 'Rajasthan', 'Monument', 1734, 'Inscribed', 'Good', 6000000, 15000, 0.55),
            ('Western Ghats', 'Multiple', 'Heritage Village', 0, 'Inscribed', 'Excellent', 2000000, 50000, 0.20),
            ('Hill Forts of Rajasthan', 'Rajasthan', 'Fort', 1200, 'Inscribed', 'Good', 15000000, 40000, 0.65),
            ('Rani-ki-Vav', 'Gujarat', 'Monument', 1063, 'Inscribed', 'Excellent', 5000000, 10000, 0.45),
            ('Archaeological Site of Nalanda', 'Bihar', 'Heritage Village', 500, 'Inscribed', 'Fair', 5000000, 15000,
             0.35),
            ('Khangchendzonga National Park', 'Sikkim', 'Heritage Village', 1977, 'Inscribed', 'Excellent', 3000000,
             8000, 0.30),
            ('Le Corbusier Capitol Complex', 'Chandigarh', 'Monument', 1953, 'Inscribed', 'Good', 4000000, 20000, 0.50),
            ('Historic City of Ahmadabad', 'Gujarat', 'Heritage Village', 1411, 'Inscribed', 'Fair', 8000000, 50000,
             0.60),
            ('Victorian Gothic and Art Deco', 'Maharashtra', 'Monument', 1878, 'Inscribed', 'Good', 5000000, 40000,
             0.75),
            ('Jaipur City', 'Rajasthan', 'Heritage Village', 1727, 'Inscribed', 'Good', 20000000, 80000, 0.80),
            ('Ramappa Temple', 'Telangana', 'Temple', 1213, 'Inscribed', 'Good', 3000000, 10000, 0.40),
            ('Dholavira', 'Gujarat', 'Heritage Village', -3000, 'Inscribed', 'Fair', 2000000, 8000, 0.30),
            ('Mysore Palace', 'Karnataka', 'Palace', 1912, 'None', 'Excellent', 20000000, 60000, 0.90),
            ('Hawa Mahal', 'Rajasthan', 'Palace', 1799, 'None', 'Good', 8000000, 20000, 0.75),
            ('Gateway of India', 'Maharashtra', 'Monument', 1924, 'None', 'Excellent', 5000000, 100000, 0.95),
            ('Charminar', 'Telangana', 'Monument', 1591, 'Tentative', 'Good', 7000000, 30000, 0.80),
            ('Golden Temple', 'Punjab', 'Temple', 1588, 'Tentative', 'Excellent', 25000000, 100000, 0.95),
            ('Meenakshi Temple', 'Tamil Nadu', 'Temple', 1623, 'Tentative', 'Excellent', 15000000, 50000, 0.85),
            ('Victoria Memorial', 'West Bengal', 'Monument', 1921, 'None', 'Excellent', 10000000, 40000, 0.70),
            ('Lotus Temple', 'Delhi', 'Temple', 1986, 'None', 'Excellent', 45000000, 35000, 0.90),
            ('Akshardham Temple', 'Delhi', 'Temple', 2005, 'None', 'Excellent', 30000000, 40000, 0.85),
            ('Udaipur City Palace', 'Rajasthan', 'Palace', 1559, 'None', 'Good', 10000000, 35000, 0.75),
            ('Jaisalmer Fort', 'Rajasthan', 'Fort', 1156, 'Tentative', 'Fair', 7000000, 20000, 0.65),
            ('Amer Fort', 'Rajasthan', 'Fort', 1592, 'None', 'Good', 15000000, 35000, 0.80),
            ('Mehrangarh Fort', 'Rajasthan', 'Fort', 1459, 'None', 'Excellent', 12000000, 30000, 0.75),
            ('Golconda Fort', 'Telangana', 'Fort', 1143, 'Tentative', 'Fair', 8000000, 25000, 0.60),
            ('Gwalior Fort', 'Madhya Pradesh', 'Fort', 800, 'Tentative', 'Good', 5000000, 20000, 0.55),
            ('Somnath Temple', 'Gujarat', 'Temple', 1951, 'None', 'Excellent', 10000000, 40000, 0.80),
            ('Jagannath Temple', 'Odisha', 'Temple', 1174, 'Tentative', 'Good', 12000000, 50000, 0.85),
            ('Kashi Vishwanath Temple', 'Uttar Pradesh', 'Temple', 1780, 'None', 'Good', 15000000, 60000, 0.90),
            ('Dilwara Temples', 'Rajasthan', 'Temple', 1031, 'Tentative', 'Excellent', 4000000, 15000, 0.60),
            ('Belur Math', 'West Bengal', 'Temple', 1897, 'None', 'Good', 5000000, 25000, 0.70),
            ('Birla Mandir Jaipur', 'Rajasthan', 'Temple', 1988, 'None', 'Good', 3000000, 20000, 0.65),
            ('India Gate', 'Delhi', 'Monument', 1931, 'None', 'Excellent', 40000000, 100000, 0.95)
        ]
        state_coords = {
            'Uttar Pradesh': (26.8467, 80.9462), 'Delhi': (28.7041, 77.1025),
            'Maharashtra': (19.7515, 75.7139), 'Karnataka': (15.3173, 75.7139),
            'Madhya Pradesh': (22.9734, 78.6569), 'Odisha': (20.9517, 85.0985),
            'Rajasthan': (27.0238, 74.2179), 'Telangana': (18.1124, 79.0193),
            'Punjab': (31.1471, 75.3412), 'Tamil Nadu': (11.1271, 78.6569),
            'West Bengal': (22.9868, 87.8550), 'Gujarat': (22.2587, 71.1924),
            'Bihar': (25.0961, 85.3131), 'Assam': (26.2006, 92.9376),
            'Goa': (15.2993, 74.1240), 'Uttarakhand': (30.0668, 79.0193),
            'Sikkim': (27.5330, 88.5122), 'Chandigarh': (30.7333, 76.7794),
            'Multiple': (20.5937, 78.9629)
        }
        print("Loading comprehensive Cultural Sites data...")
        self.cursor.execute("TRUNCATE TABLE CULTURAL_SITES")
        for data in cultural_sites_data:
            site, state, type_, year, unesco, conservation, cost, capacity, utilization = data[:9]
            lat, lon = state_coords.get(state, (20.5937, 78.9629))
            lat += random.uniform(-0.2, 0.2)
            lon += random.uniform(-0.2, 0.2)
            accessibility = random.uniform(0.6, 1.0) if conservation in ['Excellent', 'Good'] else random.uniform(0.3,                                                                                              0.7)
            digital = random.uniform(0.7, 1.0) if year > 1900 else random.uniform(0.3, 0.7)
            query = """
                    INSERT INTO CULTURAL_SITES (site_name, state, type, establishment_year, unesco_status,
                                                conservation_status, annual_maintenance_cost, visitor_capacity,
                                                current_utilization, accessibility_score, digital_presence_score,
                                                latitude, longitude)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
                    """
            self.cursor.execute(query, (site, state, type_, year, unesco, conservation, cost,capacity, utilization, accessibility, digital, lat, lon))
        self.conn.commit()
        print(f"Loaded {len(cultural_sites_data)} cultural sites")
    def load_comprehensive_tourism_data(self):
        print("Loading comprehensive Tourism data...")
        self.cursor.execute("TRUNCATE TABLE TOURISM_DATA")
        sites = [
            ('Taj Mahal', 'Uttar Pradesh', 7000000),
            ('Red Fort Complex', 'Delhi', 3500000),
            ('Qutub Minar and its Monuments', 'Delhi', 2800000),
            ('Agra Fort', 'Uttar Pradesh', 2500000),
            ('Ajanta Caves', 'Maharashtra', 1200000),
            ('Ellora Caves', 'Maharashtra', 1100000),
            ('Hampi', 'Karnataka', 900000),
            ('Mysore Palace', 'Karnataka', 4500000),
            ('Golden Temple', 'Punjab', 5500000),
            ('Gateway of India', 'Maharashtra', 4000000),
            ('Hawa Mahal', 'Rajasthan', 1800000),
            ('Amer Fort', 'Rajasthan', 2000000),
            ('Mehrangarh Fort', 'Rajasthan', 1500000),
            ('Meenakshi Temple', 'Tamil Nadu', 2500000),
            ('Charminar', 'Telangana', 1700000),
            ('Victoria Memorial', 'West Bengal', 1300000),
            ('Lotus Temple', 'Delhi', 4800000),
            ('Akshardham Temple', 'Delhi', 3200000),
            ('India Gate', 'Delhi', 4200000),
            ('Konark Sun Temple', 'Odisha', 800000),
            ('Mahabodhi Temple Complex', 'Bihar', 1000000),
            ('Jagannath Temple', 'Odisha', 1600000),
            ('Somnath Temple', 'Gujarat', 1400000),
            ('Fatehpur Sikri', 'Uttar Pradesh', 900000),
            ('Khajuraho Group of Monuments', 'Madhya Pradesh', 700000),
            ('Sanchi Stupa', 'Madhya Pradesh', 500000),
            ('Jaipur City', 'Rajasthan', 2800000),
            ('Udaipur City Palace', 'Rajasthan', 1200000),
            ('Jaisalmer Fort', 'Rajasthan', 800000),
            ('Mahabalipuram', 'Tamil Nadu', 900000)
        ]
        start_date = datetime(2022, 1, 1)
        end_date = datetime(2024, 12, 31)
        for site, state, annual_visitors in sites:
            current_date = start_date
            while current_date <= end_date:
                month = current_date.month
                year = current_date.year
                if month in [10, 11, 12, 1, 2, 3]:
                    seasonal_factor = random.uniform(1.3, 1.6)
                elif month in [4, 5, 6]:
                    seasonal_factor = random.uniform(0.5, 0.7)
                else:
                    seasonal_factor = random.uniform(0.8, 1.0)
                growth_factor = 1 + ((year - 2022) * 0.05)
                if year == 2022:
                    covid_factor = 0.7
                elif year == 2023:
                    covid_factor = 0.9
                else:
                    covid_factor = 1.0
                monthly_visitors = int(
                    (annual_visitors / 12) * seasonal_factor * growth_factor * covid_factor * random.uniform(0.9, 1.1))
                if site in ['Taj Mahal', 'Red Fort Complex', 'Agra Fort', 'Jaipur City']:
                    intl_percentage = 0.35
                elif site in ['Golden Temple', 'Meenakshi Temple', 'Jagannath Temple']:
                    intl_percentage = 0.15
                else:
                    intl_percentage = 0.25
                domestic = int(monthly_visitors * (1 - intl_percentage))
                international = int(monthly_visitors * intl_percentage)
                if 'Temple' in site:
                    revenue = monthly_visitors * random.randint(50, 100)
                else:
                    revenue = monthly_visitors * random.randint(150, 300)
                sustainability = random.uniform(0.6, 0.9)
                crowding = min(0.95, (monthly_visitors / (annual_visitors / 12)) * 0.6)
                query = """
                        INSERT INTO TOURISM_DATA (site, state, date, domestic_visitors, international_visitors,
                                                  revenue, sustainability_score, crowding_index)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) \
                        """
                self.cursor.execute(query, (site, state, current_date, domestic, international,revenue, sustainability, crowding))
                current_date = current_date.replace(day=1) + timedelta(days=32)
                current_date = current_date.replace(day=1)
        self.conn.commit()
        print(f"Loaded tourism data for {len(sites)} sites across 3 years")
    def load_comprehensive_festivals(self):
        festivals_data = [
            ('Diwali', ['All States'], 5, 50000000, 500000000, 1.0, 0.9, 10,
             'Festival of Lights celebrated nationwide'),
            ('Holi', ['All States'], 2, 30000000, 300000000, 1.0, 0.8, 3, 'Festival of Colors celebrated nationwide'),
            ('Dussehra', ['All States'], 10, 20000000, 200000000, 0.95, 0.8, 10, 'Victory of good over evil'),
            ('Ganesh Chaturthi', ['Maharashtra', 'Karnataka', 'Andhra Pradesh', 'Tamil Nadu'], 11, 8000000, 80000000,
             0.9, 0.8, 8, 'Lord Ganesha festival'),
            ('Durga Puja', ['West Bengal', 'Assam', 'Odisha', 'Tripura'], 10, 10000000, 100000000, 0.95, 0.85, 9,
             'Worship of Goddess Durga'),
            ('Navratri', ['Gujarat', 'Maharashtra', 'West Bengal', 'Karnataka'], 9, 7000000, 70000000, 0.95, 0.85, 9,
             'Nine nights festival'),
            ('Makar Sankranti', ['All States'], 1, 15000000, 100000000, 0.9, 0.7, 1, 'Harvest festival of India'),

            # Regional Festivals
            ('Onam', ['Kerala'], 10, 5000000, 50000000, 0.95, 0.9, 8, 'Harvest festival of Kerala'),
            ('Pongal', ['Tamil Nadu'], 4, 3000000, 30000000, 0.9, 0.7, 1, 'Tamil harvest festival'),
            ('Bihu', ['Assam'], 3, 2000000, 20000000, 0.85, 0.75, 4, 'Assamese New Year festival'),
            ('Baisakhi', ['Punjab', 'Haryana'], 1, 2000000, 25000000, 0.85, 0.7, 4, 'Punjabi harvest festival'),
            ('Rath Yatra', ['Odisha'], 9, 1500000, 20000000, 0.9, 0.85, 7, 'Chariot festival of Lord Jagannath'),
            ('Pushkar Fair', ['Rajasthan'], 5, 500000, 30000000, 0.85, 0.95, 11, 'Camel fair and cultural festival'),
            ('Hornbill Festival', ['Nagaland'], 10, 200000, 10000000, 0.8, 0.95, 12,
             'Festival of festivals in Nagaland'),
            ('Desert Festival', ['Rajasthan'], 3, 300000, 15000000, 0.8, 0.9, 2, 'Cultural festival in Thar Desert'),
            ('Hemis Festival', ['Ladakh'], 2, 100000, 5000000, 0.75, 0.85, 6, 'Buddhist festival in Ladakh'),
            ('Thrissur Pooram', ['Kerala'], 1, 1000000, 20000000, 0.9, 0.8, 5, 'Temple festival with elephants'),
            ('Mysore Dasara', ['Karnataka'], 10, 800000, 40000000, 0.9, 0.85, 10, 'Royal festival of Mysore'),
            ('Kumbh Mela', ['Uttar Pradesh', 'Uttarakhand', 'Madhya Pradesh', 'Maharashtra'], 30, 50000000, 1000000000,
             1.0, 0.95, 1, 'Largest religious gathering'),
            ('Goa Carnival', ['Goa'], 3, 500000, 25000000, 0.7, 0.9, 2, 'Portuguese-influenced carnival'),
            ('Sangai Festival', ['Manipur'], 10, 150000, 8000000, 0.75, 0.8, 11, 'Tourism festival of Manipur'),
            ('Losar', ['Sikkim', 'Himachal Pradesh', 'Uttarakhand'], 15, 100000, 5000000, 0.8, 0.7, 2,
             'Tibetan New Year'),
            ('Chhath Puja', ['Bihar', 'Jharkhand', 'Uttar Pradesh'], 4, 3000000, 30000000, 0.9, 0.6, 11,
             'Sun worship festival'),
            ('Poila Boishakh', ['West Bengal'], 1, 2000000, 20000000, 0.85, 0.7, 4, 'Bengali New Year'),
            ('Vishu', ['Kerala'], 1, 1500000, 15000000, 0.85, 0.6, 4, 'Malayalam New Year'),
            ('Ugadi', ['Andhra Pradesh', 'Telangana', 'Karnataka'], 1, 2500000, 25000000, 0.85, 0.6, 3,
             'Telugu/Kannada New Year'),
            ('Bhogali Bihu', ['Assam'], 3, 1000000, 10000000, 0.8, 0.7, 1, 'Harvest festival of Assam'),
            ('Natyanjali', ['Tamil Nadu'], 5, 50000, 3000000, 0.8, 0.85, 2, 'Dance festival at Chidambaram'),
            ('Konark Dance Festival', ['Odisha'], 5, 40000, 2000000, 0.75, 0.9, 12, 'Classical dance festival'),
            ('Khajuraho Dance Festival', ['Madhya Pradesh'], 7, 30000, 2500000, 0.75, 0.9, 2,
             'Classical dance festival'),
            ('International Kite Festival', ['Gujarat'], 2, 200000, 10000000, 0.7, 0.85, 1, 'Kite flying festival'),
            ('Ziro Music Festival', ['Arunachal Pradesh'], 4, 30000, 3000000, 0.6, 0.95, 9, 'Indie music festival'),
            ('Rann Utsav', ['Gujarat'], 120, 600000, 40000000, 0.7, 0.95, 11, 'White desert festival'),
            ('Tulip Festival', ['Jammu and Kashmir'], 15, 200000, 10000000, 0.7, 0.9, 4, 'Spring flower festival'),
            ('Madras Music Season', ['Tamil Nadu'], 45, 100000, 15000000, 0.85, 0.8, 12, 'Carnatic music festival'),
            ('Hampi Utsav', ['Karnataka'], 3, 50000, 5000000, 0.8, 0.85, 11, 'Cultural festival at Hampi'),
            ('Moatsu Festival', ['Nagaland'], 3, 20000, 2000000, 0.75, 0.8, 5, 'Ao tribe festival')
        ]
        print("Loading comprehensive Festivals data...")
        self.cursor.execute("TRUNCATE TABLE FESTIVALS")
        for data in festivals_data:
            festival, states, duration, visitors, impact, cultural, tourism, month, description = data
            if states == ['All States']:
                states_list = ['Delhi', 'Maharashtra', 'Karnataka', 'Tamil Nadu', 'West Bengal',
                               'Gujarat', 'Rajasthan', 'Uttar Pradesh', 'Kerala', 'Punjab',
                               'Andhra Pradesh', 'Telangana', 'Bihar', 'Madhya Pradesh', 'Assam']
            else:
                states_list = states
            for state in states_list:
                query = """
                        INSERT INTO FESTIVALS (festival, state, duration_days, expected_visitors,
                                               economic_impact, cultural_significance_score,
                                               tourism_potential_score, month)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) \
                        """
                self.cursor.execute(query, (festival, state, duration, visitors // len(states_list),impact // len(states_list), cultural, tourism, month))
        self.conn.commit()
        print(f"Loaded {len(festivals_data)} festivals")
    def create_additional_tables(self):
        print("Creating additional tables for enhanced data...")
        self.cursor.execute("""
                            CREATE TABLE IF NOT EXISTS ART_FORM_DETAILS
                            (
                                detail_id
                                INT
                                AUTOINCREMENT
                                PRIMARY
                                KEY,
                                art_form_id
                                INT,
                                description
                                TEXT,
                                learning_centers
                                INT,
                                govt_support
                                BOOLEAN,
                                international_recognition
                                BOOLEAN,
                                avg_income_practitioner
                                INT,
                                youth_participation_rate
                                FLOAT,
                                created_at
                                TIMESTAMP
                                DEFAULT
                                CURRENT_TIMESTAMP
                            (
                            ),
                                FOREIGN KEY
                            (
                                art_form_id
                            ) REFERENCES ART_FORMS
                            (
                                art_form_id
                            )
                                )
                            """)
        self.cursor.execute("""
                            CREATE TABLE IF NOT EXISTS TOURISM_FACILITIES
                            (
                                facility_id
                                INT
                                AUTOINCREMENT
                                PRIMARY
                                KEY,
                                site_name
                                VARCHAR
                            (
                                200
                            ),
                                hotels_nearby INT,
                                restaurants_nearby INT,
                                guides_available BOOLEAN,
                                audio_guide_available BOOLEAN,
                                parking_capacity INT,
                                wheelchair_accessible BOOLEAN,
                                public_transport_available BOOLEAN,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            (
                            )
                                )
                            """)
        self.cursor.execute("""
                            CREATE TABLE IF NOT EXISTS CULTURAL_EVENTS
                            (
                                event_id
                                INT
                                AUTOINCREMENT
                                PRIMARY
                                KEY,
                                event_name
                                VARCHAR
                            (
                                200
                            ),
                                event_type VARCHAR
                            (
                                50
                            ),
                                state VARCHAR
                            (
                                100
                            ),
                                city VARCHAR
                            (
                                100
                            ),
                                start_date DATE,
                                end_date DATE,
                                expected_attendance INT,
                                ticket_price INT,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            (
                            )
                                )
                            """)
        self.conn.commit()
        print("Additional tables created successfully")

    def close(self):
        self.cursor.close()
        self.conn.close()
        print("Connection closed")
def main():
    print("Starting to load enhanced cultural heritage data into Snowflake...")
    loader = EnhancedDataLoader()
    try:
        loader.create_additional_tables()
        loader.load_comprehensive_art_forms()
        loader.load_comprehensive_cultural_sites()
        loader.load_comprehensive_tourism_data()
        loader.load_comprehensive_festivals()
        print("\nAll enhanced data loaded successfully!")
        print("\nData Summary:")
        print("- Art Forms: 68 entries covering all major Indian art forms")
        print("- Cultural Sites: 60+ entries including all 40 UNESCO sites")
        print("- Tourism Data: 3 years of monthly data for 30 major sites")
        print("- Festivals: 37 major festivals with regional variations")
    except Exception as e:
        print(f"Error loading data: {e}")
    finally:
        loader.close()
if __name__ == "__main__":
    main()