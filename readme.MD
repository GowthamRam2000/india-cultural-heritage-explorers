🎭 India Cultural Heritage Explorer
An interactive data-driven platform that bridges art, culture, and tourism with technology, built for the Snowflake | YourStory Hero Challenge 2025.
🌟 Overview
India Cultural Heritage Explorer is a comprehensive solution that showcases traditional art forms, uncovers cultural experiences, and promotes responsible tourism across India. Built with Snowflake and Streamlit, it provides data-driven insights to preserve cultural treasures while enhancing traveler experiences.
🎯 Problem Statement
India's rich cultural heritage faces several challenges:

Endangered Art Forms: Many traditional art forms are at risk with declining practitioners
Undiscovered Gems: Lesser-known cultural sites remain unexplored due to lack of awareness
Unsustainable Tourism: Popular sites face overcrowding while hidden gems remain underutilized
Limited Digital Presence: Many heritage sites lack digital documentation and accessibility

💡 Solution
Our platform addresses these challenges by:

Comprehensive Database: Cataloging 68+ traditional art forms and 60+ cultural sites
Real-time Analytics: Tracking tourism patterns and sustainability metrics
Smart Recommendations: AI-powered suggestions for hidden gems and sustainable tourism
Interactive Visualizations: Maps, charts, and insights for data-driven decision making

🛠️ Technology Stack

Data Platform: Snowflake
Frontend: Streamlit
Visualization: Plotly, Folium, Altair
Data Processing: Pandas, NumPy, Scikit-learn
Languages: Python 3.9+

📊 Key Features
1. Interactive Dashboard

Real-time metrics on endangered art forms
UNESCO site statistics
Tourism revenue and sustainability scores

2. Cultural Maps

Traditional art forms distribution with risk levels
Heritage sites with UNESCO status
Tourism density heatmaps
Recommended cultural routes

3. Analytics Suite

Tourism trend analysis
Seasonal pattern identification
Digital presence scoring
Festival impact assessment

4. Smart Recommendations

Personalized cultural journey planning
Hidden gems discovery
Sustainable tourism suggestions
Similar destination finder

5. Data-Driven Insights

State-wise heritage index
Conservation priorities
Revenue optimization strategies

🚀 Getting Started
Prerequisites

Python 3.9+
Snowflake account
Git

Installation

Clone the repository

bashgit clone https://github.com/GowthamRam2000/india-cultural-heritage-explorers.git
cd india-cultural-heritage-explorer

Create virtual environment

bashpython -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

Install dependencies

bashpip install -r requirements.txt

Configure Snowflake

bash# Update .env file with your Snowflake credentials
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password

Load data into Snowflake

bashpython load_enhanced_data_to_snowflake.py

Run the application

bashstreamlit run app.py
📁 Project Structure
india-cultural-heritage-explorer/
├── app.py                    # Main application
├── config.py                 # Configuration settings
├── requirements.txt          # Dependencies
├── data/
│   ├── data_loader.py       # Data loading utilities
│   └── data_processor.py    # Data processing logic
├── components/
│   ├── maps.py              # Map visualizations
│   ├── analytics.py         # Analytics components
│   └── recommendations.py   # Recommendation engine
├── utils/
│   └── helpers.py           # Helper functions
└── assets/
    └── styles.css           # Custom styling
📈 Data Sources

Art Forms: 68 traditional art forms across 28 states
Cultural Sites: 60+ sites including all 40 UNESCO World Heritage Sites
Tourism Data: 3 years of monthly visitor data for 30 major sites
Festivals: 37 major festivals with economic impact data

🎯 Impact

Cultural Preservation: Identifies 24 endangered art forms requiring immediate attention
Tourism Optimization: Promotes 10+ hidden gems to reduce overcrowding
Economic Growth: Potential to increase tourism revenue by 20% through better distribution
Digital Transformation: Improves digital presence for underserved cultural sites

🤝 Contributing
We welcome contributions! Please see our Contributing Guidelines for details.
📄 License
This project is licensed under the MIT License - see the LICENSE file for details.
🙏 Acknowledgments

Ministry of Culture, Government of India
UNESCO World Heritage Centre
Indian Tourism Statistics
Traditional art form practitioners across India

📧 Contact
For queries, reach out to: [gowthamm161@gmail.com]

Built with ❤️ for the Snowflake | YourStory Hero Challenge 2025
