import os
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT', 'your_account'),
    'user': os.getenv('SNOWFLAKE_USER', 'your_user'),
    'password': os.getenv('SNOWFLAKE_PASSWORD', 'your_password'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
    'database': os.getenv('SNOWFLAKE_DATABASE', 'CULTURAL_HERITAGE_DB'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
}

APP_CONFIG = {
    'title': 'India Cultural Heritage Explorer',
    'page_icon': '🎭',
    'layout': 'wide',
    'initial_sidebar_state': 'expanded'
}

DATA_SOURCES = {
    'art_forms': 'https://data.gov.in/catalog/traditional-art-forms',
    'tourism_stats': 'https://data.gov.in/catalog/tourism-statistics',
    'cultural_sites': 'https://data.gov.in/catalog/cultural-heritage-sites'
}