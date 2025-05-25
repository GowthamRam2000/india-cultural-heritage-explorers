import snowflake.connector
from config import SNOWFLAKE_CONFIG
import pandas as pd


def verify_data():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()

    tables = ['ART_FORMS', 'CULTURAL_SITES', 'TOURISM_DATA', 'FESTIVALS']

    print("=== Snowflake Data Verification ===\n")

    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{table}: {count} records")

        cursor.execute(f"SELECT * FROM {table} LIMIT 5")
        df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        print(f"\nSample data from {table}:")
        print(df)
        print("-" * 80)

    print("\n=== Data Statistics ===")

    cursor.execute("""
                   SELECT risk_level, COUNT(*) as count
                   FROM ART_FORMS
                   GROUP BY risk_level
                   """)
    print("\nArt Forms by Risk Level:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]}")

    cursor.execute("""
                   SELECT unesco_status, COUNT(*) as count
                   FROM CULTURAL_SITES
                   GROUP BY unesco_status
                   """)
    print("\nCultural Sites by UNESCO Status:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]}")

    cursor.execute("""
                   SELECT state,
                          COUNT(DISTINCT art_form_id) as art_forms,
                          COUNT(DISTINCT site_id)     as sites,
                          COUNT(DISTINCT festival_id) as festivals
                   FROM (SELECT state, art_form_id, NULL as site_id, NULL as festival_id
                         FROM ART_FORMS
                         UNION ALL
                         SELECT state, NULL, site_id, NULL
                         FROM CULTURAL_SITES
                         UNION ALL
                         SELECT state, NULL, NULL, festival_id
                         FROM FESTIVALS)
                   GROUP BY state
                   ORDER BY state LIMIT 10
                   """)
    print("\nCultural Heritage by State (Top 10):")
    print(f"{'State':<20} {'Art Forms':<12} {'Sites':<12} {'Festivals':<12}")
    print("-" * 60)
    for row in cursor.fetchall():
        print(f"{row[0]:<20} {row[1]:<12} {row[2]:<12} {row[3]:<12}")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    verify_data()