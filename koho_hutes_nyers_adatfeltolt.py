import pandas as pd
import sqlite3

# 1. Adatok beolvasása CSV fájlból
df= pd.read_csv (r'E:\egyetem\3. FÉLÉV - TÁRGYAK\Adatbázisok (6)\Hutopanelek.csv')
# 2. Adatok írása SQLite adatbázisba
conn = sqlite3.connect(r'E:\egyetem\3. FÉLÉV - TÁRGYAK\Adatbázisok (6)\hf_gyak\koho_hutes.db')
df.to_sql(name='Hutopanelek', con=conn, if_exists='replace', index=False)

# 3. Módosítások mentése
conn.commit()

# 4. Adatok lekérdezése az adatbázisból
df_updated = pd.read_sql_query("""
SELECT *
FROM Hutopanelek
""", con=conn)

# 5. Excel fájlba írás
df_updated.to_excel(r'E:\egyetem\3. FÉLÉV - TÁRGYAK\Adatbázisok (6)\hf_gyak\huto_vissza_output.xlsx', index=False)

# 6. Kapcsolat bezárása
conn.close()
