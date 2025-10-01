import pandas as pd
import sqlite3

# 1. Adatok beolvasása CSV fájlból
df = pd.read_csv(
    r'E:\egyetem\3. FÉLÉV - TÁRGYAK\Adatbázisok (6)\hf_gyak\Hutopanelek.csv',
    sep="\t",
    decimal=",",
    encoding="utf-8"
)

# 2. Keresd ki az összes "Time" oszlopot
time_cols = [col for col in df.columns if "Time" in col]
print("Time oszlopok:", time_cols)

# 3. Alakítsd át datetime formátumba
for col in time_cols:
    df[col] = pd.to_datetime(df[col], errors='coerce', format='%Y.%m.%d %H:%M:%S')

# 4. Készíts egy egységes "Timestamp" oszlopot az első time col alapján
df["Timestamp"] = df[time_cols[0]]

# 5. Duplikált sorok kiszűrése időbélyeg alapján
df = df.drop_duplicates(subset=["Timestamp"])

# 6. Írás SQLite adatbázisba
with sqlite3.connect(r'E:\egyetem\3. FÉLÉV - TÁRGYAK\Adatbázisok (6)\hf_gyak\koho_hutes.db') as conn:
    df.to_sql(name='Hutopanelek', con=conn, if_exists='replace', index=False)

# 7. Excel export
df.to_excel(
    r'E:\egyetem\3. FÉLÉV - TÁRGYAK\Adatbázisok (6)\hf_gyak\huto_vissza_output.xlsx',
    index=False
)
