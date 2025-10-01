import pandas as pd
import sqlite3

# 1. Adatok beolvasása CSV fájlból
df = pd.read_csv(
    r'E:\egyetem\3. FÉLÉV - TÁRGYAK\Adatbázisok (6)\hf_gyak\Hutopanelek.csv',
    sep="\t",             # TAB szeparátor
    decimal=",",          # vessző a tizedesjel
    encoding="utf-8"      # biztos, ami biztos
)
# Nézzük meg az oszlopneveket
print(df_updated.columns)

# Például, ha a 'Panel hőfok 3 [°C] Time' az elsődleges időoszlop:
df_time_only = df_updated.drop_duplicates(subset=['Panel hőfok 3 [°C] Time'])



