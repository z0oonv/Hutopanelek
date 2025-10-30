import pandas as pd
import sqlite3

# Adatbázis létrehozása
conn = sqlite3.connect(r'C:\Users\zamb\Downloads\koho_hutes_1030_1.db')

# --- Adagok beolvasása ---
df_adag = pd.read_csv(r'C:\Users\zamb\Documents\egyetem\dtbs\Adagok.csv', sep=";", encoding="cp1250", on_bad_lines="skip")
df_adag.columns = ["adag_id", "kezdet_datum", "kezdet_ido", "vege_datum", "vege_ido", "adagkozi_ido", "adagido"]
df_adag.to_sql("adag", conn, if_exists="replace", index=False)

# --- Panel adatok létrehozása (kézzel) ---
panel_ids = [1,2,3,4,5,6,8,9,10,11,12,13,14,15]
pd.DataFrame({'panel_id': panel_ids, 'nev': [f'Panel {i}' for i in panel_ids]}).to_sql('panel', conn, if_exists='replace', index=False)

# --- Hőmérséklet adatok betöltése ---
df_hofok = pd.read_csv(r'C:\Users\zamb\Documents\egyetem\dtbs\'Panel_hofok.csv', sep=";", encoding="cp1250")
records = []
for pid in panel_ids:
    t_col = f'Panel hőfok {pid} [°C] Time'
    v_col = f'Panel hőfok {pid} [°C] ValueY'
    if t_col in df_hofok.columns:
        for t, v in zip(df_hofok[t_col], df_hofok[v_col]):
            if pd.notna(v):
                records.append((pid, t, float(str(v).replace(",", "."))))
pd.DataFrame(records, columns=["panel_id", "idopont", "homerseklet"]).to_sql("homerseklet", conn, if_exists="replace", index=False)

# Nézet létrehozása
conn.execute("""
CREATE VIEW adag_homerseklet AS
SELECT 
    a.adag_id,
    p.panel_id,
    h.idopont,
    h.homerseklet
FROM homerseklet h
JOIN panel p ON h.panel_id = p.panel_id
JOIN adag a
  ON h.idopont BETWEEN 
     datetime(a.kezdet_datum || ' ' || a.kezdet_ido) 
     AND datetime(a.vege_datum || ' ' || a.vege_ido);
""")

conn.commit()
conn.close()

