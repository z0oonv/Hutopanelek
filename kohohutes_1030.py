import csv
import sqlite3
import re

# FONTOS JAVÍTÁS: A fájlútvonal szintaktikája javítva
DB_NAME = r'C:\Users\zamb\Downloads\kohohutes_1030.db'
HUTOPANELEK_FILE = r'C:\Users\zamb\Documents\egyetem\dtbs\Hutopanelek.csv'
ADAGOK_FILE = r'C:\Users\zamb\Documents\egyetem\dtbs\Adagok.csv'


def initialize_database(conn):
    """Létrehozza (és törli!) a táblákat."""
    cursor = conn.cursor()

    # 1. KÖTELEZŐ: Töröljük a táblákat, hogy ne halmozódjon fel az adat
    drop_tables_sql = """
                      DROP TABLE IF EXISTS Homerseklet_Meresek;
                      DROP TABLE IF EXISTS Adag;
                      DROP TABLE IF EXISTS Panel;
                      """
    cursor.executescript(drop_tables_sql)

    # Létrehozó SQL-t itt kell futtatni
    create_tables_sql = """
                        CREATE TABLE IF NOT EXISTS Panel 
                        ( 
                            Panel_Szam INTEGER PRIMARY KEY 
                        );
                        CREATE TABLE IF NOT EXISTS Adag 
                        ( 
                            Adag_Szam INTEGER PRIMARY KEY, 
                            Kezdet_Idopont TEXT NOT NULL, 
                            Vege_Idopont TEXT NOT NULL, 
                            Adag_Kozti_Ido TEXT, 
                            Adag_Ido TEXT
                        );
                        CREATE TABLE IF NOT EXISTS Homerseklet_Meresek 
                        ( 
                            Meres_Id INTEGER PRIMARY KEY AUTOINCREMENT, 
                            Meres_Idopont TEXT NOT NULL, 
                            Panel_Szam_FK INTEGER NOT NULL, 
                            Hofok_Ertek REAL, 
                            Adag_Szam_FK INTEGER, 
                            FOREIGN KEY (Panel_Szam_FK) REFERENCES Panel (Panel_Szam),
                            FOREIGN KEY (Adag_Szam_FK) REFERENCES Adag (Adag_Szam)
                        );
                        """
    cursor.executescript(create_tables_sql)
    conn.commit()

    def load_adagok(conn):
        """Beolvassa és feltölti az Adag táblát."""
        print("Adagok feltöltése...")
        with open(ADAGOK_FILE, 'r', encoding='cp1250') as f:
            reader = csv.reader(f, delimiter=';')
            next(reader)

            adag_data = []
            for row in reader:
                if not row or len(row) < 7 or not row[0]: continue

                try:
                    # Dátum/Idő oszlopok összefűzése:
                    kezdet_dt = f"{row[1]} {row[2]}"  # Kezdet_DÁTUM Kezdet_IDŐ
                    vege_dt = f"{row[3]} {row[4]}"  # Vége_DÁTUM Vége_IDŐ

                    # 🚨 KRITIKUS JAVÍTÁS: Normalizálás Adag táblához
                    kezdet_dt_clean = kezdet_dt.replace('.', '-')
                    vege_dt_clean = vege_dt.replace('.', '-')

                    kezdet_dt_final = re.sub(r' (\d):', r' 0\1:', kezdet_dt_clean)
                    vege_dt_final = re.sub(r' (\d):', r' 0\1:', vege_dt_clean)

                    adag_data.append((
                        int(row[0]),
                        kezdet_dt_final,  # Normalizált dátum
                        vege_dt_final,  # Normalizált dátum
                        row[5],
                        row[6]
                    ))
            except (ValueError, IndexError) as e:
                print(f"Hiba az adag adatok feldolgozásában: {e} - Sor: {row}")

    cursor = conn.cursor()
    cursor.executemany("""
                       INSERT OR IGNORE INTO Adag (Adag_Szam, Kezdet_Idopont, Vege_Idopont, Adag_Kozti_Ido, Adag_Ido)
                       VALUES (?, ?, ?, ?, ?)
                       """, adag_data)
    conn.commit()
    print(f"{len(adag_data)} adag rekord feltöltve.")


def load_hutopanelek(conn):
    """Betölti a Hutopanelek.csv fájl adatait a Homerseklet_Meresek táblába."""
    cursor = conn.cursor()

    panel_oszlopok = {
        1: 1, 2: 3, 3: 5, 4: 7, 5: 9, 6: 11,
        8: 13, 9: 15, 10: 17, 11: 19, 12: 21, 13: 23, 14: 25, 15: 27
    }

    insert_sql = """
                 INSERT INTO Homerseklet_Meresek 
                 (Meres_Idopont, Panel_Szam_FK, Hofok_Ertek) 
                 VALUES (?, ?, ?)
                 """
    meres_data = []  # Executemany lista

    try:
        with open(HUTOPANELEK_FILE, 'r', encoding='cp1250') as f:
            csv_reader = csv.reader(f, delimiter=';')
            next(csv_reader)

            for row in csv_reader:
                for panel_szam, value_index in panel_oszlopok.items():

                    time_index = value_index - 1

                    if len(row) > value_index:
                        meres_idopont = row[time_index]
                        hofok_szoveg = row[value_index]

                        # 🚨 KRITIKUS JAVÍTÁS: Dátum normalizálása
                        # 1. Pontok cseréje kötőjelekre
                        meres_idopont_clean = meres_idopont.replace('.', '-')

                        # 2. Óra normalizálása (pl. " 1:" -> " 01:") a string összehasonlításhoz
                        meres_idopont_final = re.sub(r' (\d):', r' 0\1:', meres_idopont_clean)

                        # Tizedesvessző cseréje tizedespontra
                        hofok = hofok_szoveg.replace(',', '.')

                        if meres_idopont_final and hofok:
                            meres_data.append((meres_idopont_final, panel_szam, hofok))

            cursor.executemany(insert_sql, meres_data)
            conn.commit()
            print(f"{len(meres_data)} Hutopanelek adat rekord feltöltve.")

    except Exception as e:
        print(f"Hiba történt a Hutopanelek betöltése során: {e}")
        conn.rollback()

    def update_adag_fk(conn):
        """
        Frissíti a Homerseklet_Meresek táblát az Adag_Szam_FK-val,
        egyszerű, gyors, standardizált string összehasonlítással.
        """
        print("Adag FK frissítése időintervallum alapján (Tiszta SQL)...")
        cursor = conn.cursor()

        # Mivel a Pythonban a dátumok már normailzálva vannak, az összehasonlítás
        # stringként is tökéletesen működik, és sokkal gyorsabb, mint a DATETIME.
        update_sql = """
                     UPDATE Homerseklet_Meresek
                     SET Adag_Szam_FK = (
                         SELECT Adag_Szam
                         FROM Adag
                         WHERE 
                             Homerseklet_Meresek.Meres_Idopont >= Adag.Kezdet_Idopont
                         AND 
                             Homerseklet_Meresek.Meres_Idopont < Adag.Vege_Idopont
                     )
                     WHERE Adag_Szam_FK IS NULL;
                     """

        cursor.execute(update_sql)
        conn.commit()
        # ... (a frissített sorok számlálása)
    frissitett_sorok_szama = \
        cursor.execute("SELECT COUNT(Meres_Id) FROM Homerseklet_Meresek WHERE Adag_Szam_FK IS NOT NULL").fetchone()[0]
    print(f"{frissitett_sorok_szama} mérési adathoz találtunk Adag FK-t és frissítettünk.")


# --- Fő program futtatása ---
if __name__ == '__main__':
    try:
        # Kapcsolódás az adatbázishoz (létrehozza, ha nem létezik)
        conn = sqlite3.connect(DB_NAME)

        # Táblák inicializálása
        initialize_database(conn)

        # Adatok feltöltése
        load_adagok(conn)
        load_hutopanelek(conn)

        # Kapcsolat létrehozása (Adag_Szam_FK frissítése)
        update_adag_fk(conn)

        print("\nAdatbázis feltöltés befejezve! Kérem, folytassa a lekérdezésekkel.")

    except sqlite3.Error as e:
        print(f"SQLite hiba történt: {e}")
    except FileNotFoundError:
        print(f"Hiba: A fájlok ({HUTOPANELEK_FILE} vagy {ADAGOK_FILE}) nem találhatók.")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
