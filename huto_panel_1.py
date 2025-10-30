import csv
import sqlite3
import re

DB_NAME = r'E:\egyetem\3. FÉLÉV - TÁRGYAK\Adatbázisok (6)\hf_gyak\projekt_adatbazis.db'
HUTOPANELEK_FILE = r'E:\egyetem\3. FÉLÉV - TÁRGYAK\Adatbázisok (6)\hf_gyak\Hutopanelek.csv'
ADAGOK_FILE = r'E:\egyetem\3. FÉLÉV - TÁRGYAK\Adatbázisok (6)\hf_gyak\Adagok.csv'


def initialize_database(conn):
    """Létrehozza (és törli!) a táblákat."""
    cursor = conn.cursor()

    # 1. KÖTELEZŐ: Töröljük a táblákat, hogy ne halmozódjon fel az adat
    drop_tables_sql = """
                      DROP TABLE IF EXISTS Homerseklet_Meresek;
                      DROP TABLE IF EXISTS Adag;
                      DROP TABLE IF EXISTS Panel;
                      """
    cursor.executescript(drop_tables_sql) # <--- Ezt a futtatást felejtette el

    # Létrehozó SQL-t itt kell futtatni (lásd 3.1. pont)
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
        # Az Adagok fájl oszlopai: ADAGSZUM;Kezdet_DÁTUM;Kezdet_IDŐ;Vége_DÁTUM;Vége_IDŐ;ADAGKÖZI IDŐ;ADAGIDŐ
        reader = csv.reader(f, delimiter=';')
        header = next(reader)

        adag_data = []
        for row in reader:
            if not row: continue
            try:
                # Dátum/Idő oszlopok összefűzése:
                kezdet_dt = f"{row[1]} {row[2]}"  # Kezdet_DÁTUM Kezdet_IDŐ
                vege_dt = f"{row[3]} {row[4]}"  # Vége_DÁTUM Vége_IDŐ

                # A row[0] az ADAGSZUM, row[5] az ADAGKÖZI IDŐ, row[6] az ADAGIDŐ
                adag_data.append((
                    int(row[0]),
                    kezdet_dt,
                    vege_dt,
                    row[5],
                    row[6]
                ))
            except ValueError as e:
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

        # 1. Definiáljuk a panel sorszámát és a hozzá tartozó adat ('ValueY') oszlopindexét a CSV-ben.
        # A Panel 7 HIÁNYZIK, az indexek ennek megfelelően vannak definiálva.
        # Indexek: (Panel_szam: ValueY_index)
        panel_oszlopok = {
            1: 1, 2: 3, 3: 5, 4: 7, 5: 9, 6: 11,
            8: 13, 9: 15, 10: 17, 11: 19, 12: 21, 13: 23, 14: 25, 15: 27
        }

        insert_sql = """
                     INSERT INTO Homerseklet_Meresek 
                     (Meres_Idopont, Panel_Szam_FK, Homerseklet) 
                     VALUES (?, ?, ?)
                     """

        try:
            with open(HUTOPANELEK_FILE, 'r', encoding='utf-8') as f:
                csv_reader = csv.reader(f, delimiter=';')

                # Fejléc kihagyása
                next(csv_reader)

                for row in csv_reader:
                    for panel_szam, value_index in panel_oszlopok.items():

                        time_index = value_index - 1

                        meres_idopont = row[time_index]
                        hofok_szoveg = row[value_index]

                        # 🚨 JAVÍTÁS: Tizedesvessző cseréje tizedespontra, hogy számként kezelje az SQL
                        hofok = hofok_szoveg.replace(',', '.')

                        # Ellenőrzés, hogy ne üres értékeket szúrjunk be
                        if meres_idopont and hofok:
                            cursor.execute(insert_sql, (meres_idopont, panel_szam, hofok))

                conn.commit()
                print("Hutopanelek adatai sikeresen betöltve a Homerseklet_Meresek táblába.")

        except Exception as e:
            print(f"Hiba történt a Hutopanelek betöltése során: {e}")
            conn.rollback()

    if not panel_szamok_list:
        print("Nincsenek panel adatok a folytatáshoz.")
        return

    # --- 2. Mérési adatok UNPIVOT-olása (A fájl újraolvasása - INDEXELÉS FIX) ---
    meres_data = []
    panel_columns = {}  # {panel_szam: (time_index, valueY_index)}

    # Keresünk indexeket a kinyert panel_szamok alapján.
    for p_num in panel_szamok_list:
        time_idx = -1
        value_idx = -1

        # A legrobusztusabb keresési minta, ami csak a számra és a Time/ValueY-ra támaszkodik:
        time_pattern = f"{p_num} [°C] Time"
        value_pattern = f"{p_num} [°C] ValueY"

        # --- 2. Mérési adatok UNPIVOT-olása (A fájl újraolvasása - INDEXELÉS FIX) ---
        meres_data = []
        panel_columns = {}  # {panel_szam: (time_index, valueY_index)}

        # Keresünk indexeket a kinyert panel_szamok alapján.
        for p_num in panel_szamok_list:
            time_idx = -1
            value_idx = -1

            # A keresés stringje:
            # PÉLDÁUL: "Panel hofok 1 [°C] Time"
            time_needle = f"Time"
            value_needle = f"ValueY"
            num_needle = f" {p_num} ["  # Panel számot a szögletes zárójel előtt keressük

            # Végigmegyünk a fejléceken és KÉZZEL keressük meg az indexeket!
            for i, col_name in enumerate(header):
                # A time oszlopnak tartalmaznia kell a panel számot, és a 'Time' szót
                if col_name.find(num_needle) != -1 and col_name.find(time_needle) != -1:
                    time_idx = i

                # A value oszlopnak tartalmaznia kell a panel számot, és a 'ValueY' szót
                if col_name.find(num_needle) != -1 and col_name.find(value_needle) != -1:
                    value_idx = i

            if time_idx != -1 and value_idx != -1:
                panel_columns[p_num] = (time_idx, value_idx)
            else:
                # Ez a figyelmeztetés már nem fut le a sikeres indexelés után.
                print(f"Figyelem: Hiányzik a(z) {p_num} panel valamelyik oszlopa (Indexelés hiba).")
                continue

    try:
        # Fájl újbóli megnyitása a mérések olvasásához
        with open(HUTOPANELEK_FILE, 'r', encoding='cp1250') as f:
            reader = csv.reader(f, delimiter=';')
            next(reader)  # Átugorjuk a fejlécet, a reader most az első adatsornál áll!

            for row in reader:
                if not row: continue

                # UNPIVOT (oszlopokból sorok)
                for p_num, (time_idx, value_idx) in panel_columns.items():
                    try:
                        meres_idopont = row[time_idx]
                        hofok_ertek_str = row[value_idx].replace(',', '.')  # Vessző cseréje pontra
                        hofok_ertek = float(hofok_ertek_str)

                        # (Időpont, Panel_Szam_FK, Hofok_Ertek)
                        meres_data.append((meres_idopont, p_num, hofok_ertek))
                    except (IndexError, ValueError):
                        pass

        # Homerseklet_Meresek tábla feltöltése
        cursor.executemany("""
                           INSERT
                           OR IGNORE INTO Homerseklet_Meresek (Meres_Idopont, Panel_Szam_FK, Hofok_Ertek)
                           VALUES (?, ?, ?)
                           """, meres_data)
        conn.commit()
        print(f"{len(meres_data)} mérési adat rekord feltöltve.")

    except FileNotFoundError:
        print(f"Hiba: A fájl ({HUTOPANELEK_FILE}) nem található.")


def update_adag_fk(conn):
    """
    Frissíti a Homerseklet_Meresek táblát az Adag_Szam_FK-val
    a mérési időpont és az adag időintervalluma alapján, egyetlen
    optimalizált SQL paranccsal.
    """
    print("Adag FK frissítése időintervallum alapján (Optimalizált SQL)...")
    cursor = conn.cursor()

    # EGYETLEN SQL UPDATE utasítás korrelált allekérdezéssel
    # DATETIME(REPLACE(..., '.', '-')) biztosítja a korrekt dátumkezelést SQLite-ban
    update_sql = """
                 UPDATE Homerseklet_Meresek
                 SET Adag_Szam_FK = (SELECT Adag_Szam \
                                     FROM Adag \
                                     WHERE DATETIME(REPLACE(Homerseklet_Meresek.Meres_Idopont, '.', '-')) \
                                               BETWEEN DATETIME(REPLACE(Adag.Kezdet_Idopont, '.', '-')) \
                                               AND DATETIME(REPLACE(Adag.Vege_Idopont, '.', '-')))
                 WHERE Adag_Szam_FK IS NULL; -- Csak az üreseket frissítjük
                 """

    cursor.execute(update_sql)
    conn.commit()

    # Ellenőrizzük, hány sor frissült sikeresen
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