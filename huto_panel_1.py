import csv
import sqlite3
import re

DB_NAME = r'E:\egyetem\3. FÉLÉV - TÁRGYAK\Adatbázisok (6)\hf_gyak\projekt_adatbazis.db'
HUTOPANELEK_FILE = r'E:\egyetem\3. FÉLÉV - TÁRGYAK\Adatbázisok (6)\hf_gyak\Hutopanelek.csv'
ADAGOK_FILE = r'E:\egyetem\3. FÉLÉV - TÁRGYAK\Adatbázisok (6)\hf_gyak\Adagok.csv'


def initialize_database(conn):
    """Létrehozza a táblákat."""
    cursor = conn.cursor()
    # Létrehozó SQL-t itt kell futtatni (lásd 3.1. pont)
    create_tables_sql = """
                        CREATE TABLE IF NOT EXISTS Panel \
                        ( \
                            Panel_Szam \
                            INTEGER \
                            PRIMARY \
                            KEY \
                        );
                        CREATE TABLE IF NOT EXISTS Adag \
                        ( \
                            Adag_Szam \
                            INTEGER \
                            PRIMARY \
                            KEY, \
                            Kezdet_Idopont \
                            TEXT \
                            NOT \
                            NULL, \
                            Vege_Idopont \
                            TEXT \
                            NOT \
                            NULL, \
                            Adag_Kozti_Ido \
                            TEXT, \
                            Adag_Ido \
                            TEXT
                        );
                        CREATE TABLE IF NOT EXISTS Homerseklet_Meretek \
                        ( \
                            Meret_Id \
                            INTEGER \
                            PRIMARY \
                            KEY \
                            AUTOINCREMENT, \
                            Meret_Idopont \
                            TEXT \
                            NOT \
                            NULL, \
                            Panel_Szam_FK \
                            INTEGER \
                            NOT \
                            NULL, \
                            Hofok_Ertek \
                            REAL, \
                            Adag_Szam_FK \
                            INTEGER, \
                            FOREIGN \
                            KEY \
                        ( \
                            Panel_Szam_FK \
                        ) REFERENCES Panel \
                        ( \
                            Panel_Szam \
                        ),
                            FOREIGN KEY \
                        ( \
                            Adag_Szam_FK \
                        ) REFERENCES Adag \
                        ( \
                            Adag_Szam \
                        )
                            ); \
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
    """Beolvassa, UNPIVOT-olja és feltölti a Panel és Homerseklet_Meretek táblákat."""
    print("Hűtőpanelek adatainak normalizálása és feltöltése...")

    meres_data = []

    with open(HUTOPANELEK_FILE, 'r', encoding='cp1250') as f:
        reader = csv.reader(f, delimiter=';')
        header = next(reader)

        # 1. Panel tábla feltöltése (Panel_Szam kinyerése a fejlécekből)
        panel_szamok = set()
        for col_index, col_name in enumerate(header):
            # Csak a 'Time' oszlopokból vonjuk ki a számot
            if "Panel hőfok" in col_name and "Time" in col_name:
                try:
                    # Keresi a számot a "Panel hőfok " és a " [" között
                    match = re.search(r"Panel hőfok (\d+) \[", col_name)

                    if match:
                        # A match.group(1) tartalmazza a megtalált számot (a panel azonosítót)
                        panel_num = int(match.group(1))
                        panel_szamok.add(panel_num)
                except:
                    continue

        cursor = conn.cursor()
        panel_szamok_list = sorted(list(panel_szamok))
        cursor.executemany("INSERT INTO Panel (Panel_Szam) VALUES (?)", [(p,) for p in panel_szamok_list])
        conn.commit()
        print(f"{len(panel_szamok_list)} panel azonosító feltöltve.")

        # 2. Mérési adatok UNPIVOT-olása
        # Az oszlop indexek csoportosítása (time, valueY)
        # 0: Panel 1 Time, 1: Panel 1 ValueY, 2: Panel 2 Time, 3: Panel 2 ValueY, stb.

        panel_columns = {}  # {panel_szam: (time_index, valueY_index)}
        for p_num in panel_szamok_list:
            # Megkeresi a "Panel X Time" és "Panel X ValueY" oszlopok indexeit
            time_col_name = f"Panel hőfok {p_num} [°C] Time"
            value_col_name = f"Panel hőfok {p_num} [°C] ValueY"
            try:
                time_idx = header.index(time_col_name)
                value_idx = header.index(value_col_name)
                panel_columns[p_num] = (time_idx, value_idx)
            except ValueError:
                print(f"Figyelem: Hiányzik a(z) {p_num} panel valamelyik oszlopa.")

        for row in reader:
            if not row: continue

            # UNPIVOT (oszlopokból sorok)
            for p_num, (time_idx, value_idx) in panel_columns.items():
                try:
                    meret_idopont = row[time_idx]
                    hofok_ertek_str = row[value_idx].replace(',', '.')  # Vessző cseréje pontra
                    hofok_ertek = float(hofok_ertek_str)

                    # (Időpont, Panel_Szam_FK, Hofok_Ertek)
                    meres_data.append((meret_idopont, p_num, hofok_ertek))
                except (IndexError, ValueError) as e:
                    # Hibás vagy hiányzó adat esetén
                    # print(f"Hiba a mérési adatban: {e} - Panel: {p_num}, Sor: {row}")
                    pass  # Adattisztítás része, de most átugorjuk

    cursor.executemany("""
                       INSERT INTO Homerseklet_Meretek (Meret_Idopont, Panel_Szam_FK, Hofok_Ertek)
                       VALUES (?, ?, ?)
                       """, meres_data)
    conn.commit()
    print(f"{len(meres_data)} mérési adat rekord feltöltve.")


def update_adag_fk(conn):
    """
    Frissíti a Homerseklet_Meretek táblát az Adag_Szam_FK-val
    a mérési időpont és az adag időintervalluma alapján.
    """
    print("Adag FK frissítése időintervallum alapján...")
    cursor = conn.cursor()

    # 1. Beolvassuk az adagok időintervallumait
    adagok = cursor.execute("SELECT Adag_Szam, Kezdet_Idopont, Vege_Idopont FROM Adag").fetchall()

    # 2. Beolvassuk a mérési adatok egyedi időpontjait
    meresi_idopontok = cursor.execute("SELECT DISTINCT Meret_Idopont FROM Homerseklet_Meretek").fetchall()

    # 3. Kiszámoljuk a megfeleltetéseket
    update_params = []  # (Adag_Szam_FK, Meret_Idopont)

    for meret_idopont_tuple in meresi_idopontok:
        meret_idopont = meret_idopont_tuple[0]

        # SQL-ben a TEXT típusú DATETIME összehasonlítás működik az "YYYY-MM-DD HH:MM:SS" formátum esetén!
        # A WHERE feltételt SQLite-ban futtatva gyorsabb, mint Pythonban iterálni több ezer/millió soron.

        # Példa lekérdezés:
        # SELECT Adag_Szam FROM Adag WHERE ? BETWEEN Kezdet_Idopont AND Vege_Idopont

        # Mivel sok update lesz, először megkeressük, hogy melyik Adaghoz tartozik az időpont
        # (Feltételezzük, hogy egy időponthoz pontosan egy Adag tartozik)

        matching_adag = cursor.execute("""
                                       SELECT Adag_Szam
                                       FROM Adag
                                       WHERE ? BETWEEN Kezdet_Idopont AND Vege_Idopont
                                       """, (meret_idopont,)).fetchone()

        if matching_adag:
            adag_szam_fk = matching_adag[0]
            update_params.append((adag_szam_fk, meret_idopont))

    # 4. Végrehajtjuk a tömeges frissítést
    cursor.executemany("""
                       UPDATE Homerseklet_Meretek
                       SET Adag_Szam_FK = ?
                       WHERE Meret_Idopont = ?
                       """, update_params)
    conn.commit()
    print(f"{len(update_params)} egyedi időponthoz találtunk Adag FK-t és frissítettünk.")


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