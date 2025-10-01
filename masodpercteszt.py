import pandas as pd

df = pd.read_csv(r"E:\egyetem\3. FÉLÉV - TÁRGYAK\Adatbázisok (6)\hf_gyak\Hutopanelek.csv", sep="\t", nrows=5)

print(df.iloc[:, 0])  # első oszlop első 5 értéke

