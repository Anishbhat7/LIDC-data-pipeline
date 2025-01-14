import sqlite3
from extract_metadata import extract_metadata
from config import DB_PATH
import pandas as pd

def store_metadata(df):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Insert data into tables
    for _, row in df.iterrows():
        cursor.execute("INSERT OR IGNORE INTO patients VALUES (?)", (row["PatientID"],))
        cursor.execute("INSERT OR IGNORE INTO studies VALUES (?, ?, ?)", (row["StudyInstanceUID"], row["PatientID"], row["StudyDate"]))
        cursor.execute(
            "INSERT OR IGNORE INTO series VALUES (?, ?, ?, ?, ?)",
            (row["SeriesInstanceUID"], row["StudyInstanceUID"], row["NumberOfSlices"], row["SliceThickness"], row["PixelSpacing"]),
        )
    conn.commit()
    conn.close()

if __name__ == "__main__":
    df = pd.read_csv("metadata.csv")
    store_metadata(df)
