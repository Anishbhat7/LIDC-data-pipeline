import sqlite3

def create_database():
    # Connect to SQLite database (creates file if not exists)
    conn = sqlite3.connect("metadata.db")
    cursor = conn.cursor()

    # Create tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Patients (
            PatientID TEXT PRIMARY KEY,
            PatientName TEXT,
            BirthDate TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Studies (
            StudyInstanceUID TEXT PRIMARY KEY,
            PatientID TEXT,
            StudyDate TEXT,
            FOREIGN KEY (PatientID) REFERENCES Patients(PatientID)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Series (
            SeriesInstanceUID TEXT PRIMARY KEY,
            StudyInstanceUID TEXT,
            SeriesDescription TEXT,
            NumberOfSlices INTEGER,
            SliceThickness REAL,
            FOREIGN KEY (StudyInstanceUID) REFERENCES Studies(StudyInstanceUID)
        )
    """)

    # Commit and close
    conn.commit()
    conn.close()
    print("Database setup completed!")

if __name__ == "__main__":
    create_database()
