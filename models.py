import sqlite3

def get_db_connection():
    """
    Establishes a connection to the SQLite database.
    """
    return sqlite3.connect("metadata.db")

def create_tables():
    """
    Creates the necessary tables in the SQLite database.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS Studies;")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Studies (
            StudyInstanceUID TEXT PRIMARY KEY,
            PatientID TEXT,
            StudyDate TEXT,
            FOREIGN KEY (PatientID) REFERENCES Patients (PatientID)
        );
    """)

    cursor.execute("DROP TABLE IF EXISTS Series;")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Series (
            SeriesUID TEXT PRIMARY KEY,
            StudyInstanceUID TEXT,
            SliceThickness TEXT,
            PixelSpacing TEXT,
            FOREIGN KEY (StudyInstanceUID) REFERENCES Studies (StudyInstanceUID)
        );
    """)

    cursor.execute("DROP TABLE IF EXISTS Patients;")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Patients (
            PatientID TEXT PRIMARY KEY,
            Name TEXT,
            BirthDate TEXT
        );
    """)

    conn.commit()
    conn.close()
    print("Tables created successfully!")

if __name__ == "__main__":
    create_tables()
