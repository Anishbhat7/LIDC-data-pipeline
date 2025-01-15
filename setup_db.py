import sqlite3

def create_db():
    # conn = sqlite3.connect('metadata.db')
    conn = sqlite3.connect('slices.db')
    cursor = conn.cursor()

    # Drop existing tables if they exist to avoid errors when re-creating them
    cursor.execute('DROP TABLE IF EXISTS Patients')
    cursor.execute('DROP TABLE IF EXISTS Studies')
    cursor.execute('DROP TABLE IF EXISTS Series')
    cursor.execute('DROP TABLE IF EXISTS Slices')

    # Create the Patients table
    cursor.execute(
        '''
            CREATE TABLE IF NOT EXISTS Patients (
                patient_id TEXT PRIMARY KEY,
                patient_name TEXT,
                patient_birthdate TEXT
            )
        '''
    )

    # Create the Studies table
    cursor.execute(
        '''
            CREATE TABLE IF NOT EXISTS Studies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                study_instance_uid TEXT UNIQUE,
                study_date TEXT,
                patient_id INTEGER,
                FOREIGN KEY (patient_id) REFERENCES Patients(id)
            )
        '''
    )

    # Create the Series table
    cursor.execute(
        '''
            CREATE TABLE IF NOT EXISTS Series (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                series_instance_uid TEXT UNIQUE,
                num_slices INTEGER,
                study_instance_uid INTEGER,
                FOREIGN KEY (study_instance_uid) REFERENCES Studies(id)
            )
        '''
    )

    cursor.execute(
        '''
            CREATE TABLE IF NOT EXISTS Slices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_id TEXT,
                study_instance_uid TEXT,
                series_instance_uid TEXT,
                slice_thickness TEXT,
                pixel_spacing TEXT,
                study_date TEXT,
                acquisition_date_time TEXT,
                FOREIGN KEY (patient_id) REFERENCES Patients(patient_id),
                FOREIGN KEY (study_instance_uid) REFERENCES Studies(study_instance_uid),
                FOREIGN KEY (series_instance_uid) REFERENCES Series(series_instance_uid)
            )
        '''
    )

    conn.commit()
    return conn, cursor