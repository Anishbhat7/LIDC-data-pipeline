import sqlite3

def get_db_connection():
    """
    Establishes a connection to the SQLite database.
    """
    return sqlite3.connect("metadata.db")

def insert_patient(patient_id, name, birth_date):
    """
    Inserts a patient record into the Patients table if it doesn't already exist.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if the patient already exists before inserting
        cursor.execute("SELECT 1 FROM Patients WHERE PatientID = ?", (patient_id,))
        if cursor.fetchone():
            print(f"Patient {patient_id} already exists.")
            return False

        cursor.execute("""INSERT INTO Patients (PatientID, Name, BirthDate)
                          VALUES (?, ?, ?)""", 
                          (patient_id or None, name or None, birth_date or None))

        conn.commit()
        print(f"Inserted patient {patient_id}")
        return cursor.rowcount > 0  # Returns True if a row was inserted

    except sqlite3.Error as e:
        print(f"Error inserting patient {patient_id}: {e}")
        return False
    finally:
        conn.close()

def insert_study(study_uid, patient_id, study_date):
    """
    Inserts a study record into the Studies table if it doesn't already exist.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if the study already exists before inserting
        cursor.execute("SELECT 1 FROM Studies WHERE StudyInstanceUID = ?", (study_uid,))
        if cursor.fetchone():
            print(f"Study {study_uid} already exists.")
            return False

        cursor.execute("""INSERT INTO Studies (StudyInstanceUID, PatientID, StudyDate)
                          VALUES (?, ?, ?)""", 
                          (study_uid or None, patient_id or None, study_date or None))

        conn.commit()
        print(f"Inserted study {study_uid}")
        return cursor.rowcount > 0  # Returns True if a row was inserted

    except sqlite3.Error as e:
        print(f"Error inserting study {study_uid}: {e}")
        return False
    finally:
        conn.close()

def insert_series(series_uid, study_uid, slice_thickness, pixel_spacing):
    """
    Inserts a series record into the Series table if it doesn't already exist.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if the series already exists before inserting
        cursor.execute("SELECT 1 FROM Series WHERE SeriesUID = ?", (series_uid,))
        if cursor.fetchone():
            print(f"Series {series_uid} already exists.")
            return False

        cursor.execute("""INSERT INTO Series (SeriesUID, StudyInstanceUID, SliceThickness, PixelSpacing)
                          VALUES (?, ?, ?, ?)""", 
                          (series_uid or None, study_uid or None, slice_thickness or None, pixel_spacing or None))

        conn.commit()
        print(f"Inserted series {series_uid}")
        return cursor.rowcount > 0  # Returns True if a row was inserted

    except sqlite3.Error as e:
        print(f"Error inserting series {series_uid}: {e}")
        return False
    finally:
        conn.close()
