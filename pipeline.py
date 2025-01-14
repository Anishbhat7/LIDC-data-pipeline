from db_operations import insert_patient, insert_study, insert_series

def test_database():
    # Insert a patient
    insert_patient("P001", "John Doe", "1980-01-01")

    # Insert a study for the patient
    insert_study("ST001", "P001", "2025-01-13")

    # Insert a series for the study
    insert_series("SR001", "ST001", "CT Chest", 150, 1.25)

    print("Test data inserted successfully!")

if __name__ == "__main__":
    test_database()