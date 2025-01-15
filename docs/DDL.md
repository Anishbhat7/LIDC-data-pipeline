Database Schema Definition

The database schema is designed to store metadata extracted from DICOM files. The schema consists of four main tables: Patients, Studies, Series, and Slices. These tables are normalized to store metadata related to patients, studies, series, and individual DICOM slices. Each table stores key attributes related to the respective entity, and the relationships between them are represented by foreign keys.
1. Patients Table

This table stores metadata related to the patient, such as the patient ID, name, and birth date.
DDL Statement:

CREATE TABLE IF NOT EXISTS Patients (
    patient_id TEXT PRIMARY KEY,
    patient_name TEXT,
    patient_birthdate TEXT
);

patient_id: Unique identifier for the patient (from DICOM PatientID).
patient_name: Patient's name (from DICOM PatientName).
patient_birthdate: Patient's birth date (from DICOM PatientBirthDate).

2. Studies Table

This table stores metadata related to studies, such as the study instance UID, the associated patient ID, and the study date.
DDL Statement:

CREATE TABLE IF NOT EXISTS Studies (
    study_instance_uid TEXT PRIMARY KEY,
    study_date TEXT,
    patient_id TEXT,
    FOREIGN KEY (patient_id) REFERENCES Patients(patient_id)
);

study_instance_uid: Unique identifier for the study (from DICOM StudyInstanceUID).
study_date: Date of the study (from DICOM StudyDate).
patient_id: The ID of the patient who underwent the study (foreign key, referencing Patients table).

3. Series Table

This table stores metadata related to DICOM series, including the series instance UID and its associated study instance UID.
DDL Statement:

CREATE TABLE IF NOT EXISTS Series (
    series_instance_uid TEXT PRIMARY KEY,
    study_instance_uid TEXT,
    FOREIGN KEY (study_instance_uid) REFERENCES Studies(study_instance_uid)
);

series_instance_uid: Unique identifier for the series (from DICOM SeriesInstanceUID).
study_instance_uid: The ID of the study to which the series belongs (foreign key, referencing Studies table).

4. Slices Table

This table stores metadata for individual DICOM slices, such as slice thickness, pixel spacing, and acquisition date/time. It also links each slice to a specific series instance UID.
DDL Statement:

CREATE TABLE IF NOT EXISTS Slices (
    slice_id INTEGER PRIMARY KEY AUTOINCREMENT,
    series_instance_uid TEXT,
    slice_thickness REAL,
    pixel_spacing TEXT,
    acquisition_date_time TEXT,
    study_date TEXT,
    patient_id TEXT,
    FOREIGN KEY (series_instance_uid) REFERENCES Series(series_instance_uid),
    FOREIGN KEY (patient_id) REFERENCES Patients(patient_id),
    FOREIGN KEY (study_date) REFERENCES Studies(study_date)
);

slice_id: A unique ID for each slice record (auto-incremented).
series_instance_uid: The ID of the series to which the slice belongs (foreign key, referencing Series table).
slice_thickness: The thickness of the slice (from DICOM SliceThickness).
pixel_spacing: The spacing between pixels (from DICOM PixelSpacing).
acquisition_date_time: The timestamp when the slice was acquired (from DICOM AcquisitionDateTime).
study_date: The date of the study associated with the slice (foreign key, referencing Studies table).
patient_id: The ID of the patient associated with the slice (foreign key, referencing Patients table).


Relationships Between Tables:-

Patients to Studies: A patient can have multiple studies, but each study is associated with only one patient. This is a one-to-many relationship between Patients and Studies, with the patient_id as the foreign key.

Studies to Series: A study can have multiple series, but each series belongs to exactly one study. This is a one-to-many relationship between Studies and Series, with the study_instance_uid as the foreign key.

Series to Slices: A series can contain multiple slices, but each slice belongs to only one series. This is a one-to-many relationship between Series and Slices, with the series_instance_uid as the foreign key.

Slices to Patients/Studies: The slice also references both Patients and Studies tables to maintain data integrity across entities.

Summary of Data Model

The schema is normalized with the following key relationships:

Patients have multiple Studies.
Studies have multiple Series.
Series have multiple Slices.

This structure ensures that metadata related to DICOM files is efficiently stored and easy to query, with foreign keys linking the related tables. The database design is simple yet scalable for handling larger datasets.