# import os
# import SimpleITK as sitk
# import sqlite3

# # Function to print all available metadata keys in a DICOM file for debugging
# def print_metadata(dicom_file):
#     dicom_image = sitk.ReadImage(dicom_file)
#     for key in dicom_image.GetMetaDataKeys():
#         print(f"Key: {key}, Value: {dicom_image.GetMetaData(key)}")

# # Function to safely extract metadata from a DICOM file
# def extract_metadata(dicom_file):
#     dicom_image = sitk.ReadImage(dicom_file)
    
#     # Extract relevant metadata fields safely
#     patient_id = dicom_image.GetMetaData('0010|0020') if dicom_image.HasMetaDataKey('0010|0020') else None  # Patient ID
#     study_instance_uid = dicom_image.GetMetaData('0020|000D') if dicom_image.HasMetaDataKey('0020|000D') else None  # Study Instance UID
#     series_instance_uid = dicom_image.GetMetaData('0020|000E') if dicom_image.HasMetaDataKey('0020|000E') else None  # Series Instance UID
#     slice_thickness = dicom_image.GetMetaData('0018|0050') if dicom_image.HasMetaDataKey('0018|0050') else None  # Slice Thickness
#     pixel_spacing = dicom_image.GetMetaData('0028|0030') if dicom_image.HasMetaDataKey('0028|0030') else None  # Pixel Spacing
    
#     # Convert Pixel Spacing to a string if it's multi-valued
#     if pixel_spacing:
#         pixel_spacing = pixel_spacing.split("\\")  # DICOM stores multi-value fields with a backslash separator
#         pixel_spacing = ','.join(pixel_spacing)
    
#     study_date = dicom_image.GetMetaData('0008|0020') if dicom_image.HasMetaDataKey('0008|0020') else None  # Study Date
#     acquisition_date = dicom_image.GetMetaData('0008|0022') if dicom_image.HasMetaDataKey('0008|0022') else None  # Acquisition Date

#     # Get the number of slices for the current series (count DICOM files in the same directory)
#     num_slices = len([file for file in os.listdir(os.path.dirname(dicom_file)) if file.endswith('.dcm')])

#     return {
#         'patient_id': patient_id,
#         'study_instance_uid': study_instance_uid,
#         'series_instance_uid': series_instance_uid,
#         'slice_thickness': slice_thickness,
#         'pixel_spacing': pixel_spacing,
#         'study_date': study_date,
#         'acquisition_date': acquisition_date,
#         'num_slices': num_slices
#     }

# # Function to collect metadata from all DICOM files in a directory
# def collect_metadata(src_folder):
#     metadata_list = []
#     for root, dirs, files in os.walk(src_folder):
#         for file in files:
#             if file.lower().endswith('.dcm'):
#                 dicom_file = os.path.join(root, file)
#                 print_metadata(dicom_file)  # Print metadata to debug
#                 metadata = extract_metadata(dicom_file)
#                 metadata_list.append(metadata)
#     return metadata_list

# # Function to create a SQLite database and schema
# def create_db():
#     conn = sqlite3.connect('dicom_metadata.db')
#     cursor = conn.cursor()

#     # Create tables for patients, studies, and series
#     cursor.execute('''CREATE TABLE IF NOT EXISTS patients (
#                         patient_id TEXT PRIMARY KEY
#                       )''')

#     cursor.execute('''CREATE TABLE IF NOT EXISTS studies (
#                         study_instance_uid TEXT PRIMARY KEY,
#                         patient_id TEXT,
#                         study_date TEXT,
#                         acquisition_date TEXT,
#                         FOREIGN KEY (patient_id) REFERENCES patients (patient_id)
#                       )''')

#     cursor.execute('''CREATE TABLE IF NOT EXISTS series (
#                         series_instance_uid TEXT PRIMARY KEY,
#                         study_instance_uid TEXT,
#                         slice_thickness TEXT,
#                         pixel_spacing TEXT,
#                         num_slices INTEGER,
#                         FOREIGN KEY (study_instance_uid) REFERENCES studies (study_instance_uid)
#                       )''')

#     conn.commit()
#     return conn, cursor

# # Function to insert metadata into the database
# def insert_metadata(metadata):
#     conn, cursor = create_db()

#     for data in metadata:
#         patient_id = data['patient_id']
#         study_instance_uid = data['study_instance_uid']
#         series_instance_uid = data['series_instance_uid']
#         slice_thickness = data['slice_thickness']
#         pixel_spacing = data['pixel_spacing']
#         study_date = data['study_date']
#         acquisition_date = data['acquisition_date']
#         num_slices = data['num_slices']
        
#         # Insert patient if not already exists
#         cursor.execute('''INSERT OR IGNORE INTO patients (patient_id) VALUES (?)''', (patient_id,))
        
#         # Insert study if not already exists
#         cursor.execute('''INSERT OR IGNORE INTO studies (study_instance_uid, patient_id, study_date, acquisition_date) 
#                           VALUES (?, ?, ?, ?)''', 
#                        (study_instance_uid, patient_id, study_date, acquisition_date))
        
#         # Insert series
#         cursor.execute('''INSERT OR IGNORE INTO series (series_instance_uid, study_instance_uid, slice_thickness, pixel_spacing, num_slices)
#                           VALUES (?, ?, ?, ?, ?)''', 
#                        (series_instance_uid, study_instance_uid, slice_thickness, pixel_spacing, num_slices))

#     conn.commit()
#     conn.close()

# # Main execution: collect metadata and insert into database
# if __name__ == "__main__":
#     src_folder = 'path_to_your_dicom_folder'  # Replace with the path to your DICOM folder
#     metadata_list = collect_metadata(src_folder)
#     print(f"Extracted metadata: {metadata_list}")
#     insert_metadata(metadata_list)
#     print("Metadata inserted into the database successfully.")


import os
import pydicom
import sqlite3
import logging

# Setup logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to extract metadata from each DICOM file
def extract_metadata(dicom_file):
    dicom_image = pydicom.dcmread(dicom_file)

    # Extracting metadata
    patient_id = dicom_image.get('PatientID', 'Unknown')
    study_instance_uid = dicom_image.get('StudyInstanceUID', 'Unknown')
    series_instance_uid = dicom_image.get('SeriesInstanceUID', 'Unknown')
    slice_thickness = dicom_image.get('SliceThickness', 'Unknown')
    slice_location = dicom_image.get('SliceLocation', 'Unknown')
    pixel_spacing = dicom_image.get('PixelSpacing', ['Unknown'])
    study_date = dicom_image.get('StudyDate', 'Unknown')
    acquisition_date = dicom_image.get('AcquisitionDate', 'Unknown')

    # Handle pixel_spacing as a string in case it's a list
    pixel_spacing_str = ', '.join(map(str, pixel_spacing))

    return {
        'patient_id': patient_id,
        'study_instance_uid': study_instance_uid,
        'series_instance_uid': series_instance_uid,
        'slice_thickness': slice_thickness,
        'slice_location': slice_location,
        'pixel_spacing': pixel_spacing_str,
        'study_date': study_date,
        'acquisition_date': acquisition_date
    }

# Function to collect metadata from all DICOM files in a directory
def collect_metadata(root_dir):
    metadata_list = []
    dicom_files = []

    logging.info(f"Collecting metadata from directory: {root_dir}")
    
    # Traverse through the directory and get all DICOM files
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if file.endswith('.dcm'):
                dicom_file = os.path.join(root, file)
                dicom_files.append(dicom_file)
                metadata = extract_metadata(dicom_file)
                metadata_list.append(metadata)

    logging.info(f"Collected metadata for {len(dicom_files)} DICOM files.")
    return metadata_list, dicom_files

# Function to create and populate the SQLite database
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

# Insert metadata into the database
def insert_metadata(metadata_list, conn, cursor):
    for metadata in metadata_list:
        # Insert into Patients table
        cursor.execute('''
        INSERT OR IGNORE INTO Patients (patient_id)
        VALUES (?)
        ''', (metadata['patient_id'],))
        cursor.execute('SELECT id FROM Patients WHERE patient_id = ?', (metadata['patient_id'],))
        patient_id = cursor.fetchone()[0]

        # Insert into Studies table
        cursor.execute('''
        INSERT OR IGNORE INTO Studies (study_instance_uid, study_date, acquisition_date, patient_id)
        VALUES (?, ?, ?, ?)
        ''', (metadata['study_instance_uid'], metadata['study_date'], metadata['acquisition_date'], patient_id))
        cursor.execute('SELECT id FROM Studies WHERE study_instance_uid = ?', (metadata['study_instance_uid'],))
        study_id = cursor.fetchone()[0]

        # Insert into Series table
        cursor.execute('''
        INSERT OR IGNORE INTO Series (series_instance_uid, slice_thickness, slice_location, pixel_spacing, study_id)
        VALUES (?, ?, ?, ?, ?)
        ''', (metadata['series_instance_uid'], metadata['slice_thickness'], metadata['slice_location'], metadata['pixel_spacing'], study_id))
        conn.commit()

# # Function to calculate number of slices based on Slice Location
# def calculate_num_slices(dicom_files):
#     slice_locations = set()
#     for dicom_file in dicom_files:
#         dicom_image = pydicom.dcmread(dicom_file)
#         slice_location = dicom_image.get('', None)
#         if slice_location:
#             slice_locations.add(slice_location)
#     return len(slice_locations)

# Main function to process the metadata extraction and insertion
def main():
    root_dir = r"/Users/anishbhat/code/assignments/LIDC-data-pipeline/"  # Change to your path
    metadata_list, dicom_files = collect_metadata(root_dir)

    # Calculate the number of slices based on Slice Location
    num_slices = len(dicom_files)
    logging.info(f"Total number of slices: {num_slices}")

    conn, cursor = create_db()
    insert_metadata(metadata_list, conn, cursor)

    # Verify the row counts
    cursor.execute('SELECT COUNT(*) FROM Patients')
    logging.info(f"Patients table row count: {cursor.fetchone()[0]}")

    cursor.execute('SELECT COUNT(*) FROM Studies')
    logging.info(f"Studies table row count: {cursor.fetchone()[0]}")

    cursor.execute('SELECT COUNT(*) FROM Series')
    logging.info(f"Series table row count: {cursor.fetchone()[0]}")

    conn.close()
    logging.info("Database operations completed and connection closed.")

if __name__ == "__main__":
    main()
