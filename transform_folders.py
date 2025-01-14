import os
import shutil
import pydicom

# Define the root directory of your current data structure
root_dir = r"D:\lidc_small_dset\lidc_small_dset"

# Function to extract metadata from DICOM file
def extract_metadata(dicom_file):
    # Load the dicom file
    dicom = pydicom.dcmread(dicom_file)

    # Extract relevant metadata
    patient_id = dicom.get('PatientID', 'UnknownPatient')
    study_instance_uid = dicom.get('StudyInstanceUID', 'UnknownStudy')
    series_instance_uid = dicom.get('SeriesInstanceUID', 'UnknownSeries')

    return patient_id, study_instance_uid, series_instance_uid

# Function to create the new folder structure
def create_new_structure(src_folder):
    # Iterate over the original folder structure
    for root, dirs, files in os.walk(src_folder):
        for file in files:
            if file.lower().endswith('.dcm'):  # Only process DICOM files
                # Full path of the DICOM file
                dicom_file = os.path.join(root, file)

                # Extract metadata from the DICOM file
                patient_id, study_instance_uid, series_instance_uid = extract_metadata(dicom_file)

                # Define the new folder path
                new_folder = os.path.join(root_dir, patient_id, study_instance_uid, series_instance_uid)

                # Create the new folder structure if it does not exist
                if not os.path.exists(new_folder):
                    os.makedirs(new_folder)

                # Define the destination path for the file
                destination = os.path.join(new_folder, file)

                # Move the file to the new location
                shutil.move(dicom_file, destination)
                print(f"Moved: {dicom_file} -> {destination}")

# Run the transformation
create_new_structure(root_dir)
