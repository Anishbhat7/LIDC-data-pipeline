import os
import pydicom
from db_operations import insert_patient, insert_study, insert_series
import logging

logging.basicConfig(level=logging.INFO)

# Function to handle MultiValue objects by converting them to strings
def handle_multivalue(value):
    if isinstance(value, pydicom.multival.MultiValue):
        return ', '.join(map(str, value))  # Convert list or array to a comma-separated string
    return value

# Main function to process DICOM files
def process_dicom_files(directory_path):
    # Loop through each subfolder (LIDC-IDRI-0401 to LIDC-IDRI-0409)

    processed_data = {}

    for patient in os.listdir(directory_path):
        print(f"Patient is {patient}")
        patient_path = os.path.join(directory_path, patient)

        patient_data = processed_data.get(patient, {})
        processed_data[patient] = patient_data
        
        if not os.path.isdir(patient_path):
            logging.info(f"Not processing patient: {patient_path}")
            continue

        # for dirpath, _, filenames in os.walk(subfolder_path):
        for study in os.listdir(patient_path):
            dirpath = None
            print(f"Study is {study}")

            study_path = os.path.join(patient_path, study)
            study_data = patient_data.get(study, {})
            processed_data[patient][study]= study_data


            for series in os.listdir(study_path):
                print(f"Series is {series}")

                series_path = os.path.join(study_path, series)

                series_data = study_data.get(series, {})
                processed_data[patient][study][series] = series_data

                slice_data = {}

                # slice_path, _, slice_names = os.walk(series_path)
                for slice_name in os.listdir(series_path):
                    # print(f"Slice is {slice_name}")

                    filename = slice_name


                    if not filename.lower().endswith('.dcm'):  # Ensure the file is a DICOM file
                        continue

                    file_path = os.path.join(series_path, filename)
                    try:
                        ds = pydicom.dcmread(file_path)

                        slice_data = series_data.get(slice_name, ds)
                        processed_data[patient][study][series][slice_name] = slice_data


                    except Exception as e:
                        logging.error(f"Error processing {file_path}: {e}")
        print("--------------------------------------------")
        print()

    print(f"Processed data is {processed_data}")

if __name__ == "__main__":
    dicom_root_dir = r"D:\lidc_small_dset\lidc_small_dset"  # Root directory where all LIDC-IDRI folders are stored
    process_dicom_files(dicom_root_dir)

