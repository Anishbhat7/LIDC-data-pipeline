import os
import pydicom
import logging
import pandas as pd
from setup_db import create_db
import matplotlib.pyplot as plt
import seaborn as sns
import os
import shutil
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from config import S3_BUCKET, ACCESS_KEY, SECRET_KEY, DATA_DIR

import sqlite3
conn = sqlite3.connect('slices.db')

logging.basicConfig(level=logging.INFO)


def download_and_validate_from_s3():
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
        )
        os.makedirs(DATA_DIR, exist_ok=True)

        # List files in the S3 bucket
        objects = s3.list_objects_v2(Bucket=S3_BUCKET).get("Contents", [])
        for obj in objects:
            file_path = os.path.join(DATA_DIR, obj["Key"])
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            s3.download_file(S3_BUCKET, obj["Key"], file_path)
            print(f"Downloaded: {file_path}")

    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Error: {e}")


def handle_multivalue(value):
    if isinstance(value, pydicom.multival.MultiValue):
        return ', '.join(map(str, value)) 
    elif isinstance(value, pydicom.valuerep.PersonName): 
        return str(value)
    return value


def read_ds(ds):

    d = dict(
        # Patient
        patientID = handle_multivalue(ds.PatientID),
        patientName = handle_multivalue(ds.PatientName),
        patientBirthDate = handle_multivalue(ds.PatientBirthDate),

        # Study
        studyInstanceUID = handle_multivalue(ds.StudyInstanceUID),
        studyDate = handle_multivalue(ds.StudyDate),

        # Series
        seriesInstanceUID = handle_multivalue(ds.SeriesInstanceUID),

        # Slice
        sliceThickness = handle_multivalue(ds.SliceThickness),
        pixelSpacing = handle_multivalue(ds.PixelSpacing),
        acquisitionDateTime = handle_multivalue(ds.AcquisitionDateTime),
    )
    return d


def process_dicom_files(directory_path):

    data_in_list = []
    processed_data = {}
    printed = 0

    for patient in os.listdir(directory_path):
        # print(f"Patient is {patient}")
        patient_path = os.path.join(directory_path, patient)

        patient_data = processed_data.get(patient, {})
        processed_data[patient] = patient_data
        
        if not os.path.isdir(patient_path):
            logging.info(f"Not processing patient: {patient_path}")
            continue

        for study in os.listdir(patient_path):
            # print(f"Study is {study}")

            study_path = os.path.join(patient_path, study)
            study_data = patient_data.get(study, {})
            processed_data[patient][study]= study_data


            for series in os.listdir(study_path):
                # print(f"Series is {series}")

                series_path = os.path.join(study_path, series)

                series_data = study_data.get(series, {})
                processed_data[patient][study][series] = series_data

                slice_data = {}

                for slice_name in os.listdir(series_path):
                    # print(f"Slice name is {slice_name}")

                    filename = slice_name


                    if not filename.lower().endswith('.dcm'): 
                        continue

                    file_path = os.path.join(series_path, filename)
                    try:
                        ds = pydicom.dcmread(file_path)
                        if printed < 0:
                            print("First DS")
                            print(ds)

                            read_ds(ds)
                            printed += 1

                        d = read_ds(ds)
                        data_in_list.append(d)

                        slice_data = series_data.get(slice_name, ds)
                        processed_data[patient][study][series][slice_name] = slice_data


                    except Exception as e:
                        logging.error(f"Error processing {file_path}: {e}")

                total_slices = len(slice_data.keys())
                print(f"Total slices processed: {total_slices}")

    return processed_data, data_in_list


def extract_metadata(dicom_file):
    dicom = pydicom.dcmread(dicom_file, force=True)

    patient_id = dicom.get('PatientID', 'UnknownPatient')
    study_instance_uid = dicom.get('StudyInstanceUID', 'UnknownStudy')
    series_instance_uid = dicom.get('SeriesInstanceUID', 'UnknownSeries')

    return patient_id, study_instance_uid, series_instance_uid


def transform_folders(src_folder, output_folder):
    for root, dirs, files in os.walk(src_folder):
        for file in files:
            print(file)
            if file.lower().endswith('.dcm'):  
                dicom_file = os.path.join(root, file)

                patient_id, study_instance_uid, series_instance_uid = extract_metadata(dicom_file)

                new_folder = os.path.join(output_folder, patient_id, study_instance_uid, series_instance_uid)

                if not os.path.exists(new_folder):
                    os.makedirs(new_folder)

                destination = os.path.join(new_folder, file)

                shutil.copy(dicom_file, destination)
                print(f"Copied: {dicom_file} -> {destination}")



def populate_data_in_db(df: pd.DataFrame):
    print(f"DF is: {df}, {df.columns}")
    """
    Insert the data into the Patients, Studies, Series, and Slices tables in sequence.
    """
    # Insert data into 'Patients' table
    patients_df = df[['patientID', 'patientName', 'patientBirthDate']].drop_duplicates()
    patients_df = patients_df.rename(columns={
        'patientID': 'patient_id',
        'patientName': 'patient_name',
        'patientBirthDate': 'patient_birthdate'
    })

    conn.execute("BEGIN TRANSACTION;")
    for _, row in patients_df.iterrows():
        conn.execute("""
            INSERT OR REPLACE INTO Patients (patient_id, patient_name, patient_birthdate)
            VALUES (?, ?, ?)
        """, (row['patient_id'], row['patient_name'], row['patient_birthdate']))
    conn.commit()
    print(f"Inserted {len(patients_df)} records into Patients table.")


    # Insert data into 'Studies' table
    studies_df = df[['studyInstanceUID', 'studyDate', 'patientID']].drop_duplicates()
    studies_df = studies_df.rename(columns={
        'studyInstanceUID': 'study_instance_uid',
        'studyDate': 'study_date',
        'patientID': 'patient_id'
    })

    conn.execute("BEGIN TRANSACTION;")
    for _, row in studies_df.iterrows():
        conn.execute("""
            INSERT OR REPLACE INTO Studies (study_instance_uid, study_date, patient_id)
            VALUES (?, ?, ?)
        """, (row['study_instance_uid'], row['study_date'], row['patient_id']))
    conn.commit()
    print(f"Inserted {len(studies_df)} records into Studies table.")


    # Insert data into 'Series' table
    series_df = df[['seriesInstanceUID', 'studyInstanceUID']].drop_duplicates()
    series_df = series_df.rename(columns={
        'seriesInstanceUID': 'series_instance_uid',
        'studyInstanceUID': 'study_instance_uid'
    })

    conn.execute("BEGIN TRANSACTION;")
    for _, row in series_df.iterrows():
        conn.execute("""
            INSERT OR REPLACE INTO Series (series_instance_uid, study_instance_uid)
            VALUES (?, ?)
        """, (row['series_instance_uid'], row['study_instance_uid']))
    conn.commit()
    print(f"Inserted {len(series_df)} records into Series table.")


    # Insert data into 'Slices' table
    slices_df = df[['seriesInstanceUID', 'sliceThickness', 'pixelSpacing', 'acquisitionDateTime', 'studyDate', 'patientID']]
    slices_df = slices_df.rename(columns={
        'patientID': 'patient_id',
        'studyInstanceUID': 'study_instance_uid',
        'seriesInstanceUID': 'series_instance_uid',
        'sliceThickness': 'slice_thickness',
        'pixelSpacing': 'pixel_spacing',
        'acquisitionDateTime': 'acquisition_date_time',
        'studyDate': 'study_date'
    })
    
    conn.execute("BEGIN TRANSACTION;")
    for _, row in slices_df.iterrows():
        conn.execute("""
            INSERT OR REPLACE INTO Slices (series_instance_uid, slice_thickness, pixel_spacing, acquisition_date_time, study_date, patient_id)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (row['series_instance_uid'], row['slice_thickness'], row['pixel_spacing'], row['acquisition_date_time'], row['study_date'], row['patient_id']))
    conn.commit()
    print(f"Inserted {len(slices_df)} records into Slices table.")

    print("Data inserted into all tables.")


def read_df_from_sql():
    df = pd.read_sql('select * from slices2', conn)
    return df


def extract_patient_data(df: pd.DataFrame):
    """
        1. Clean, process and insert into db / data warehouse
    """
    pass


def extract_study_data(df: pd.DataFrame):
    """
        1. Clean, process and insert into db / data warehouse
    """
    pass


def extract_series_data(df: pd.DataFrame):
    """
        1. Clean, process and insert into db / data warehouse
    """
    pass


def extract_slice_data(df: pd.DataFrame):
    """
        1. Clean, process and insert into db / data warehouse
    """
    pass


def process_dataframe(df: pd.DataFrame):

    extract_patient_data(df)
    extract_study_data(df)
    extract_series_data(df)
    extract_slice_data(df)

    pass


def analyze_data(df: pd.DataFrame, output_excel_path = "analysis_of_scans_data.xlsx"):
    """
        1. Analyze using Athena or related tools
        2. For now we'll do the analysis on the df directly
    """
    """
    Analyze the metadata extracted from DICOM files.
    1. Total number of studies
    2. Total number of slices across all scans
    3. Average number of slices per study
    4. Distribution of slice thickness (or other relevant metadata)
    """

    # 1. Total number of studies
    total_studies = df['studyInstanceUID'].nunique()
    print(f"Total number of studies: {total_studies}")

    # 2. Total number of slices across all scans
    total_slices = len(df)
    print(f"Total number of slices: {total_slices}")

    # 3. Average number of slices per study
    slices_per_study = df.groupby('studyInstanceUID').size()
    avg_slices_per_study = slices_per_study.mean()
    print(f"Average number of slices per study: {avg_slices_per_study:.2f}")

    # 4. Distribution of slice thickness (assuming 'sliceThickness' exists in the dataframe)
    if 'sliceThickness' in df.columns:
        slice_thickness = pd.to_numeric(df['sliceThickness'], errors='coerce')  # Convert to numeric, handle errors gracefully
        slice_thickness_summary = {
            "Mean": slice_thickness.mean(),
            "Median": slice_thickness.median(),
            "Std": slice_thickness.std(),
            "Min": slice_thickness.min(),
            "Max": slice_thickness.max()
        }
        print("Slice Thickness Distribution:")
        print(f"  Mean slice thickness: {slice_thickness.mean():.2f} mm")
        print(f"  Median slice thickness: {slice_thickness.median():.2f} mm")
        print(f"  Standard deviation of slice thickness: {slice_thickness.std():.2f} mm")
        print(f"  Min slice thickness: {slice_thickness.min()} mm")
        print(f"  Max slice thickness: {slice_thickness.max()} mm")

        # Quick Visualization: Histogram of Slice Thickness
        plt.figure(figsize=(8, 6))
        sns.histplot(slice_thickness, kde=True, bins=20, color='blue', stat='density')
        plt.title('Distribution of Slice Thickness')
        plt.xlabel('Slice Thickness (mm)')
        plt.ylabel('Density')
        plt.grid(True)
        plt.savefig('slice_thickness_histogram.png')
        plt.show()
    else:
        print("No slice thickness data available")
        slice_thickness_summary = {
            "Mean": None,
            "Median": None,
            "Std": None,
            "Min": None,
            "Max": None
        }

    # Store the data in an Excel file
    with pd.ExcelWriter(output_excel_path, engine='xlsxwriter') as writer:
        pd.DataFrame({'Total Studies': [total_studies]}).to_excel(writer, sheet_name='Total Studies', index=False)

        pd.DataFrame({'Total Slices': [total_slices]}).to_excel(writer, sheet_name='Total Slices', index=False)

        slices_per_study_df = slices_per_study.reset_index(name='Number of Slices')
        slices_per_study_df.to_excel(writer, sheet_name='Slices per Study', index=False)

        slice_thickness_df = pd.DataFrame(slice_thickness_summary, index=[0])
        slice_thickness_df.to_excel(writer, sheet_name='Slice Thickness', index=False)

    slices_per_study = df.groupby('studyInstanceUID').size()

    # Quick Visualization: Bar Chart of Slices per Study
    plt.figure(figsize=(10, 6))
    sns.barplot(x=slices_per_study.index, y=slices_per_study.values, color='green')
    plt.title('Number of Slices per Study')
    plt.xlabel('Study Instance UID (Truncated)')
    plt.ylabel('Number of Slices')

    truncated_labels = slices_per_study.index.str.slice(0, 10)  # Truncate to first 10 characters
    plt.xticks(ticks=range(len(slices_per_study)), labels=truncated_labels, rotation=90)

    plt.tight_layout()
    plt.savefig('slices_per_study_bar_chart.png')
    plt.show()

    analysis_summary = {
        "total_studies": total_studies,
        "total_slices": total_slices,
        "avg_slices_per_study": avg_slices_per_study,
        "slice_thickness_mean": slice_thickness.mean() if 'sliceThickness' in df.columns else None,
        "slice_thickness_median": slice_thickness.median() if 'sliceThickness' in df.columns else None,
        "slice_thickness_std": slice_thickness.std() if 'sliceThickness' in df.columns else None,
        "slice_thickness_min": slice_thickness.min() if 'sliceThickness' in df.columns else None,
        "slice_thickness_max": slice_thickness.max() if 'sliceThickness' in df.columns else None
    }

    print("\nAnalysis Summary:")
    for key, value in analysis_summary.items():
        print(f"{key}: {value}")
    
    return analysis_summary


def process():
    create_db()
    dicom_root_dir = DATA_DIR
    dataset_name = "lidc_small_dset"
    file_path = os.path.join(dicom_root_dir, dataset_name)

    #Download from s3 and validate the data
    # download_and_validate_from_s3()

    #Metadata Extraction
    processed_data, dlist = process_dicom_files(file_path)
    df = pd.DataFrame.from_records(dlist)

    # #Transform Folders
    root_dir = file_path
    output_dir = os.path.join(dicom_root_dir, "transformed")
    transform_folders(root_dir, output_dir)

    #Populate Data in DB
    populate_data_in_db(df)
    
    #For Huge Amounts of Data
    # # process_dataframe(df)


    # #Analyze Data
    analyze_data(df)


if __name__ == "__main__":
    process()

