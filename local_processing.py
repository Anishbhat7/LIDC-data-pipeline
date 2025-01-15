import os
import pydicom
from db_operations import insert_patient, insert_study, insert_series
import logging
import pandas as pd
from metadata_extraction import create_db
import matplotlib.pyplot as plt
import seaborn as sns

import sqlite3
conn = sqlite3.connect('slices.db')

logging.basicConfig(level=logging.INFO)


def handle_multivalue(value):
    if isinstance(value, pydicom.multival.MultiValue):
        return ', '.join(map(str, value))  # Convert list or array to a comma-separated string
    elif isinstance(value, pydicom.valuerep.PersonName):  # Handle PersonName type
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


                    if not filename.lower().endswith('.dcm'):  # Ensure the file is a DICOM file
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


def insert_into_db(df: pd.DataFrame):
    print(f"DF is: {df}, {df.columns}")
    # df.to_sql(
    #     name='slices2', 
    #     con=conn, 
    #     if_exists="append"
    # )
    """
    Insert the data into the Patients, Studies, Series, and Slices tables in sequence.
    """
    # Prepare the Patients DataFrame and insert
    # Insert data into 'Patients' table
    patients_df = df[['patientID', 'patientName', 'patientBirthDate']].drop_duplicates()
    patients_df = patients_df.rename(columns={
        'patientID': 'patient_id',
        'patientName': 'patient_name',
        'patientBirthDate': 'patient_birthdate'
    })
    patients_df.to_sql(
        name='Patients', 
        con=conn, 
        if_exists="append",  # Append if data already exists
        index=False
    )
    print(f"Inserted {len(patients_df)} records into Patients table.")

    # Insert data into 'Studies' table
    studies_df = df[['studyInstanceUID', 'studyDate', 'patientID']].drop_duplicates()
    studies_df = studies_df.rename(columns={
        'studyInstanceUID': 'study_instance_uid',
        'studyDate': 'study_date',
        'patientID': 'patient_id'
    })
    studies_df.to_sql(
        name='Studies', 
        con=conn, 
        if_exists="append",  # Append if data already exists
        index=False
    )
    print(f"Inserted {len(studies_df)} records into Studies table.")

    # Insert data into 'Series' table
    series_df = df[['seriesInstanceUID', 'studyInstanceUID']].drop_duplicates()
    series_df = series_df.rename(columns={
        'seriesInstanceUID': 'series_instance_uid',
        'studyInstanceUID': 'study_instance_uid'
    })
    series_df.to_sql(
        name='Series', 
        con=conn, 
        if_exists="append",  # Append if data already exists
        index=False
    )
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

    slices_df.to_sql(
        name='Slices', 
        con=conn, 
        if_exists="append",  # Append if data already exists
        index=False
    )
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


def analyze_data(df: pd.DataFrame):
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
        plt.show()
    else:
        print("No slice thickness data available")

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



def process_lidc_dataframe(df: pd.DataFrame):
    new_data_df = df
    # existing_data_df = read_df_from_sql()

    print("New Data Frame")
    print(new_data_df)

    process_dataframe(new_data_df)

    analyze_data(new_data_df)

    """
        1. IF data exists
            1. Handle conflicts
        2. If no new data
            1. Create a new table and insert
        3. Perform analytics on the df or directly sql
        4. Insert analytics data into the new table
    """

    insert_into_db(df)


def extract_data(df: pd.DataFrame):
    process_lidc_dataframe(df)
    

def process():
    # read_df_from_sql()
    # return
    create_db()
    dicom_root_dir = "/Users/anishbhat/code/assignments/LIDC-data-pipeline/"
    dataset_name = "lidc_small_dset"
    file_path = os.path.join(dicom_root_dir, dataset_name)

    processed_data, dlist = process_dicom_files(file_path)
    df = pd.DataFrame.from_records(dlist)

    extract_data(df)


if __name__ == "__main__":
    process()

