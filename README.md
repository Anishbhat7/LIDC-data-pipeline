# LIDC-data-pipeline
DICOM Metadata Extraction and Storage
Project Overview

This project implements a pipeline to download, process, and store metadata from DICOM files in the LIDC-IDRI dataset (Lung Image Database Consortium). The pipeline performs the following tasks:

    Data Ingestion: Downloads DICOM files from an Amazon S3 bucket, ensuring all files are successfully downloaded.
    Metadata Extraction: Extracts key metadata from DICOM file headers, such as patient ID, study instance UID, series instance UID, slice thickness, and acquisition date.
    Data Transformation: Organizes the files into a logical folder structure and processes metadata into a structured format.
    Data Storage: Stores extracted metadata in an SQLite database.
    Reporting & Analysis: Generates basic summary statistics and visualizations for the metadata.


Setup Instructions
1. Clone the Repository

git clone https://github.com/Anishbhat7/LIDC-data-pipeline.git

2. Install Dependencies

Create a virtual environment and install the required dependencies:

python3 -m venv venv
source venv/bin/activate    # On Windows, use `venv\Scripts\activate`
pip install -r requirements.txt

3. Configuration

Edit the config.py file with your AWS S3 credentials and other relevant paths:

# config.py

S3_BUCKET = 'your-s3-bucket-name'
ACCESS_KEY = 'your-aws-access-key'
SECRET_KEY = 'your-aws-secret-key'
DATA_DIR = './data'  # Directory to save downloaded DICOM files

4. Database Setup

The project uses SQLite for storing metadata. When the pipeline runs, the database (slices.db) will be created automatically. No additional setup is needed for the database.
5. Running the Pipeline

To run the entire pipeline, execute the following command:

python pipeline.py

This will:

    Download DICOM files from the specified S3 bucket.
    Extract metadata from each DICOM file.
    Organize the files into a structured directory.
    Store the extracted metadata in the slices.db database.
    Generate summary statistics and visualizations.

6. View the Data

To view the data stored in the SQLite database, you can query it directly using tools like DB Browser for SQLite or use the following Python code:

import sqlite3
conn = sqlite3.connect('slices.db')
df = pd.read_sql('SELECT * FROM Slices', conn)
print(df.head())

or go to terminal and run:
sqlite3 slices.db
then you can run any query on the db

7. Analysis

To analyze the metadata, the pipeline generates a report with summary statistics and visualizations, including:

    Total number of studies
    Total number of slices
    Average number of slices per study
    Distribution of slice thickness

The analysis is saved in an Excel file (analysis_of_scans_data.xlsx), and visualizations will be displayed using matplotlib.