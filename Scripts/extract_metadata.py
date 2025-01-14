import os
import pydicom
import pandas as pd

import logging

logging.basicConfig(filename="download.log", level=logging.INFO)
def log_issue(issue):
    logging.error(issue)

from config import DATA_DIR

def extract_metadata():
    metadata = []
    for root, _, files in os.walk(DATA_DIR):
        for file in files:
            try:
                file_path = os.path.join(root, file)
                dicom = pydicom.dcmread(file_path)
                metadata.append({
                    "PatientID": dicom.PatientID,
                    "StudyInstanceUID": dicom.StudyInstanceUID,
                    "SeriesInstanceUID": dicom.SeriesInstanceUID,
                    "NumberOfSlices": len(files),
                    "SliceThickness": dicom.get("SliceThickness", None),
                    "PixelSpacing": dicom.get("PixelSpacing", None),
                    "StudyDate": dicom.StudyDate,
                    "AcquisitionDate": dicom.get("AcquisitionDate", None),
                })
            except Exception as e:
                log_issue(f"Error processing {file_path}: {e}")

    return pd.DataFrame(metadata)

if __name__ == "__main__":
    df = extract_metadata()
    df.to_csv("metadata.csv", index=False)
