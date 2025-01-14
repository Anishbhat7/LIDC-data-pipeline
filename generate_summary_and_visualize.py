import sqlite3
import matplotlib.pyplot as plt

# Connect to the database
conn = sqlite3.connect('metadata.db')
cursor = conn.cursor()

# Generate Summary Statistics
cursor.execute("SELECT COUNT(DISTINCT study_id) FROM Series")
total_studies = cursor.fetchone()[0]

cursor.execute("""
    SELECT COUNT(DISTINCT slice_location) 
    FROM Series
""")
total_slices = cursor.fetchone()[0] or 0

average_slices = total_slices / total_studies if total_studies else 0

cursor.execute("SELECT COUNT(DISTINCT study_id) FROM Series")
num_studies = cursor.fetchone()[0]

# # Check if total_slices is None, and set it to 0 if so
# total_slices = total_slices if total_slices is not None else 0
# average_slices = total_slices / num_studies if num_studies else 0

# Print the summary statistics
print(f"Total number of studies: {total_studies}")
print(f"Total slices across all scans: {total_slices}")
print(f"Average number of slices per study: {average_slices}")

# Distribution of slice thickness
cursor.execute("SELECT slice_thickness FROM Series")
thickness_values = [row[0] for row in cursor.fetchall()]
thickness_values = [float(value) for value in thickness_values if value]  # Convert to float and remove empty values

# Plotting the histogram for slice thickness
plt.hist(thickness_values, bins=10, edgecolor='black')
plt.title('Distribution of Slice Thickness')
plt.xlabel('Slice Thickness')
plt.ylabel('Frequency')
plt.show()

# Bar Chart: Number of slices per series
cursor.execute("SELECT series_instance_uid, num_slices FROM Series")
series_data = cursor.fetchall()

# Filter out rows where num_slices is None
series_uids = [row[0] for row in series_data if row[1] is not None]
num_slices_per_series = [row[1] for row in series_data if row[1] is not None]

# Check if there are valid series to plot
if series_uids and num_slices_per_series:
    plt.bar(series_uids, num_slices_per_series)
    plt.title('Number of Slices per Series')
    plt.xlabel('Series Instance UID')
    plt.ylabel('Number of Slices')
    plt.xticks(rotation=90)  # Rotate x-axis labels for better readability
    plt.show()
else:
    print("No valid series to plot.")

# Close the database connection
conn.close()
