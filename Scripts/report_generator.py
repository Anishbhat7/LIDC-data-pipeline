import pandas as pd
import matplotlib.pyplot as plt

def generate_summary():
    df = pd.read_csv("metadata.csv")
    print(f"Total Studies: {df['StudyInstanceUID'].nunique()}")
    print(f"Total Slices: {df['NumberOfSlices'].sum()}")
    print(f"Average Slices per Study: {df['NumberOfSlices'].mean()}")
    print(df['SliceThickness'].describe())

    # Visualization
    df['SliceThickness'].hist(bins=10)
    plt.title("Slice Thickness Distribution")
    plt.show()

if __name__ == "__main__":
    generate_summary()