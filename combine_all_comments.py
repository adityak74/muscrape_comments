import pandas as pd
import os

def combine_csv_files(input_folder, output_file):
    # Create an empty DataFrame to store the combined data
    df_list = []

    # List all CSV files in the input folder
    csv_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]

    if not csv_files:
        print("No CSV files found in the input folder.")
        return

    # Loop through each CSV file and append its data to the combined DataFrame
    for csv_file in csv_files:
        file_path = os.path.join(input_folder, csv_file)
        try:
            df = pd.read_csv(file_path)
            df_list.append(df)
            print(f"Read {len(df)} rows from {csv_file}")
        except Exception as e:
            print(f"Error reading {csv_file}: {str(e)}")

    # Save the combined DataFrame as a new CSV file
    combined_df = pd.concat(df_list, ignore_index=True)

    combined_df.to_csv(output_file, index=False)
    print(f"Combined data saved to {output_file}")

if __name__ == "__main__":
    input_folder = "/Users/aditya.karnam/Projects/muscrape_yt_comments/comments_data"  # Replace with the folder containing your CSV files
    output_file = "/Users/aditya.karnam/Projects/muscrape_yt_comments/combined_comments.csv"  # Replace with the desired output file name
    combine_csv_files(input_folder, output_file)
