import pandas as pd
import os

def process_csv_file(csv_file_path):
    """
    Process a CSV file, display its dimensions, and print specific columns.

    Args:
    csv_file_path (str): The path to the CSV file to be processed.

    Returns:
    None
    """
    try:
        # Check if the file exists
        if not os.path.exists(csv_file_path):
            raise FileNotFoundError(f"The file {csv_file_path} does not exist.")

        # Attempt to read the CSV file
        dataframe = pd.read_csv(csv_file_path)

        # Get and print the dimensions of the dataframe
        row_count, column_count = dataframe.shape
        print(f'Number of Rows: {row_count}')
        print(f'Number of Columns: {column_count}')

        # Define the required columns
        required_columns = ['First Name', 'Last Name', 'Mobile Number']

        # Check if all required columns are present in the CSV
        missing_columns = [col for col in required_columns if col not in dataframe.columns]
        if missing_columns:
            raise ValueError(f"CSV file is missing the following required columns: {', '.join(missing_columns)}")

        # Print names and phone numbers
        print("\nNames and Phone Numbers:")
        for _, row in dataframe[required_columns].iterrows():
            full_name = f"{row['First Name']} {row['Last Name']}"
            phone_number = row['Mobile Number']
            print(f"Name: {full_name}, Phone Number: {phone_number}")

    except FileNotFoundError as e:
        print(f"File Error: {str(e)}")
    except pd.errors.EmptyDataError:
        print("Error: The CSV file is empty.")
    except pd.errors.ParserError:
        print("Error: Unable to parse the CSV file. Please check if it's a valid CSV.")
    except ValueError as e:
        print(f"Value Error: {str(e)}")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")

# Main execution block
if __name__ == "__main__":
    csv_file_path = 'SampleData1.csv'
    process_csv_file(csv_file_path)