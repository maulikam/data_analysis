import pandas as pd

def process_csv(csv_file):
    df = pd.read_csv(csv_file)

    rows, columns = df.shape
    print(f'Number of Rows: {rows}')
    print(f'Number of Columns: {columns}')

    required_columns = ['First Name', 'Last Name','Mobile Number']
    if not all(col in df.columns for col in required_columns):
        print("Error: CSV file does not contain the required columns.")
        return

    print("\nNames and Phone Numbers:")
    for _, row in df[required_columns].iterrows():
        print(f"Name: {row['First Name']} {row['Last Name']}, Phone Number: {row['Mobile Number']}")


csv_file = 'SampleData1.csv'
process_csv(csv_file)
