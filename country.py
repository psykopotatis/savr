import pandas as pd
from tabulate import tabulate


def clean_data(df):
    """
    Identifies and removes potential duplicate entries based on 'agentName' and 'numberOfShares'.
    """
    initial_count = len(df)

    # Identify duplicates: same 'agentName' and 'numberOfShares'
    duplicates = df.duplicated(subset=['agentName', 'numberOfShares'], keep='first')
    duplicate_count = duplicates.sum()

    if duplicate_count > 0:
        print(f"Found {duplicate_count} duplicate entries. Removing duplicates...")
        df = df[~duplicates]
        print(f"Data cleaned. {len(df)} entries remaining.")
    else:
        print("No duplicate entries found.")

    return df


def aggregate_shares_and_percentages(csv_file_path, output_csv_path):
    """
    Reads the agent holdings CSV, cleans data by removing duplicates,
    aggregates total shares and sums percentages by country,
    rounds the percentages to 2 decimals, and saves the result to a new CSV file.

    Parameters:
    - csv_file_path: Path to the input CSV file with agent holdings.
    - output_csv_path: Path to save the aggregated and sorted CSV file.
    """
    try:
        # Load the CSV data into a DataFrame
        df = pd.read_csv(csv_file_path)
        print(f"Loaded data from '{csv_file_path}' successfully.")

        # Display the first few rows (optional)
        print("\nSample Data:")
        print(df.head())

        # Clean the data by removing duplicates
        df = clean_data(df)

        # Ensure 'numberOfShares' is numeric
        df['numberOfShares'] = pd.to_numeric(df['numberOfShares'], errors='coerce')

        # Ensure 'percentageOfShares' is numeric
        df['percentageOfShares'] = pd.to_numeric(df['percentageOfShares'], errors='coerce')

        # Drop rows with invalid 'numberOfShares' or 'percentageOfShares'
        invalid_shares = df['numberOfShares'].isnull().sum()
        invalid_percentages = df['percentageOfShares'].isnull().sum()
        if invalid_shares > 0 or invalid_percentages > 0:
            print(
                f"Found {invalid_shares} rows with invalid 'numberOfShares' and {invalid_percentages} rows with invalid 'percentageOfShares'. These will be removed.")
            df = df.dropna(subset=['numberOfShares', 'percentageOfShares'])

        # Check if necessary columns exist
        required_columns = {"countryCode", "numberOfShares", "percentageOfShares", "agentName"}
        if not required_columns.issubset(df.columns):
            missing = required_columns - set(df.columns)
            raise ValueError(f"Missing columns in CSV: {missing}")

        # Aggregate the total number of shares and sum of percentages per country
        aggregation = df.groupby('countryCode').agg(
            totalNumberOfShares=pd.NamedAgg(column='numberOfShares', aggfunc='sum'),
            totalPercentageOfShares=pd.NamedAgg(column='percentageOfShares', aggfunc='sum'),
            numberOfAgents=pd.NamedAgg(column='agentName', aggfunc='count')
        ).reset_index()

        # Sort the aggregation by totalNumberOfShares in descending order
        aggregation_sorted = aggregation.sort_values(by='totalNumberOfShares', ascending=False)

        # Round 'totalPercentageOfShares' to 2 decimal places
        aggregation_sorted['totalPercentageOfShares'] = aggregation_sorted['totalPercentageOfShares'].round(2)

        # Define column alignment: left for 'countryCode', right for numerical columns
        column_alignments = ("left", "right", "right", "right")

        # Display the aggregated and sorted data with right-aligned numbers
        print("\nAggregated Total Shares and Summed Percentages by Country:")
        print(tabulate(aggregation_sorted, headers='keys', tablefmt='pretty', showindex=False,
                       colalign=column_alignments))

        # Save the aggregated data to a new CSV file
        aggregation_sorted.to_csv(output_csv_path, index=False)
        print(f"\nAggregated data has been saved to '{output_csv_path}'.")

    except FileNotFoundError:
        print(f"Error: The file '{csv_file_path}' does not exist.")
    except pd.errors.EmptyDataError:
        print("Error: The provided CSV file is empty.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    # Define the input and output CSV file paths
    input_csv = "latest_agent_holdings_with_details.csv"
    output_csv = "total_shares_and_summed_percentage_by_country_sorted.csv"

    # Call the function to perform aggregation
    aggregate_shares_and_percentages(input_csv, output_csv)
