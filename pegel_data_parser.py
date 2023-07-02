import pandas as pd
import requests

def convert_json_to_csv(json_url, csv_path):
    try:
        # Download the JSON data from the URL
        response = requests.get(json_url)
        response.raise_for_status()  # Check if the request was successful

        # Load JSON data into a pandas DataFrame
        json_data = response.json()
        df = pd.json_normalize(json_data)

        # Check if the necessary columns exist in the DataFrame
        required_columns = ['shortname', 'water.shortname', 'timeseries']
        for column in required_columns:
            if column not in df.columns:
                raise ValueError(f"Required column '{column}' is missing in the JSON data.")

        # Select the desired columns
        df_selected = df[['shortname', 'water.shortname', 'timeseries']]

        # Extract the nested data
        df_selected['timeseries_currentMeasurement_timestamp'] = df_selected['timeseries'].apply(lambda x: x[0]['currentMeasurement']['timestamp'] if len(x) > 0 else None)
        df_selected['timeseries_currentMeasurement_value'] = df_selected['timeseries'].apply(lambda x: x[0]['currentMeasurement']['value'] if len(x) > 0 else None)

        # Rename the columns
        column_names = ['shortname', 'water_shortname', 'currentMeasurement_timestamp', 'currentMeasurement_value']
        df_selected = df_selected[['shortname', 'water.shortname', 'timeseries_currentMeasurement_timestamp', 'timeseries_currentMeasurement_value']]
        df_selected.columns = column_names

        # Convert DataFrame to CSV and save it
        df_selected.to_csv(csv_path, index=False)
        print(f"CSV file saved successfully to {csv_path}")

    except requests.exceptions.HTTPError as err:
        print(f"HTTP Error occurred: {err}")
    
    except requests.exceptions.RequestException as err:
        print(f"Request Exception occurred: {err}")
    
    except (ValueError, TypeError) as err:
        print(f"Error occurred while processing JSON data: {err}")

    except Exception as err:
        print(f"An unexpected error occurred: {err}")

# Example usage
json_url = 'https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json?includeTimeseries=true&includeCurrentMeasurement=true'
csv_path = 'C:/Users/Lenovo/Git_repos/pegel_water_level_analysis/data.csv'  # Save to the root directory of the C drive

convert_json_to_csv(json_url, csv_path)
