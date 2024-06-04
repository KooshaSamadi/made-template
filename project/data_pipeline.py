import requests
import zipfile
import os
import pandas as pd
import sqlite3
import tempfile

#download dataset from a URL
def download_dataset_from_url(url, download_path):
    response = requests.get(url)
    with open(download_path, "wb") as file:
        file.write(response.content)


#extract zip files
def extract_zip(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)


def process_data(file_path):
    try:
        df = pd.read_csv(file_path, encoding="utf-8") 
    except UnicodeDecodeError:
        df = pd.read_csv(
            file_path, encoding="ISO-8859-1"
        )
    start_year = "Y1961"
    end_year = "Y1999"
    print(
        f"number of rows before processing: {df.shape[0]} and number of columns before processing: {df.shape[1]}"
    )
    # drop column between start_year and end_year
    df.drop(df.loc[:, start_year:end_year].columns, axis=1, inplace=True)
    df.rename(columns={'Y2030':'Y2022','Y2050':'Y2023'}, inplace=True)
    #drop Area Code and Area Code (M49) and Item Code and Element Code and Source Code and Source from emmision data
    if 'Item Code' in df.columns:
        df.drop(['Area Code','Area Code (M49)','Item Code','Element Code','Source Code','Source'], axis=1, inplace=True)
    #drop Area Code and Area Code (M49) and Months Code and Element Code from temperature data
    if 'Months Code' in df.columns:
        df.drop(['Area Code','Area Code (M49)','Months Code','Element Code'], axis=1, inplace=True)
        #drop rows that Element == Standard Deviation
        df = df[df.Element != 'Standard Deviation']
   
    return df

def clean_data(df):
    df.dropna(inplace=True)
    return df


def save_to_sqlite(df, db_path):
    conn = sqlite3.connect(db_path)
    df.to_sql("data", conn, if_exists="replace", index=False)
    conn.close()


def main():
    download_url = [
        "https://bulks-faostat.fao.org/production/Environment_Temperature_change_E_Europe.zip",
        "https://bulks-faostat.fao.org/production/Emissions_Totals_E_Europe.zip",
    ]

    save_to_path = "../data"
    database_names = ["temperture_change.db", "emissions_totals.db"]
    #if there is no data folder, let's create one
    if not os.path.exists(save_to_path):
        os.makedirs(save_to_path)
    #use temporary directory to store the downloaded files
    with tempfile.TemporaryDirectory() as extract_to_path:
        for url, database_name in zip(download_url, database_names):
            zip_file_path = os.path.join(extract_to_path, os.path.basename(url))
            download_dataset_from_url(url, zip_file_path)
            print(f"Extracting dataset {zip_file_path}")
            extract_zip(zip_file_path, extract_to_path)
            print("Processing data...")
            base_file_name = os.path.basename(zip_file_path).split(".")[0]
            # print(base_file_name)
            for f in os.listdir(extract_to_path):
                if f.startswith(base_file_name) and f.endswith("NOFLAG.csv"):
                    data_file = f
            # print(data_file)
            file_path = os.path.join(extract_to_path, data_file)
            df_processed = process_data(file_path)
            print(
                f"number of rows after processing: {df_processed.shape[0]} and number of columns after processing: {df_processed.shape[1]}"
            )
            df_cleaned = clean_data(df_processed)
            print(
                f"number of rows after cleaning: {df_cleaned.shape[0]} and number of columns after cleaning: {df_cleaned.shape[1]}"
            )
            #save to ../data folder
            save_to_sqlite(df_cleaned, os.path.join(save_to_path, database_name))
            print("Data saved to SQLite database")
            print("#############################################")
                
main()
