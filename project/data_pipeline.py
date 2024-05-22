import requests
import zipfile
import os
import pandas as pd
import sqlite3

# Function to download dataset from a URL
def download_dataset_from_url(url, download_path):
    response = requests.get(url)
    with open(download_path, 'wb') as file:
        file.write(response.content)

# Function to extract zip files
def extract_zip(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

# Function to clean data
def clean_data(file_path):
    df = pd.read_csv(file_path)
    print("Column names:", df.columns)
    df.dropna(inplace=True)
    return df

# Function to save DataFrame to SQLite
def save_to_sqlite(db_path, table_name, dataframe):
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    # Write records stored in a DataFrame to a SQL database
    dataframe.to_sql(table_name, conn, if_exists='replace', index=False)
    # Close the connection
    conn.close()

# Main function
def main():
    #forest area dataset
    #historical emissions dataset
    download_url = ['https://storage.googleapis.com/kaggle-data-sets/4582549/7821337/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20240522%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240522T193403Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=912629c7ef8a913efc774a42ec1c5ca576bcc9cf267cd66ff9c5d9832fcd07513b53a12b4cb55acbe4bfde087a73b19e5d6a3560dafa09485d4bbc4739b67e303cced5005fddac0e6cca45e07a3eda63b2e3041053658deae47bd0a8f015e4bbff2b0855163e913cfa0e8a3b5fba3e77dc5c31e7520a9decdc905fd955aedfc6279c412d24ae568b542f65372924130a8af04414604d68eb5514513c1ae7b02d2fb7214e9e6cbd610924a460abe5972db5726aa0d7997338e65bc87fef0302cd8b852af632a060885aad762a269516db6618282ac87cc481f53bea913498bb190b806491ae0d70ecc9dfa95539414731564fbf668311b54322b416aeea35dcea',
                    'https://storage.googleapis.com/kaggle-data-sets/2020597/3348143/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20240522%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240522T193351Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=69be2800f739a0e9b1f95b25db0e22d4b4a3a79309f4b427c0c5fb6ef5b0a5bee8ad294256d361c8f51a887159ba827c1398bba2bf898d600ba9882034dd77458bff484b53e11e8a80afd7b8028cd8de75c61107cefa3a1ab24e30ef7e2a283f1d7648f95190309d98e623e4f3c2cd92a503f88ca818a1cd478894ef770e58f3591a5879e0c0588d9b2be4bda47a2b6c047a554b710046c590f4f53ed39cd938824c3bb39474972691dc031bbfb04301d410ec0567a99e8a6afc7d457e8148693a65019c579a618789f635c590d0be35f21d3d7999cb1230e19a7eb7717fcd4273c627ffdf94889c8454d681ef11b8e45e784c657cc750e687ff2bc4c2f6303f']
    zip_file_path = ['Forest_Area.zip','historical_emissions.zip']
    extract_to_path = './'
    db_path = ['Forest_Area.db','historical_emissions.db']  
    for url, zip_file_path, db_path in zip(download_url, zip_file_path, db_path):
        download_dataset_from_url(url, zip_file_path)
        print("Extracting dataset...")
        extract_zip(zip_file_path, extract_to_path)
        print("Processing data...")
        base_file_name = os.path.basename(zip_file_path).split('.')[0]
        data_files = [f for f in os.listdir(extract_to_path) if f.startswith(base_file_name) and f.endswith('.csv')]
        for file in data_files:
            file_path = os.path.join(extract_to_path, file)
            df_cleaned = clean_data(file_path)
            print(f"Data from {file} cleaned")
            table_name = os.path.splitext(file)[0]
            save_to_sqlite(db_path, table_name, df_cleaned)


main()