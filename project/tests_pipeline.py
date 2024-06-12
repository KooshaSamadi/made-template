import unittest
from unittest.mock import patch, MagicMock
import os
import sqlite3
import pandas as pd
from io import StringIO, BytesIO
import tempfile
import data_pipeline

class TestDataPipeline(unittest.TestCase):
    @patch('requests.get')
    def test_download_dataset_from_url(self, mock_get):
        """Test downloading dataset from URL."""
        mock_get.return_value.content = b'test content'
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            data_pipeline.download_dataset_from_url('http://example.com/test.zip', temp_file.name)
            with open(temp_file.name, 'rb') as file:
                self.assertEqual(file.read(), b'test content')
        os.remove(temp_file.name)  # Ensure cleanup

    @patch('zipfile.ZipFile.extractall')
    @patch('zipfile.ZipFile.__init__', return_value=None)
    def test_extract_zip(self, mock_zip_init, mock_extractall):
        """Test extracting ZIP file."""
        data_pipeline.extract_zip('test.zip', 'test_dir')
        mock_zip_init.assert_called_with('test.zip', 'r')
        mock_extractall.assert_called_with('test_dir')

    def test_process_data(self):
        """Test processing data from CSV to DataFrame."""
        data = StringIO('A,B,C\n1,2,3\n4,5,6')
        with patch('pandas.read_csv', return_value=pd.read_csv(data)):
            df = data_pipeline.process_data('test.csv')
            pd.testing.assert_frame_equal(df, pd.DataFrame({'A': [1, 4], 'B': [2, 5], 'C': [3, 6]}))

    def test_clean_data(self):
        """Test cleaning data by removing NaN values."""
        df = pd.DataFrame({'A': [1, None, 3], 'B': [4, 5, None]})
        cleaned_df = data_pipeline.clean_data(df)
        self.assertFalse(cleaned_df.isnull().values.any())

    def test_save_to_sqlite(self):
        """Test saving DataFrame to SQLite database."""
        df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
        with tempfile.NamedTemporaryFile(suffix='.db') as temp_file:
            temp_file.close()  # Close the file to allow sqlite to open it
            data_pipeline.save_to_sqlite(df, temp_file.name)
            conn = sqlite3.connect(temp_file.name)
            saved_df = pd.read_sql_query("SELECT * FROM data", conn)
            conn.close()
            pd.testing.assert_frame_equal(df, saved_df)

    @patch('data_pipeline.download_dataset_from_url')
    @patch('data_pipeline.extract_zip')
    @patch('data_pipeline.process_data')
    @patch('data_pipeline.clean_data')
    @patch('data_pipeline.save_to_sqlite')
    @patch('os.makedirs')
    @patch('os.path.exists', return_value=False)
    def test_main(self, mock_exists, mock_makedirs, mock_save, mock_clean, mock_process, mock_extract, mock_download):
        """Test the main pipeline function end-to-end."""
        mock_process.return_value = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
        with patch('tempfile.TemporaryDirectory', return_value=tempfile.TemporaryDirectory()) as temp_dir:
            with patch('os.listdir', return_value=['Environment_Temperature_change_E_Europe_NOFLAG.csv', 'Emissions_Totals_E_Europe_NOFLAG.csv']):
                data_pipeline.main()
                self.assertTrue(mock_download.called)
                self.assertTrue(mock_extract.called)
                self.assertTrue(mock_process.called)
                self.assertTrue(mock_clean.called)
                self.assertTrue(mock_save.called)

    def test_system_level(self):
        """Test the system-level workflow."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock URLs for downloading
            download_url = [
                "https://example.com/Environment_Temperature_change_E_Europe.zip",
                "https://example.com/Emissions_Totals_E_Europe.zip",
            ]

            # Simulate downloading files
            for url in download_url:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.zip', dir=temp_dir) as temp_file:
                    temp_file.write(b"Dummy zip content")
                    temp_file_path = temp_file.name

            # Simulate extracted files
            extracted_files = [
                ('Environment_Temperature_change_E_Europe_NOFLAG.csv', 'Area,Months,Element,Unit,Y1961,Y2000,Y2030\nAlbania,January,Temperature change,°c,-2.5,1.2,2.5\n'),
                ('Emissions_Totals_E_Europe_NOFLAG.csv', 'Area,Item,Element,Unit,Y1961,Y2000,Y2030\nAlbania,Crop Residues,Direct emissions (N2O),kt,0.1,0.2,0.3\n')
            ]

            for filename, content in extracted_files:
                with open(os.path.join(temp_dir, filename), 'w') as f:
                    f.write(content)

            # Override file operations in the main function
            with patch('data_pipeline.download_dataset_from_url') as mock_download, \
                 patch('data_pipeline.extract_zip') as mock_extract, \
                 patch('data_pipeline.process_data', side_effect=lambda x: pd.read_csv(StringIO(content.split('\n')[1]))) as mock_process, \
                 patch('data_pipeline.clean_data', side_effect=lambda df: df.dropna()) as mock_clean, \
                 patch('data_pipeline.save_to_sqlite') as mock_save:

                # Configure mocks
                mock_download.side_effect = lambda url, path: open(path, 'wb').write(b'Dummy content')
                mock_extract.side_effect = lambda zip_path, extract_to: [
                    open(os.path.join(extract_to, filename), 'w').write(content) for filename, content in extracted_files
                ]

                data_pipeline.main()

                # Assertions to ensure all functions were called correctly
                self.assertTrue(mock_download.called)
                self.assertTrue(mock_extract.called)
                self.assertTrue(mock_process.called)
                self.assertTrue(mock_clean.called)
                self.assertTrue(mock_save.called)

if __name__ == '__main__':
    unittest.main()
