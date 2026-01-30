import unittest
import sys
import os
from pathlib import Path
import warnings

# Add parent directory to path to allow importing tasks
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from tasks.gfs import GFS_025_T2M_CAUCASUS, GFS_025_PCP_CAUCASUS

# Suppress warnings (like those from cfgrib or missing keys) for cleaner output
warnings.filterwarnings("ignore")

class TestGFSReading(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Locate the data directory relative to this test file
        cls.root_dir = Path(__file__).parent.parent
        cls.data_dir = cls.root_dir / 'tests' / 'data' / 'GFS'
        
        print(f"Data Directory: {cls.data_dir.absolute()}")

    def test_read_local_files(self):
        if not self.data_dir.exists():
            print("SKIPPING: Test data directory not found.")
            return

        files = list(self.data_dir.glob('*'))
        # Filter out directories and keep only files (e.g., .nc, .grb, etc.)
        files = [f for f in files if f.is_file()]
        
        if not files:
            print("SKIPPING: No files found in GFS data directory.")
            return

        print(f"Found {len(files)} files to test.")
        
        import pandas as pd
        import re

        # Track if we successfully read anything
        ops_success = 0

        for f in files:
            print(f"\nScanning: {f.name}")
            file_path = str(f)

            # Try to infer date from filename (expecting YYYYMMDD format in name)
            # Pattern looks for 8 digits starting with 20 or 202
            match = re.search(r'(20\d{6})', f.name)
            if match:
                file_date = pd.to_datetime(match.group(1), format='%Y%m%d')
            else:
                # Fallback if no date in filename
                file_date = pd.Timestamp.now()
                print("  Could not infer date from filename, using current date.")

            date_from = (file_date - pd.Timedelta(days=10)).strftime('%Y-%m-%d')
            date_to = (file_date + pd.Timedelta(days=10)).strftime('%Y-%m-%d')
            
            # Instantiate tasks for this specific file context
            t2m_task = GFS_025_T2M_CAUCASUS(date_from=date_from, date_to=date_to, verbose=0, download_from_source=False)
            t2m_task.verbose = 1

            pcp_task = GFS_025_PCP_CAUCASUS(date_from=date_from, date_to=date_to, verbose=0, download_from_source=False)
            pcp_task.verbose = 1

            # --- Try reading as T2M ---
            try:
                mr = t2m_task.read_local(file_path)
                data_shape = mr.data['data'].shape if isinstance(mr.data, dict) else mr.data.shape
                print(f"  [T2M] SUCCESS. Shape: {data_shape}")
                ops_success += 1
            except Exception as e:
                # Expected failure if the file is for a different variable (e.g. Precipitation)
                print(f"  [T2M] Skipped/Error: {e}")

            # --- Try reading as PCP ---
            try:
                mr = pcp_task.read_local(file_path)
                data_shape = mr.data['data'].shape if isinstance(mr.data, dict) else mr.data.shape
                print(f"  [PCP] SUCCESS. Shape: {data_shape}")
                ops_success += 1
            except Exception as e:
                # Expected failure if the file is for a different variable (e.g. Temperature)
                print(f"  [PCP] Skipped/Error: {e}")
        
        # Assert that at least one read operation was successful across all files
        # This ensures the test fails if valid data exists but code is broken.
        self.assertGreater(ops_success, 0, "Failed to read any files with either T2M or PCP configurations.")

if __name__ == '__main__':
    unittest.main()
