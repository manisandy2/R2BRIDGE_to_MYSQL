
import sys
import os
import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from fastapi import HTTPException

# Adjust path to import the module
sys.path.append("/Users/mac-1/Desktop/R2BridgeMysql")

# Mock dependencies before importing
sys.modules["core.r2_client"] = MagicMock()
sys.modules["csv_to_json"] = MagicMock()
sys.modules["mysql_creds"] = MagicMock()
sys.modules["core.catalog_client"] = MagicMock()
sys.modules["...mysql_creds"] = MagicMock()
sys.modules["...core.catalog_client"] = MagicMock()

# Import the function to test
# We need to mock pyarrow.parquet and pyarrow.fs
with patch("pyarrow.parquet.ParquetFile") as MockParquetFile, \
     patch("pyarrow.fs.S3FileSystem") as MockS3FileSystem:
    
    from routers.transaction_model02.parquet import read_parquet_conver_to_json

    class TestParquetRead(unittest.TestCase):
        def test_read_parquet_success(self):
            # Setup mock
            mock_pq_file = MockParquetFile.return_value
            mock_pq_file.metadata.num_rows = 100
            
            # Mock batch iterator
            mock_df = pd.DataFrame([{"id": 1, "val": "a"}, {"id": 2, "val": "b"}])
            mock_batch = MagicMock()
            mock_batch.to_pandas.return_value = mock_df
            
            mock_pq_file.iter_batches.return_value = iter([MagicMock(to_pandas=lambda: mock_df)])

            # Call function
            result = read_parquet_conver_to_json(path="s3://bucket/test.parquet", limit=10)
            
            self.assertEqual(result["status"], "success")
            self.assertEqual(result["path"], "bucket/test.parquet")
            self.assertEqual(len(result["sample_rows"]), 2)
            self.assertEqual(result["sample_rows"][0]["id"], 1)

        def test_read_parquet_empty(self):
             # Setup mock for empty file
            mock_pq_file = MockParquetFile.return_value
            mock_pq_file.iter_batches.return_value = iter([])
            
            result = read_parquet_conver_to_json(path="s3://bucket/empty.parquet", limit=10)
            
            self.assertEqual(result["status"], "success")
            self.assertEqual(result["row_count_file"], 0)
            self.assertEqual(result["sample_rows"], [])

if __name__ == '__main__':
    unittest.main()
