import unittest
import pandas as pd
from update_database import process_data

class TestDataProcessing(unittest.TestCase):
    def test_process_data(self):
        """Tests the processing of date data."""
        test_data = pd.DataFrame({
            'date': ['2022-09-12', 'invalid_date', '0-09-12', '0-0-0', '0/0/0', 'abc', '2024/06/20'],
            'value': [1, 2, 3, 4, 5, 6, 7]
        })

        processed_data = process_data(test_data)

        # Check that valid dates are correctly parsed
        self.assertEqual(processed_data['date'][0], pd.Timestamp('2022-09-12'))

        # Check that invalid dates are set to NaT
        self.assertFalse(pd.isna(processed_data['date'][0]))
        self.assertTrue(pd.isna(processed_data['date'][1]))
        self.assertTrue(pd.isna(processed_data['date'][2]))
        self.assertTrue(pd.isna(processed_data['date'][3]))
        self.assertTrue(pd.isna(processed_data['date'][4]))
        self.assertTrue(pd.isna(processed_data['date'][5]))
        self.assertTrue(pd.isna(processed_data['date'][6]))

if __name__ == '__main__':
    unittest.main()
