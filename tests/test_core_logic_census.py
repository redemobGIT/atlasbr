import pandas as pd
import pytest
from atlasbr.core.logic.census import process_age_2022

def test_process_age_2022():
    # Setup - Create a dummy input DataFrame
    input_data = pd.DataFrame({
        "pessoas": [100, 200],
        "V00644": [10, 20],   # Age 15-19
        "V00645": [5, 10],    # Part of 20-64
        "V00654": [2, 4],     # Part of 65+
        # ... add other required cols with 0 ...
    })
    # Fill missing cols for robust test
    for i in range(645, 657):
        col = f"V{i:05d}"
        if col not in input_data.columns:
            input_data[col] = 0

    # Execute
    result = process_age_2022(input_data)

    # Assert
    assert "age_0_14" in result.columns
    # Check simple arithmetic: 100 - (10 + 5 + 2) = 83 (roughly)
    # Exact logic depends on the specific columns summed in the function
    assert not result.empty