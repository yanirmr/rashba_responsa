import pandas as pd
import pytest

from key_handler import KeyHandler

test_data = [
    ("א:צז", "א:צז", None),
    (pd.NA, None, "nan_key"),
    ("א:צז-א:צח", "א:צז-א:צח", "minus_key"),
    ("ב:ג (בסוף)", "ב:ג", "paren_key"),
    ("ב:ג מ:ד", ["מ:ד", "ב:ג"], "repeated_key"),
]


@pytest.mark.parametrize("input_key, expected_output, handler_name", test_data)
def test_key_handler(input_key, expected_output, handler_name):
    # Initialize the KeyHandler instance
    key_handler = KeyHandler()

    # Create a sample DataFrame
    data = {"key_col": [input_key]}
    df = pd.DataFrame(data)

    # Define the key_pattern and df_name for testing purposes
    key_pattern = r'^[\u0590-\u05FF]+'
    df_name = "test_df"

    # Clean and validate keys
    cleaned_keys = key_handler.clean_validate_keys(df, "key_col", key_pattern, df_name)

    # Check if the expected handler was called
    if handler_name is not None:
        assert key_handler.handler_counts[handler_name] == 1

    # Check if the output matches the expected output
    if isinstance(expected_output, str):
        assert cleaned_keys == {expected_output}
    elif isinstance(expected_output, list):
        assert cleaned_keys == set(expected_output)
    else:
        assert not cleaned_keys
