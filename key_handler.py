import logging

logger = logging.getLogger(__name__)

import re
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def handle_invalid_key(key: str, filename: str):
    logger.warning(f"Invalid key found in file '{filename}': {key}")


def handle_nan_key(key: str, filename: str):
    logger.warning(f"NaN key found in file '{filename}': {key}")


def handle_minus_key(key: str, filename: str):
    logger.warning(f"Minus key found in file '{filename}': {key}")


def handle_paren_key(key: str, filename: str):
    logger.warning(f"Paren key found in file '{filename}': {key}")
    key = re.sub(r'[\(\)\[\]]', '', key)
    return key


def handle_repeated_key(key, filename):
    logger.warning(f"Repeated key found in file '{filename}': {key}")
    key_parts = re.split(r'[\s\n]+', key)
    key_parts = [key_part.strip() for key_part in key_parts]
    return key_parts


def clean_validate_keys(df, key_col, key_pattern, df_name):
    """
    Clean and validate keys in a DataFrame using a regular expression pattern and handlers for different types of invalid keys.
    Returns a set of cleaned and validated keys.
    df: DataFrame to clean and validate keys in
    key_col: column name of the key column
    key_pattern: regular expression pattern to validate keys
    df_name: name of the DataFrame based on the file name
    """
    keys = set(df[key_col])
    clean_keys = set()
    for key in keys:
        if pd.isna(key):
            handle_nan_key(key, df_name)
        elif '-' in key:
            if re.match(key_pattern, key[2:]):  # check if the key is valid without the aleph-minus
                clean_keys.add(key)
            else:
                handle_minus_key(key, df_name)
        elif re.search(r'[\(\)\[\]]', key):
            key = handle_paren_key(key, df_name)
            clean_keys.add(key)
        elif re.search(fr'[\s\n]+{key_pattern}[\s\n]+{key_pattern}', key):
            keys = handle_repeated_key(key, df_name)
            clean_keys.update(keys)
        elif not re.match(key_pattern, key):
            handle_invalid_key(key, df_name)
        else:
            key = key.strip()
            clean_keys.add(key)
    return clean_keys
