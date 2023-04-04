import json
import logging
import os
import re

import pandas as pd
import semantic_version

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_csv_files_into_dict(folder_path: str):
    """
    Read all CSV files in a folder into a dictionary of DataFrames.
    """
    dfs = {}
    for i, filename in enumerate(os.listdir(folder_path)):
        if filename.endswith(".csv"):
            csv_path = os.path.join(folder_path, filename)
            df_name = f"df_{filename}"
            dfs[df_name] = pd.read_csv(csv_path, usecols=lambda col: col.lower() != "פתיחה וחתימה")
    return dfs


def handle_invalid_key(key: str, filename: str):
    logger.warning(f"Invalid key found in file '{filename}': {key}")


def handle_nan_key(key: str, filename: str):
    logger.warning(f"NaN key found in file '{filename}': {key}")


def handle_minus_key(key: str, filename: str):
    logger.warning(f"Minus key found in file '{filename}': {key}")


def handle_paren_key(key: str, filename: str):
    logger.warning(f"Paren key found in file '{filename}': {key}")


def handle_repeated_key(key, filename):
    logger.warning(f"Repeated key found in file '{filename}': {key}")


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
            handle_minus_key(key, df_name)
        elif re.search(r'[\(\)\[\]]', key):
            handle_paren_key(key, df_name)
        elif re.search(r'^[\u0590-\u05FF]+[\s\n]+[\u0590-\u05FF]+:[\u0590-\u05FF]+$', key):
            handle_repeated_key(key, df_name)
        elif not re.match(key_pattern, key):
            handle_invalid_key(key, df_name)
        else:
            key = key.strip()
            clean_keys.add(key)
    return clean_keys


def merge_filtered_dfs(filtered_dfs: list, key_col: str):
    """
    Merge a list of filtered DataFrames on a common key column.
    Returns the merged DataFrame.
    """
    merged_df = pd.concat(filtered_dfs).drop_duplicates(subset=key_col).reset_index(drop=True)
    return merged_df


if __name__ == "__main__":
    # Define the version number
    version_number = "0.1.0"
    version = semantic_version.Version(version_number)

    # Set input folder
    input_folder = "csv_formatted"
    key_pattern = r'^[\u0590-\u05FF]+:[\u0590-\u05FF]+$'
    key_col = 'דפוס'

    # Read all CSV files in input folder into a dictionary of DataFrames
    dfs = read_csv_files_into_dict(input_folder)

    # Clean and validate keys in each DataFrame and collect into a super set
    super_set = set()
    for df_name, df in dfs.items():
        clean_keys = clean_validate_keys(df, key_col, key_pattern, df_name)
        super_set.update(clean_keys)

    # Filter and merge the DataFrames based on the cleaned and validated keys
    filtered_dfs = []
    for df_name, df in dfs.items():
        filtered_df = df[df[key_col].isin(super_set)]
        filtered_dfs.append(filtered_df)
    merged_df = merge_filtered_dfs(filtered_dfs, key_col)

    # Create a dictionary to store the output statistics
    output_stats = {}

    # Add number of clean keys to the output dictionary
    output_stats['num_clean_keys'] = len(super_set)
    output_stats['num_of_files'] = len(dfs)
    output_stats['version'] = str(version)

    # Add statistics for each DataFrame to the output dictionary
    df_stats = {}
    for df_name, df in dfs.items():
        all_keys = set(df[key_col])
        clean_keys = set(df[key_col][df[key_col].isin(super_set)])
        clean_count = len(clean_keys)
        all_count = len(all_keys)
        clean_percent = clean_count / all_count * 100 if all_count > 0 else 0
        df_stats[df_name] = {
            'clean_count': clean_count,
            'all_count': all_count,
            'problematic keys': all_count - clean_count,
            'clean_percent': clean_percent
        }
    output_stats['df_stats'] = df_stats

    # Save the output statistics to a JSON file
    with open(f'output_stats_v{str(version)}.json', 'w') as f:
        json.dump(output_stats, f, indent=4)

    logger.info(f"Statistics:\n{json.dumps(output_stats, indent=4)}")
