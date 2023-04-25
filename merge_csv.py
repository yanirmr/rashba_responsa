import json
import logging
import os

import pandas as pd
import semantic_version

from key_handler import clean_validate_keys
from statistics import Statistics

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


def merge_filtered_dfs(filtered_dfs: list, key_col: str):
    """
    Merge a list of filtered DataFrames on a common key column.
    Returns the merged DataFrame.
    """
    merged_df = pd.concat(filtered_dfs).drop_duplicates(subset=key_col).reset_index(drop=True)
    return merged_df


if __name__ == "__main__":
    # Define the version number
    version_number = "0.5.2"
    version = semantic_version.Version(version_number)

    # Set input folder
    input_folder = "csv_formatted"
    key_pattern = r'^[\u0590-\u05FF]+:[\u0590-\u05FF]+'
    key_col = 'דפוס'

    # Read all CSV files in input folder into a dictionary of DataFrames
    dfs = read_csv_files_into_dict(input_folder)

    stats = Statistics()
    # Clean and validate keys in each DataFrame and collect into a super set
    super_set = set()
    for df_name, df in dfs.items():
        clean_keys = clean_validate_keys(df, key_col, key_pattern, df_name)
        super_set.update(clean_keys)
        all_keys = set(df[key_col])
        clean_keys = set(df[key_col][df[key_col].isin(super_set)])
        all_count = len(all_keys)
        nan_count = len(df[pd.isna(df[key_col])])
        clean_count = len(clean_keys)
        stats.update_df_stats(df_name, clean_count, all_count, nan_count)

    # Filter and merge the DataFrames based on the cleaned and validated keys
    filtered_dfs = []
    for df_name, df in dfs.items():
        # Remove parentheses/brackets from key_col
        df[key_col] = df[key_col].astype(str).str.replace(r'[\(\)\[\]]', '', regex=True)

        # Split key_col by plus sign, whitespace, and newline
        delimiters = r'\s+|\n+|\++'
        df[key_col] = df[key_col].str.split(delimiters)
        exploded_df = df.explode(key_col)

        filtered_df = exploded_df[exploded_df[key_col].isin(super_set)]
        filtered_dfs.append(filtered_df)

        all_keys = set(exploded_df[key_col])
        clean_keys = set(filtered_df[key_col])
        all_count = len(all_keys)
        # count how many records are nan or empty strings in exploded_df[key_col]
        nan_count = len(exploded_df[key_col][exploded_df[key_col] == 'nan']) + \
                    len(exploded_df[key_col][exploded_df[key_col] == ''])

    merged_df = merge_filtered_dfs(filtered_dfs, key_col)
    merged_df.to_csv(f"merged_v{str(version)}.csv", index=False)

    # Create a dictionary to store the output statistics
    output_stats = stats.get_output_stats(len(super_set), len(dfs), str(version))

    # Write the output statistics to a JSON file
    with open(f'output_stats_v{str(version)}.json', 'w') as f:
        json.dump(output_stats, f, indent=4)

    logger.info(f"Statistics:\n{json.dumps(output_stats, indent=4)}")
