import json
import logging
from pathlib import Path

import pandas as pd
import semantic_version

from key_handler import clean_validate_keys
from statistics import Statistics

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_csv_files_into_dict(folder_path: str) -> dict[str, pd.DataFrame]:
    """
    Read all CSV files in a folder into a dictionary of DataFrames.
    """
    dfs = {}
    folder = Path(folder_path)

    for file in folder.glob("*.csv"):
        df_name = f"df_{file.name}"
        dfs[df_name] = pd.read_csv(file, usecols=lambda col: col.lower() != "פתיחה וחתימה")

    return dfs


def filter_and_merge_dataframes(dfs: dict[str, pd.DataFrame], super_set: set, key_col: str) -> pd.DataFrame:
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

    merged_df = pd.concat(filtered_dfs).drop_duplicates(subset=key_col).reset_index(drop=True)
    return merged_df


if __name__ == "__main__":
    # Define the version number
    version_number = "0.6.2"
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

    merged_df = filter_and_merge_dataframes(dfs, super_set, key_col)

    merged_df.to_csv(f"merged_v{str(version)}.csv", index=False)

    # Create a dictionary to store the output statistics
    output_stats = stats.get_output_stats(len(super_set), len(dfs), str(version))

    # Write the output statistics to a JSON file
    with open(f'output_stats_v{str(version)}.json', 'w') as f:
        json.dump(output_stats, f, indent=4)

    logger.info(f"Statistics:\n{json.dumps(output_stats, indent=4)}")
