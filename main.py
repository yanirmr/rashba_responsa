# TODO: check why the problematic keys are not being counted correctly (negative values)
# TODO: make manual test on real data to verify results
# TODO: handle the files with low clean keys percentage

import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import semantic_version

from key_handler import KeyHandler
from statistics import Statistics

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def hebrew_to_integer(hebrew_numeral):
    """
    Converts a string representing a Hebrew numeral into an integer.

    Parameters:
    hebrew_numeral (str): A string representing a Hebrew numeral.

    Returns:
    int: The integer value of the Hebrew numeral.

    Raises:
    ValueError: If the input string contains characters that are not valid Hebrew numerals.
    """

    hebrew_values = {
        'א': 1,
        'ב': 2,
        'ג': 3,
        'ד': 4,
        'ה': 5,
        'ו': 6,
        'ז': 7,
        'ח': 8,
        'ט': 9,
        'י': 10,
        'כ': 20,
        'ל': 30,
        'מ': 40,
        'נ': 50,
        'ס': 60,
        'ע': 70,
        'פ': 80,
        'צ': 90,
        'ק': 100,
        'ר': 200,
        'ש': 300,
        'ת': 400,
    }

    total = 0
    for char in hebrew_numeral:
        if char in hebrew_values:
            total += hebrew_values[char]
        elif char == "'":
            total *= 1000
        elif char in [" ", ".", "ץ", "?", "-", ","]:
            continue
        else:
            raise ValueError(f"Invalid character '{char}' in Hebrew numeral.")

    return total

def hebrew_keys_to_numeric_keys(hebrew_keys):
    """
    Converts a string representing a key in Hebrew numerals into a float for key ordering.

    Parameters:
    hebrew_keys (str): A string representing a key in Hebrew numerals.

    Returns:
    float or str: The float representation of the key for ordering, or the original string if it is not in the expected format.
    """

    try:
        # Split the input string into two parts using ":" as a separator
        parts = hebrew_keys.split(':')
        if len(parts) != 2:
            raise ValueError("The input string is not in the expected format.")

        # Convert each part to an integer using the hebrew_to_integer function
        integer_part = hebrew_to_integer(parts[0])
        decimal_part = hebrew_to_integer(parts[1])

        # Construct the floating point number
        numeric_key = round(integer_part + (decimal_part / 10000), 4)

        return numeric_key

    except ValueError as e:
        # Log a warning with the input string
        logging.warning(f"{e} Input was: {hebrew_keys}")
        # Return the input string
        return hebrew_keys


def filter_values(cell):
    """     Filter out '--' and 'nan' values from a list within a DataFrame cell.
    :param cell: Cell content, which can either be a string or a list of strings/floats.
    :return: The original cell content, but in case of a list, without '--' and 'nan' values.
                         If a list is empty after filtering, None is returned instead.

    """
    if isinstance(cell, list):
        filtered_list = [x for x in cell if x != "--" and not pd.isna(x)]
        return filtered_list if filtered_list else None
    return cell


def format_print_marks_over_one_thousand(s: str) -> str:
    """
    This function takes a string formatted as "A-B:C",
    where A, B, and C are placeholders for actual string contents.
    It modifies the format to "A:B' C".

    :param s: A string formatted as "A-B:C".
    :return: A string formatted as "A:B' C" or the original string if it is not in the expected format.
    """

    try:
        # Check if input string is in the correct format
        if "-" not in s or ":" not in s:
            raise ValueError("The input string is not in the expected format.")

        # Split the string into three parts using "-" and ":" as separators
        first_part, remaining = s.split('-', 1)
        second_part, third_part = remaining.split(':', 1)

        return f"{first_part}:{second_part}' {third_part}"

    except ValueError as e:
        # Print a warning with the input string
        print(f"Warning: {e} Input was: {s}")
        # Return the input string as output
        return s


def list_to_string(l: list) -> str:
    """
    Convert a list to a semicolon-separated string without brackets.

    :param l: The list to convert.
    :return: The list as a string, with semicolons between elements and no brackets.
    """
    return '; '.join(str(e) for e in l if e is not None)


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
    filtered_dfs = {}

    for df_name, df in dfs.items():
        # Remove parentheses/brackets from key_col
        df[key_col] = df[key_col].apply(lambda x: key_handler.handle_key(x))
        exploded_df = df.explode(key_col)

        filtered_df = exploded_df[exploded_df[key_col].isin(super_set)]
        # Group the dataframe by 'key_col' and aggregate the values into a list
        filtered_df = filtered_df.groupby(key_col).agg(list).reset_index()

        filtered_dfs[df_name] = filtered_df

    merged_df = pd.DataFrame()

    for df_name, df in filtered_dfs.items():
        if merged_df.empty:
            merged_df = df
        else:
            merged_df = merged_df.merge(df, on=key_col, how='outer', suffixes=('', f'_{df_name}'))

    merged_df = merged_df.applymap(filter_values)

    return merged_df


if __name__ == "__main__":
    # Define the version number
    version_number = "1.0.2"
    version = semantic_version.Version(version_number)

    # Set input folder
    input_folder = "csv_formatted"
    key_col = 'דפוס'

    # Read all CSV files in input folder into a dictionary of DataFrames
    dfs = read_csv_files_into_dict(input_folder)

    stats = Statistics()

    # Initialize KeyHandler
    special_cases_filename = "special_cases.json"
    special_cases = KeyHandler.load_special_cases(special_cases_filename)
    key_handler = KeyHandler(special_cases)  # Add special_cases if needed

    # Clean and validate keys in each DataFrame and collect into a super set
    super_set = set()
    for df_name, df in dfs.items():
        clean_keys = set()
        for key in df[key_col]:
            cleaned_keys = key_handler.handle_key(key)
            clean_keys.update(cleaned_keys)
        super_set.update(clean_keys)
        all_keys = set(df[key_col])
        all_count = len(all_keys)
        nan_count = len(df[pd.isna(df[key_col])])
        clean_count = len(clean_keys)
        stats.update_df_stats(df_name, clean_count, all_count, nan_count)

    merged_df = filter_and_merge_dataframes(dfs, super_set, key_col)

    formatted_merged_df = merged_df.applymap(lambda cell: list_to_string(cell) if isinstance(cell, list) else cell)

    # run format_print_marks_over_one_thousand on each cell in 'דפוס" column if the cell inclides a '-' and a ':'
    formatted_merged_df[key_col] = formatted_merged_df[key_col].apply(
        lambda cell: format_print_marks_over_one_thousand(cell) if isinstance(cell,
                                                                              str) and '-' in cell and ':' in cell else cell)
    # rename the column "Jerusalem 1987_table_0_df_Jerusalem Benayahu O 204_table_0.csv" to "Benayahu O 204"
    formatted_merged_df = formatted_merged_df.rename(
        columns={"Jerusalem 1987_table_0_df_Jerusalem Benayahu O 204_table_0.csv": "Benayahu O 204"})
    # remove ".ץץ" from 'דפוס' column
    formatted_merged_df[key_col] = formatted_merged_df[key_col].apply(
        lambda cell: cell.replace(".ץץ", "") if isinstance(cell, str) else cell)

    # add new column of numeric keys based on hebrew keys from 'דפוס' column
    formatted_merged_df['numeric_key'] = formatted_merged_df[key_col].apply(
        lambda cell: hebrew_keys_to_numeric_keys(cell) if isinstance(cell, str) else cell)

    # make numeric_key column the first column in the dataframe
    formatted_merged_df = formatted_merged_df[
        ['numeric_key'] + [col for col in formatted_merged_df.columns if col != 'numeric_key']]
    # order the formatted_merged_df by numeric_key column
    # Replace string values in 'numeric_key' column with numpy.inf
    formatted_merged_df['numeric_key'] = pd.to_numeric(formatted_merged_df['numeric_key'], errors='coerce').fillna(
        np.inf)
    formatted_merged_df = formatted_merged_df.sort_values(by=['numeric_key'])
    formatted_merged_df.to_csv(Path("outputs") / f"rashba_responsa_index_{str(version)}.csv", index=False)

    # Create a dictionary to store the output statistics
    output_stats = stats.get_output_stats(len(super_set), len(dfs), str(version))

    # Write the output statistics to a JSON file
    with open(Path("outputs") / f'output_stats_v{str(version)}.json', 'w') as f:
        json.dump(output_stats, f, indent=4)

    logger.info(f"Statistics:\n{json.dumps(output_stats, indent=4)}")

    # Save unmatched keys
    key_handler.save_unmatched_keys(Path("outputs") / f'unmatched_keys_v{str(version)}.txt')
    logger.info(f"Number of unmatched keys: {len(key_handler.unmatched_keys)}")
