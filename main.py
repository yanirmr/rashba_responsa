# TODO: check why the problematic keys are not being counted correctly (negative values)
# TODO: make manual test on real data to verify results
# TODO: handle the files with low clean keys percentage

import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import semantic_version
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

from key_handler import KeyHandler
from statistics import Statistics

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_index_pdf(df, pdf_filename):
    pdf = canvas.Canvas(pdf_filename, pagesize=letter)
    width, height = letter

    x = 50  # Horizontal position
    y = height - 50  # Vertical position, starting from top

    for index, row in df.iterrows():
        key = row[0]
        pdf.setFont("Helvetica", 14)
        pdf.drawString(x, y, f'{key}:')
        y -= 20  # Move to next line

        # Loop through each document
        for i in range(1, len(row)):
            if pd.notnull(row[i]):  # Check if value is not null
                doc = df.columns[i]
                value = row[i]
                pdf.setFont("Helvetica", 12)
                pdf.drawString(x + 10, y, f'{doc}: {value}')  # Indent doc details
                y -= 20  # Move to next line
        y -= 10  # Add extra space between keys

        # Check if we're running out of space and need a new page
        if y < 50:
            pdf.showPage()
            y = height - 50  # Reset y

    pdf.save()


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
    # Exclude None or NaN values
    l = [e for e in l if e is not None and e == e]

    return '; '.join(str(e) for e in l) if l else ''


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


def organized_df_cols(formatted_merged_df):
    # llst of columsn pairs to bo concatenated and column target names
    cols_to_concat = [
        (["Cambridge 499_table_0", """קמברידג' 499 ח"ב"""], "קמברידג' 499"),
        (["לונדון 10753", "לונדון 10753 סימן ודף", "לונדון 10753_df_HaBattim vol 2_table_1.csv"], "לונדון 10753 מאוחד"),
        (["ניו יורק 1383", "ניו יורק 1383_df_HaBattim vol 2_table_0.csv", "ניו יורק 1383 - השלמות",
          "ניו יורק 1383_df_HaBattim vol 2_table_1.csv", "ניו יורק 1383 - השלמות_df_HaBattim vol 2_table_1.csv",
          "New York 1383 - Additions_table_0", "פתיחה_df_New York 1383 - Additions_table_0.csv"],
         "ניו יורק 1383 מאוחד"),
        (["London BL 571_table_0", "London BL 571_table_1", "London BL 571_table_2"], "לונדון 571 חאוחד"),
        (["מונטיפיורי 103 סימן ודף", "מונטיפיורי 103 דף"], "מונטיפיורי 103 מאוחד"),
        (
            ["Montefiore 102_table_0", "מונטיפיורי 102", "מונטיפיורי 102_df_Vercelli_table_0.csv"],
            "מונטיפיורי 102 מאוחד"),
        (["סימן ודף לונדון 573", "פתיחה וחתימה.1_df_Moscow 550_London 573_table_1.csv"], "לונדון 573 מאוחד"),

    ]

    # concatenate columns
    for cols, target in cols_to_concat:
        formatted_merged_df[target] = formatted_merged_df[cols].apply(lambda x: list_to_string(x), axis=1)
        formatted_merged_df = formatted_merged_df.drop(columns=cols)
    formatted_merged_df = formatted_merged_df.rename(
        columns={"Cambridge 498 - part 2_table_0": "קמברידג' 498",
                 "Cambridge 500_table_0": "קמברידג' 500",
                 "לונדון 573 מאוחד": "לונדון 573",
                 "ניו יורק 1423": "ניו יורק 1423",
                 "אוקספורד 2365 כרך א סימן ודף": "אוקספורד 2365 כרך א",
                 "ניו יורק 1383 מאוחד": 'ניו יורק 1383',
                 'אוקספורד 2365 כרך ב': 'אוקספורד 2365 כרך ב',
                 'לונדון 10753 מאוחד': 'לונדון 10753',
                 'מוסקבה 549': 'מוסקבה 549',
                 'מוסקבה 549_df_HaBattim vol 2_table_1.csv': 'מוסקבה 549 הבתים',
                 "ירושלים 1987": "ירושלים 1987 - 2",
                 'Jerusalem 1987_table_0': 'ירושלים 1987',
                 'כ"י וושינגטון 157': 'וושינגטון 157',
                 'אוקספורד סימן ודף': 'אוקספורד 815',
                 'Benayahu O 204': 'בניהו ע 204',
                 'London BL 569_table_0': 'לונדון 569',
                 'כ"י מונטיפיורי 124': 'מונטיפיורי 124',
                 'London BL 570_table_0': 'לונדון 570',
                 'לונדון 571 מאוחד': 'לונדון 571',
                 'כ"י שוקן 2055': 'שוקן 2055',
                 'London BL 572_table_0': 'לונדון 572',
                 'Montefiore 102_table_0': 'מונטיפיורי 102',
                 'כ"י מונטיפיורי 130 סימן ודף': 'מונטיפיורי 130',
                 'כ"י מונטיפיורי 100 דף': 'מונטיפיורי 100',
                 "מונטיפיורי 103 מאוחד": 'מונטיפיורי 103',
                 "מונטיפיורי 102 מאוחד": 'מונטיפיורי 102',
                 'Moscow 1378_table_0': 'מוסקבה 1378',
                 'מוסקבה 527 סימן ודף': 'מוסקבה 527',
                 'Moscow 595_table_0': 'מוסקבה 595',
                 'New York 1422_table_0': 'ניו יורק 1422',
                 'New York 1476_table_0': 'ניו יורק 1476',
                 'New York 9689_table_0': 'ניו יורק 9689',
                 'Orhot Haim II_table_0': 'אורחות חיים ח"ב מוסקבה 107',
                 'אוקספורד 2550 סימן ודף': 'אוקספורד 2550',
                 'כ"י אלפנדארי': 'אלפנדארי',
                 'לונדון 10099': 'לונדון 10099',
                 'Oxford 670_table_0': 'מרדכי אוקספורד 670',
                 'אוקספורד 781 סימן ודף': 'אוקספורד 781',
                 'ירושלים 90': 'ירושלים 90',
                 'מוסקבה 1013': 'מוסקבה 1013',
                 'Oxford 817_table_0': 'אוקספורד 817',
                 'Paris 411_table_0': 'פריז 411',
                 'Paris 416_table_0': 'פריז 416',
                 'Parma 3525_table_0': 'פרמה 3525',
                 'פריז 585': 'פריז 585',
                 'Parma 426_2605_table_0': 'פרמה 2605',
                 'Small collections_table_0': 'דפוס קושטא רעו',
                 'Small collections_table_1': 'ניו יורק 2425',
                 'Small collections_table_10': 'גניזה שטרסבורג 4104.7',
                 'Small collections_table_11': "גניזה קמברידג' TS 13J24.26",
                 'Small collections_table_2': 'מינכן 356/14',
                 'Small collections_table_3': 'וושינגטון 157',
                 'Small collections_table_4': 'ירושלים 7817',
                 'Small collections_table_5': 'ירושלים 1800.713',
                 'Small collections_table_6': "גניזה קמברידג' TS AS 91.289 + TS AS 82.238",
                 'Small collections_table_7': 'גניזה ניו יורק 2397.9',
                 'Small collections_table_8': 'גניזה ניו יורק 2626.1-4',
                 'Small collections_table_9': 'גניזה PARIS AIU III B 134 + ניו יורק, JTS ENA 3153.3',
                 'Vercelli_table_0': "מרדכי וירצ'לי 1",
                 'סימן ודף כ"י מוסקבה': 'מוסקבה 550', })

    # delete the col that is name is "Cambridge 498 - part 1_table_0"
    formatted_merged_df = formatted_merged_df.drop(
        columns=["Cambridge 498 - part 1_table_0",
                 "פתיחה",
                 "פתיחה_df_Small collections_table_2.csv",
                 "פתיחה וחתימה.1",
                 "אוקספורד 2365 כרך ב_df_HaBattim vol 2_table_1.csv",
                 "פתיחה_df_Orhot Haim II_table_0.csv"])

    # Save the 'numeric_key' and 'דפוס' columns
    numeric_key = formatted_merged_df['numeric_key']
    dafus = formatted_merged_df['דפוס']

    # Drop the 'numeric_key' and 'דפוס' columns from the dataframe
    formatted_merged_df = formatted_merged_df.drop(['numeric_key', 'דפוס'], axis=1)

    # Sort the rest of the columns alphabetically
    formatted_merged_df = formatted_merged_df.sort_index(axis=1)

    # Add the 'numeric_key' and 'דפוס' columns back at the beginning
    formatted_merged_df.insert(0, 'דפוס', dafus)
    formatted_merged_df.insert(0, 'numeric_key', numeric_key)

    return formatted_merged_df


if __name__ == "__main__":
    # Define the version number
    version_number = "1.1.0"
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

    # remove ".ץץ" from 'דפוס' column
    formatted_merged_df[key_col] = formatted_merged_df[key_col].apply(
        lambda cell: cell.replace(".ץץ", "") if isinstance(cell, str) else cell)

    # add new column of numeric keys based on hebrew keys from 'דפוס' column
    formatted_merged_df['numeric_key'] = formatted_merged_df[key_col].apply(
        lambda cell: hebrew_keys_to_numeric_keys(cell) if isinstance(cell, str) else cell)

    formatted_merged_df = organized_df_cols(formatted_merged_df)

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

    # create_index_pdf(formatted_merged_df, f"outputs\\rashba_responsa_index_{str(version)}.pdf")
