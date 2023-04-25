import csv
import json
import logging
import re

import pandas as pd

logger = logging.getLogger(__name__)


class KeyHandler:
    def __init__(self):
        self.handler_counts = {
            "nan_key": 0,
            "minus_key": 0,
            "paren_key": 0,
            "repeated_key": 0,
            "invalid_key": 0,
        }

    def register_handler(self, name, func):
        self.handlers[name] = func
        self.handler_counts[name] = 0

    def handle_invalid_key(self, key: str, filename: str):
        logger.warning(f"Invalid key found in file '{filename}': {key}")
        self.handler_counts['invalid_key'] += 1

    def handle_nan_key(self, key: str, filename: str):
        logger.warning(f"NaN key found in file '{filename}': {key}")
        self.handler_counts['nan_key'] += 1

    def handle_minus_key(self, key: str, filename: str):
        logger.warning(f"Minus key found in file '{filename}': {key}")
        self.handler_counts['minus_key'] += 1

    def handle_paren_key(self, key: str, filename: str):
        logger.warning(f"Paren key found in file '{filename}': {key}")
        self.handler_counts['paren_key'] += 1
        key = re.sub(r'[\(\)\[\]]', '', key)
        return key

    def handle_repeated_key(self, key, filename):
        logger.warning(f"Repeated key found in file '{filename}': {key}")
        self.handler_counts['repeated_key'] += 1
        key_parts = re.split(r'[\s\n]+', key)
        key_parts = [key_part.strip() for key_part in key_parts]
        return key_parts

    def clean_validate_keys(self, df, key_col, key_pattern, df_name):
        keys = set(df[key_col])
        clean_keys = set()
        for key in keys:
            if pd.isna(key):
                self.handle_nan_key(key, df_name)
            elif '-' in key:
                if re.match(key_pattern, key[2:]):  # check if the key is valid without the aleph-minus
                    clean_keys.add(key)
                else:
                    self.handle_minus_key(key, df_name)
            elif re.search(r'[\(\)\[\]]', key):
                key = self.handle_paren_key(key, df_name)
                clean_keys.add(key)
            elif re.search(fr'[\s\n]+{key_pattern}[\s\n]+{key_pattern}', key):
                keys = self.handle_repeated_key(key, df_name)
                clean_keys.update(keys)
            elif not re.match(key_pattern, key):
                self.handle_invalid_key(key, df_name)
            else:
                key = key.strip()
                clean_keys.add(key)
        return clean_keys

    def export_summary(self, export_format='json', file_path=None):
        if export_format == 'json':
            if file_path is None:
                file_path = 'handler_summary.json'
            with open(file_path, 'w') as f:
                json.dump(self.handler_counts, f, indent=4)
        elif export_format == 'csv':
            if file_path is None:
                file_path = 'handler_summary.csv'
            with open(file_path, 'w', newline='') as f:
                writer = csv.writer(f)
