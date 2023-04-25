import logging
import re

logger = logging.getLogger(__name__)


class KeyHandler:
    def __init__(self, special_cases=None):
        self.special_cases = special_cases if special_cases is not None else {}
        self.pattern = r'^[\u0590-\u05FF]+:[\u0590-\u05FF]+'
        self.pattern_with_prefix = r'^[\u0590-\u05FF]-[\u0590-\u05FF]+:[\u0590-\u05FF]+'
        self.unmatched_keys = []

    def handle_key(self, key):
        if key is None or key == "":
            return []

        if key in self.special_cases:
            return self.special_cases[key]

        # Remove parentheses and brackets
        key = re.sub(r'[\(\)\[\]]', '', key)

        # Split the key by plus sign, whitespace, and newline
        delimiters = r'\s+|\n+|\t+|\++'
        keys = re.split(delimiters, key)

        processed_keys = []
        for k in keys:
            k = k.strip()
            if re.match(self.pattern, k) or re.match(self.pattern_with_prefix, k):
                processed_keys.append(k)
            else:
                logger.warning(f"Unmatched key: {k}")
                self.unmatched_keys.append(k)

        return processed_keys

    def save_unmatched_keys(self, filename):
        with open(filename, 'w') as f:
            for key in self.unmatched_keys:
                f.write(f"{key}\n")
