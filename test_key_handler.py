import pytest

from key_handler import KeyHandler

special_cases = {
    "א:יט-א:כב": ["א:יט", "א:כב"]
}


@pytest.fixture
def key_handler():
    return KeyHandler(special_cases=special_cases)


@pytest.mark.parametrize("input_key,expected_output", [
    (None, []),
    ("", []),
    ("א:קז", ["א:קז"]),
    ("ג:רמט ", ["ג:רמט"]),
    ("א:יט  ב:טו", ["א:יט", "ב:טו"]),
    ("א:יט \n  ב:טו", ["א:יט", "ב:טו"]),
    ("א:יט \t ב:טו", ["א:יט", "ב:טו"]),
    ("א:יט + ב:טו", ["א:יט", "ב:טו"]),
    ("א-א:קסז", ["א-א:קסז"]),
    ("א-א:רמט   א-א:ג", ["א-א:רמט", "א-א:ג"]),
    ("א-א:רמט   (א-א:ג)", ["א-א:רמט", "א-א:ג"]),
    ("א-א:רמט   [א-א:ג]", ["א-א:רמט", "א-א:ג"]),
    ("א-א:רמט   [סוף]", ["א-א:רמט"]),
    ("א-א:רמט   (אמצע)", ["א-א:רמט"]),
    ("א:יט-א:כב", ["א:יט", "א:כב"]),
])
def test_key_handler(input_key, expected_output, key_handler):
    assert key_handler.handle_key(input_key) == expected_output


def test_unmatched_key(key_handler):
    input_key = "א:א - א:ג"
    expected_output = []
    unmatched_key = "א:א - א:ג"

    assert key_handler.handle_key(input_key) == expected_output
    assert unmatched_key in key_handler.unmatched_keys
