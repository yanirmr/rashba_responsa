import pytest

from main import format_print_marks_over_one_thousand, hebrew_to_integer, hebrew_keys_to_numeric_keys


@pytest.mark.parametrize("input_str,expected_output", [
    ("א-א:א", "א:א' א"),
    ("א-א:ב", "א:א' ב"),
    ("א-א:ד", "א:א' ד"),
    ("א-א:טו", "א:א' טו"),
    ("א-א:י", "א:א' י"),
    ("א-א:קכה", "א:א' קכה"),
    ("א-א:רנה", "א:א' רנה"),
])
def test_format_print_marks_over_one_thousand(input_str, expected_output):
    assert format_print_marks_over_one_thousand(input_str) == expected_output


def test_format_print_marks_over_one_thousand_string_error():
    with pytest.raises(ValueError):
        format_print_marks_over_one_thousand("א:י-יא")


@pytest.mark.parametrize("input_str,expected_output", [
    ("א", 1),
    ("יא", 11),
    ("טו", 15),
    ("יה", 15),
    ("קיא", 111),
    ("תתמד", 844),
    ("א' קכה", 1125),
])
def test_hebrew_to_integer(input_str, expected_output):
    assert hebrew_to_integer(input_str) == expected_output


@pytest.mark.parametrize("input_str,expected_output", [
    ("א:א", 1.0001),
    ("ב:א", 2.0001),
    ("א:יא", 1.0011),
    ("א:קיא", 1.0111),
    ("א:א' קיא", 1.1111),
])
def test_hebrew_keys_to_numeric_keys(input_str, expected_output):
    assert hebrew_keys_to_numeric_keys(input_str) == pytest.approx(expected_output, 0.0001)
