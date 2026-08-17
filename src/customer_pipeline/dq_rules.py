"""Data quality rules - pure Python, no Spark.

Every rule is a small function that takes a value and returns True/False.
Because there is no Spark here, these run in milliseconds in CI and give
SonarQube a real code-coverage number.
"""

import re

EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[a-zA-Z]{2,}$")

VALID_SEGMENTS = frozenset({"RETAIL", "WHOLESALE", "ONLINE"})

MIN_AGE = 18
MAX_AGE = 120


def is_not_null(value):
    """Rule: the field must be present and not blank."""
    if value is None:
        return False
    if isinstance(value, str) and value.strip() == "":
        return False
    return True


def is_valid_email(value):
    """Rule: the field must look like an email address."""
    if not is_not_null(value):
        return False
    return bool(EMAIL_PATTERN.match(value.strip()))


def is_valid_age(value):
    """Rule: age must be a whole number between MIN_AGE and MAX_AGE."""
    if not isinstance(value, int) or isinstance(value, bool):
        return False
    return MIN_AGE <= value <= MAX_AGE


def is_valid_segment(value):
    """Rule: segment must be one of the allowed reference values."""
    if not is_not_null(value):
        return False
    return value.strip().upper() in VALID_SEGMENTS


def normalise_segment(value):
    """Standardise the segment value, or return UNKNOWN if it is not valid."""
    if not is_valid_segment(value):
        return "UNKNOWN"
    return value.strip().upper()


def validate_customer(row):
    """Run every rule against one customer row.

    Returns a list of failure names. An empty list means the row is clean.
    """
    failures = []
    if not is_not_null(row.get("customer_id")):
        failures.append("customer_id_null")
    if not is_valid_email(row.get("email")):
        failures.append("email_invalid")
    if not is_valid_age(row.get("age")):
        failures.append("age_out_of_range")
    if not is_valid_segment(row.get("segment")):
        failures.append("segment_unknown")
    return failures


def split_valid_invalid(rows):
    """Split a list of rows into (good_rows, quarantined_rows).

    Quarantined rows carry a dq_failures field so the reason is auditable.
    """
    good = []
    quarantined = []
    for row in rows:
        failures = validate_customer(row)
        if failures:
            quarantined.append({**row, "dq_failures": ",".join(failures)})
        else:
            good.append({**row, "segment": normalise_segment(row.get("segment"))})
    return good, quarantined
