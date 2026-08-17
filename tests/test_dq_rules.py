"""Unit tests for the data quality rules.

No Spark, no cluster, no network - these run in about a second and produce
the coverage.xml file that SonarQube reads.
"""

import pytest

from customer_pipeline import dq_rules as r


@pytest.mark.parametrize(
    "value,expected",
    [("abc", True), ("", False), ("   ", False), (None, False), (0, True)],
)
def test_is_not_null(value, expected):
    assert r.is_not_null(value) is expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("thilak@example.com", True),
        ("  thilak@example.com  ", True),
        ("thilak.example.com", False),
        ("thilak@example", False),
        ("@example.com", False),
        (None, False),
        ("", False),
    ],
)
def test_is_valid_email(value, expected):
    assert r.is_valid_email(value) is expected


@pytest.mark.parametrize(
    "value,expected",
    [(18, True), (120, True), (17, False), (121, False), ("30", False), (None, False), (True, False)],
)
def test_is_valid_age(value, expected):
    assert r.is_valid_age(value) is expected


@pytest.mark.parametrize(
    "value,expected",
    [("RETAIL", True), ("retail", True), (" online ", True), ("B2B", False), (None, False)],
)
def test_is_valid_segment(value, expected):
    assert r.is_valid_segment(value) is expected


@pytest.mark.parametrize(
    "value,expected",
    [("retail", "RETAIL"), (" Online ", "ONLINE"), ("B2B", "UNKNOWN"), (None, "UNKNOWN")],
)
def test_normalise_segment(value, expected):
    assert r.normalise_segment(value) == expected


def test_validate_customer_clean_row():
    row = {"customer_id": "C1", "email": "a@b.com", "age": 30, "segment": "RETAIL"}
    assert r.validate_customer(row) == []


def test_validate_customer_collects_every_failure():
    row = {"customer_id": None, "email": "nope", "age": 5, "segment": "B2B"}
    assert r.validate_customer(row) == [
        "customer_id_null",
        "email_invalid",
        "age_out_of_range",
        "segment_unknown",
    ]


def test_split_valid_invalid():
    rows = [
        {"customer_id": "C1", "email": "a@b.com", "age": 30, "segment": "retail"},
        {"customer_id": "C2", "email": "bad", "age": 30, "segment": "RETAIL"},
    ]
    good, quarantined = r.split_valid_invalid(rows)

    assert len(good) == 1
    assert good[0]["segment"] == "RETAIL"

    assert len(quarantined) == 1
    assert quarantined[0]["dq_failures"] == "email_invalid"
