import pandas as pd

# import pytest
from src.data_pipeline.preprocessing.cleaning import (
    clean_credit_card_data,
    clean_transaction_data,
)


def test_clean_credit_card_data():
    data = [
        {
            "card_id": "1",
            "card_name": "A",
            "issuer": "amex",
            "annual_fee": 0,
            "reward_rates": None,
        },
        {
            "card_id": "1",
            "card_name": "A",
            "issuer": "amex",
            "annual_fee": 0,
            "reward_rates": None,
        },
        {
            "card_id": "2",
            "card_name": "B",
            "issuer": "Chase",
            "annual_fee": 2000,
            "reward_rates": {"universal_base_rate": 2.0},
        },
        {
            "card_id": "3",
            "card_name": "C",
            "issuer": "Citi",
            "annual_fee": 95,
            "reward_rates": None,
        },
    ]
    df = pd.DataFrame(data)
    cleaned, report = clean_credit_card_data(df)
    assert report["initial_count"] == 4
    assert report["after_dedup"] == 3
    assert report["missing_reward_rates"] == 2
    assert report["invalid_annual_fees"] == 1
    assert report["final_count"] == 2
    assert all(cleaned["annual_fee"] < 1000)
    assert cleaned["issuer"].str.isupper().all()


def test_clean_credit_card_data_no_card_id():
    # No card_id, dedup by card_name+issuer
    data = [
        {"card_name": "A", "issuer": "amex", "annual_fee": 0, "reward_rates": None},
        {"card_name": "A", "issuer": "amex", "annual_fee": 0, "reward_rates": None},
        {"card_name": "B", "issuer": "Chase", "annual_fee": 1000, "reward_rates": None},
        {"card_name": "C", "issuer": "Citi", "annual_fee": 999, "reward_rates": None},
    ]
    df = pd.DataFrame(data)
    cleaned, report = clean_credit_card_data(df)
    assert report["dedup_key"] == "card_name+issuer"
    assert report["dedup_removed"] == 1
    assert report["invalid_annual_fees"] == 1
    assert report["annual_fee_removed"] == 1
    assert all(cleaned["annual_fee"] < 1000)


def test_clean_credit_card_data_no_dedup_key():
    # No card_id, no card_name+issuer
    data = [
        {"issuer": "amex", "annual_fee": 0, "reward_rates": None},
        {"issuer": "amex", "annual_fee": 0, "reward_rates": None},
    ]
    df = pd.DataFrame(data)
    cleaned, report = clean_credit_card_data(df)
    assert report["dedup_key"] == "none"
    assert report["dedup_removed"] == 0


def test_clean_credit_card_data_missing_columns():
    # No reward_rates, no issuer, no annual_fee
    data = [
        {"card_id": "1"},
        {"card_id": "2"},
    ]
    df = pd.DataFrame(data)
    cleaned, report = clean_credit_card_data(df)
    assert report["missing_reward_rates"] == 0
    assert report["unique_issuers"] == 0
    assert report["invalid_annual_fees"] == 0
    assert report["annual_fee_removed"] == 0


def test_clean_transaction_data():
    data = [
        {
            "transaction_id": "t1",
            "user_id": "u1",
            "date": "2025-08-01",
            "category": None,
            "amount": 10,
        },
        {
            "transaction_id": "t2",
            "user_id": "u1",
            "date": "2027-01-01",
            "category": "dining",
            "amount": 20,
        },
        {
            "transaction_id": "t3",
            "user_id": "u1",
            "date": "2025-08-01",
            "category": "travel",
            "amount": -5,
        },
        {
            "transaction_id": "t4",
            "user_id": "u1",
            "date": "2025-08-01",
            "category": "shopping",
            "amount": 20000,
        },
    ]
    df = pd.DataFrame(data)
    cleaned, report = clean_transaction_data(df)
    assert report["initial_count"] == 4
    assert report["negative_amounts"] == 1
    assert report["future_dates"] == 1
    assert report["missing_categories"] == 1
    assert report["suspicious_high_amounts"] == 1
    assert report["final_count"] == 2
    assert (cleaned["amount"] >= 0).all()
    assert (pd.to_datetime(cleaned["date"]) <= pd.Timestamp.today()).all()
    assert "suspicious" in cleaned.columns
    assert cleaned["category"].isnull().sum() == 0


def test_clean_transaction_data_missing_columns():
    # No amount, no date, no category
    data = [
        {"transaction_id": "t1", "user_id": "u1"},
        {"transaction_id": "t2", "user_id": "u2"},
    ]
    df = pd.DataFrame(data)
    cleaned, report = clean_transaction_data(df)
    assert report["negative_amounts"] == 0
    assert report["invalid_dates"] == 0
    assert report["missing_categories"] == 0
    assert "suspicious" in cleaned.columns


def test_clean_transaction_data_invalid_dates():
    # Invalid date string
    data = [
        {"transaction_id": "t1", "user_id": "u1", "date": "notadate", "amount": 10},
        {"transaction_id": "t2", "user_id": "u2", "date": "2025-08-01", "amount": 20},
    ]
    df = pd.DataFrame(data)
    cleaned, report = clean_transaction_data(df)
    assert report["invalid_dates"] == 1
    assert report["dates_removed"] == 1
    assert len(cleaned) == 1
