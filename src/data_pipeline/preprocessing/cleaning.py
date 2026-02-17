import logging
from typing import Tuple, Dict, Any
import pandas as pd

logger = logging.getLogger(__name__)


def clean_credit_card_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Cleans credit card data by handling missing values, duplicates, and invalid entries.
    Returns cleaned DataFrame and cleaning report.
    Idempotent and logs all cleaning steps.
    """
    df = df.copy()
    report: Dict[str, Any] = {}

    report["initial_count"] = len(df)

    # Deduplication
    before = len(df)
    if "card_id" in df.columns:
        df = df.drop_duplicates(subset=["card_id"])
        report["dedup_key"] = "card_id"
    elif "card_name" in df.columns and "issuer" in df.columns:
        df = df.drop_duplicates(subset=["card_name", "issuer"])
        report["dedup_key"] = "card_name+issuer"
    else:
        report["dedup_key"] = "none"
    report["after_dedup"] = len(df)
    report["dedup_removed"] = before - len(df)

    # Handle missing reward rates (impute with 1.0 or flag)
    if "reward_rates" in df.columns:
        missing_reward = int(df["reward_rates"].isnull().sum())
        df["reward_rates"] = df["reward_rates"].apply(
            lambda x: x if pd.notnull(x) else {"universal_base_rate": 1.0}
        )
        report["missing_reward_rates"] = missing_reward
    else:
        report["missing_reward_rates"] = 0

    # Standardize issuer names
    if "issuer" in df.columns:
        df["issuer"] = (
            df["issuer"].astype("string").str.upper().str.replace("_", " ").str.strip()
        )
        report["unique_issuers"] = int(df["issuer"].nunique())
    else:
        report["unique_issuers"] = 0

    # Validate annual fee ranges (0 <= annual_fee < 1000)
    if "annual_fee" in df.columns:
        df["annual_fee"] = pd.to_numeric(df["annual_fee"], errors="coerce")
        invalid_mask = (
            (df["annual_fee"] < 0)
            | (df["annual_fee"] >= 1000)
            | df["annual_fee"].isna()
        )
        report["invalid_annual_fees"] = int(invalid_mask.sum())
        before = len(df)
        df = df.loc[~invalid_mask].copy()
        report["annual_fee_removed"] = before - len(df)
    else:
        report["invalid_annual_fees"] = 0
        report["annual_fee_removed"] = 0

    report["final_count"] = len(df)
    logger.info("clean_credit_card_data report=%s", report)
    return df.reset_index(drop=True), report


def clean_transaction_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Cleans transaction data by removing invalid transactions, handling missing categories, and flagging suspicious patterns.
    Returns cleaned DataFrame and cleaning report.
    Idempotent and logs all cleaning steps.
    """
    df = df.copy()
    report: Dict[str, Any] = {}

    report["initial_count"] = len(df)
    now = pd.Timestamp.now()

    # Amount
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        neg_mask = df["amount"] < 0
        report["negative_amounts"] = int(neg_mask.sum())
        before = len(df)
        df = df.loc[~neg_mask].copy()
        report["negative_amounts_removed"] = before - len(df)
    else:
        report["negative_amounts"] = 0
        report["negative_amounts_removed"] = 0

    # Date (robust)
    if "date" in df.columns:
        dt = pd.to_datetime(df["date"], errors="coerce")
        invalid_mask = dt.isna()
        report["invalid_dates"] = int(invalid_mask.sum())
        future_mask = dt > now
        report["future_dates"] = int(future_mask.sum())
        drop_mask = invalid_mask | future_mask
        before = len(df)
        df = df.loc[~drop_mask].copy()
        report["dates_removed"] = before - len(df)
    else:
        report["invalid_dates"] = 0
        report["future_dates"] = 0
        report["dates_removed"] = 0

    # Category
    if "category" in df.columns:
        missing_cat = int(df["category"].isnull().sum())
        df["category"] = df["category"].fillna("unknown")
        report["missing_categories"] = missing_cat
    else:
        report["missing_categories"] = 0

    # Suspicious flag
    if "amount" in df.columns:
        suspicious_mask = df["amount"] > 10000
        report["suspicious_high_amounts"] = int(suspicious_mask.sum())
        df["suspicious"] = suspicious_mask
    else:
        report["suspicious_high_amounts"] = 0
        df["suspicious"] = False

    report["final_count"] = len(df)
    logger.info("clean_transaction_data report=%s", report)
    return df.reset_index(drop=True), report
