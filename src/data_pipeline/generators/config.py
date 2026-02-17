"""
Configuration constants for synthetic data generation.

Defines MCC code mappings, spending archetypes, card portfolios,
and temporal patterns used by the generators.
"""

from dataclasses import dataclass
from typing import Dict, List, Tuple

# ---------------------------------------------------------------------------
# MCC (Merchant Category Code) mapping
# Maps standardized spending categories to representative MCC ranges.
# Reference: https://www.citibank.com/tts/solutions/commercial-cards/assets/docs/govt/Merchant-Category-Codes.pdf
# ---------------------------------------------------------------------------
SPENDING_CATEGORIES: Dict[str, Dict] = {
    "groceries": {
        "mcc_codes": [5411, 5422, 5441, 5451, 5462],
        "merchants": [
            "Whole Foods",
            "Trader Joe's",
            "Kroger",
            "Safeway",
            "Costco",
            "Aldi",
            "Publix",
            "H-E-B",
            "Wegmans",
        ],
    },
    "dining": {
        "mcc_codes": [5812, 5813, 5814],
        "merchants": [
            "Chipotle",
            "Starbucks",
            "McDonald's",
            "Olive Garden",
            "Chili's",
            "Panera Bread",
            "Subway",
            "Local Restaurant",
            "DoorDash",
            "Uber Eats",
        ],
    },
    "gas": {
        "mcc_codes": [5541, 5542],
        "merchants": [
            "Shell",
            "Chevron",
            "ExxonMobil",
            "BP",
            "Costco Gas",
            "Sam's Club Gas",
            "Speedway",
        ],
    },
    "travel": {
        "mcc_codes": [3000, 3001, 4511, 4722, 7011, 7512],
        "merchants": [
            "Delta Airlines",
            "United Airlines",
            "Southwest Airlines",
            "Marriott",
            "Hilton",
            "Airbnb",
            "Expedia",
            "Hertz",
            "Enterprise",
            "Booking.com",
        ],
    },
    "online_shopping": {
        "mcc_codes": [5691, 5699, 5944, 5945, 5947],
        "merchants": [
            "Amazon",
            "Target.com",
            "Walmart.com",
            "Best Buy",
            "Nike.com",
            "Etsy",
            "Wayfair",
        ],
    },
    "streaming": {
        "mcc_codes": [4899, 5815, 5816, 5818],
        "merchants": [
            "Netflix",
            "Spotify",
            "Hulu",
            "Disney+",
            "YouTube Premium",
            "Apple Music",
            "HBO Max",
        ],
    },
    "utilities": {
        "mcc_codes": [4900, 4814, 4816],
        "merchants": [
            "Electric Company",
            "Water Utility",
            "Gas Utility",
            "Internet Provider",
            "Phone Bill",
        ],
    },
    "insurance": {
        "mcc_codes": [6300, 6381, 5960],
        "merchants": [
            "State Farm",
            "Geico",
            "Progressive",
            "Allstate",
            "Health Insurance Premium",
        ],
    },
    "entertainment": {
        "mcc_codes": [7832, 7922, 7941, 7991, 7999],
        "merchants": [
            "AMC Theatres",
            "Regal Cinemas",
            "Ticketmaster",
            "Live Nation",
            "Gym Membership",
            "Bowling Alley",
        ],
    },
    "drugstore": {
        "mcc_codes": [5912],
        "merchants": [
            "CVS",
            "Walgreens",
            "Rite Aid",
            "CVS Pharmacy",
        ],
    },
    "home_improvement": {
        "mcc_codes": [5200, 5211, 5231, 5251, 5261],
        "merchants": [
            "Home Depot",
            "Lowe's",
            "Ace Hardware",
            "Menards",
            "True Value",
        ],
    },
    "transit": {
        "mcc_codes": [4111, 4112, 4121, 4131],
        "merchants": [
            "Uber",
            "Lyft",
            "Metro Transit",
            "Amtrak",
            "Greyhound",
            "Taxi",
        ],
    },
    "other": {
        "mcc_codes": [5999],
        "merchants": [
            "Miscellaneous Store",
            "Local Shop",
            "Specialty Retailer",
            "General Merchandise",
        ],
    },
}

# ---------------------------------------------------------------------------
# User spending archetypes
# Each archetype defines a spending distribution (weights per category)
# and monthly budget range. These drive realistic diversity in the data.
# ---------------------------------------------------------------------------


@dataclass
class SpendingArchetype:
    """Defines a user spending persona with category weights and budget."""

    name: str
    description: str
    monthly_budget_range: Tuple[float, float]
    category_weights: Dict[str, float]


SPENDING_ARCHETYPES: List[SpendingArchetype] = [
    SpendingArchetype(
        name="young_professional",
        description="Urban renter, heavy on dining and transit, moderate online shopping",
        monthly_budget_range=(2500.0, 4500.0),
        category_weights={
            "groceries": 0.12,
            "dining": 0.20,
            "gas": 0.02,
            "travel": 0.08,
            "online_shopping": 0.15,
            "streaming": 0.04,
            "utilities": 0.08,
            "insurance": 0.05,
            "entertainment": 0.10,
            "drugstore": 0.03,
            "home_improvement": 0.01,
            "transit": 0.08,
            "other": 0.04,
        },
    ),
    SpendingArchetype(
        name="suburban_family",
        description="Homeowner with kids, heavy groceries, gas, and home improvement",
        monthly_budget_range=(5000.0, 9000.0),
        category_weights={
            "groceries": 0.22,
            "dining": 0.10,
            "gas": 0.10,
            "travel": 0.05,
            "online_shopping": 0.12,
            "streaming": 0.03,
            "utilities": 0.10,
            "insurance": 0.08,
            "entertainment": 0.06,
            "drugstore": 0.04,
            "home_improvement": 0.06,
            "transit": 0.01,
            "other": 0.03,
        },
    ),
    SpendingArchetype(
        name="frequent_traveler",
        description="Business or leisure traveler, heavy travel and dining spend",
        monthly_budget_range=(4000.0, 8000.0),
        category_weights={
            "groceries": 0.08,
            "dining": 0.18,
            "gas": 0.03,
            "travel": 0.25,
            "online_shopping": 0.10,
            "streaming": 0.03,
            "utilities": 0.06,
            "insurance": 0.05,
            "entertainment": 0.08,
            "drugstore": 0.02,
            "home_improvement": 0.01,
            "transit": 0.08,
            "other": 0.03,
        },
    ),
    SpendingArchetype(
        name="budget_conscious",
        description="Frugal spender, essentials-heavy, low discretionary",
        monthly_budget_range=(1500.0, 3000.0),
        category_weights={
            "groceries": 0.25,
            "dining": 0.05,
            "gas": 0.08,
            "travel": 0.02,
            "online_shopping": 0.08,
            "streaming": 0.03,
            "utilities": 0.15,
            "insurance": 0.10,
            "entertainment": 0.03,
            "drugstore": 0.06,
            "home_improvement": 0.02,
            "transit": 0.05,
            "other": 0.08,
        },
    ),
    SpendingArchetype(
        name="high_roller",
        description="High income, large discretionary spend across all categories",
        monthly_budget_range=(8000.0, 15000.0),
        category_weights={
            "groceries": 0.10,
            "dining": 0.15,
            "gas": 0.04,
            "travel": 0.18,
            "online_shopping": 0.14,
            "streaming": 0.02,
            "utilities": 0.05,
            "insurance": 0.06,
            "entertainment": 0.12,
            "drugstore": 0.02,
            "home_improvement": 0.05,
            "transit": 0.04,
            "other": 0.03,
        },
    ),
    SpendingArchetype(
        name="minimal_user",
        description="Rarely uses cards, very low transaction volume",
        monthly_budget_range=(300.0, 800.0),
        category_weights={
            "groceries": 0.20,
            "dining": 0.10,
            "gas": 0.10,
            "travel": 0.00,
            "online_shopping": 0.15,
            "streaming": 0.05,
            "utilities": 0.15,
            "insurance": 0.05,
            "entertainment": 0.05,
            "drugstore": 0.05,
            "home_improvement": 0.00,
            "transit": 0.05,
            "other": 0.05,
        },
    ),
    SpendingArchetype(
        name="category_specialist",
        description="Concentrated spend in 1-2 categories (e.g., heavy grocery shopper)",
        monthly_budget_range=(2000.0, 5000.0),
        category_weights={
            "groceries": 0.40,
            "dining": 0.05,
            "gas": 0.05,
            "travel": 0.02,
            "online_shopping": 0.10,
            "streaming": 0.03,
            "utilities": 0.10,
            "insurance": 0.05,
            "entertainment": 0.05,
            "drugstore": 0.05,
            "home_improvement": 0.03,
            "transit": 0.02,
            "other": 0.05,
        },
    ),
]

# Archetype distribution: controls how many users fall into each archetype.
# Weights are relative (don't need to sum to 1; they get normalized).
ARCHETYPE_DISTRIBUTION: Dict[str, float] = {
    "young_professional": 0.25,
    "suburban_family": 0.20,
    "frequent_traveler": 0.10,
    "budget_conscious": 0.20,
    "high_roller": 0.05,
    "minimal_user": 0.10,
    "category_specialist": 0.10,
}

# ---------------------------------------------------------------------------
# Credit card portfolio templates
# Maps card names to the categories they're best for.
# These mirror real-world popular cards (anonymized where appropriate).
# ---------------------------------------------------------------------------
CARD_PORTFOLIO_TEMPLATES: Dict[str, List[str]] = {
    "starter_cashback": [
        "Citi Double Cash",
        "Chase Freedom Flex",
    ],
    "dining_focused": [
        "Amex Gold",
        "Chase Sapphire Preferred",
        "Capital One SavorOne",
    ],
    "travel_focused": [
        "Chase Sapphire Reserve",
        "Amex Platinum",
        "Capital One Venture X",
    ],
    "grocery_focused": [
        "Amex Blue Cash Preferred",
        "Amex Gold",
    ],
    "all_rounder": [
        "Chase Freedom Unlimited",
        "Citi Double Cash",
        "Amex Blue Cash Everyday",
    ],
    "premium_stack": [
        "Chase Sapphire Reserve",
        "Amex Gold",
        "Chase Freedom Flex",
        "Amex Blue Cash Preferred",
    ],
}

# ---------------------------------------------------------------------------
# Redemption preferences
# ---------------------------------------------------------------------------
REDEMPTION_PREFERENCES: List[str] = [
    "cash_back",
    "travel_transfer",
    "statement_credit",
    "gift_cards",
    "merchandise",
    "travel_portal",
]

REDEMPTION_PREFERENCE_WEIGHTS: Dict[str, float] = {
    "cash_back": 0.35,
    "travel_transfer": 0.20,
    "statement_credit": 0.20,
    "gift_cards": 0.10,
    "merchandise": 0.05,
    "travel_portal": 0.10,
}

# ---------------------------------------------------------------------------
# Temporal patterns: seasonal spending multipliers by month (1-indexed)
# ---------------------------------------------------------------------------
SEASONAL_MULTIPLIERS: Dict[str, Dict[int, float]] = {
    "groceries": {11: 1.3, 12: 1.4, 1: 0.9},  # holiday cooking
    "travel": {6: 1.5, 7: 1.6, 8: 1.4, 12: 1.3},  # summer + holidays
    "online_shopping": {11: 1.8, 12: 2.0, 1: 0.7},  # Black Friday + holiday
    "dining": {2: 1.2, 5: 1.1, 12: 1.3},  # Valentine's, Mother's Day, holidays
    "entertainment": {6: 1.2, 7: 1.3, 12: 1.2},  # summer + holidays
    "home_improvement": {3: 1.2, 4: 1.4, 5: 1.3, 9: 1.2},  # spring + fall
}

# Default multiplier when no seasonal adjustment applies
DEFAULT_SEASONAL_MULTIPLIER: float = 1.0

# ---------------------------------------------------------------------------
# Transaction amount distributions per category (mean, std)
# Used to generate realistic individual transaction amounts.
# ---------------------------------------------------------------------------
TRANSACTION_AMOUNT_PARAMS: Dict[str, Tuple[float, float]] = {
    "groceries": (65.0, 30.0),
    "dining": (28.0, 18.0),
    "gas": (42.0, 15.0),
    "travel": (350.0, 250.0),
    "online_shopping": (55.0, 45.0),
    "streaming": (14.0, 4.0),
    "utilities": (120.0, 50.0),
    "insurance": (180.0, 80.0),
    "entertainment": (35.0, 25.0),
    "drugstore": (22.0, 15.0),
    "home_improvement": (85.0, 60.0),
    "transit": (18.0, 12.0),
    "other": (40.0, 30.0),
}

# Minimum transaction amount (floor)
MIN_TRANSACTION_AMOUNT: float = 1.50

# ---------------------------------------------------------------------------
# Generation defaults
# ---------------------------------------------------------------------------
DEFAULT_NUM_USERS: int = 100
DEFAULT_HISTORY_MONTHS: int = 14  # > 12 months per acceptance criteria
DEFAULT_SEED: int = 42
