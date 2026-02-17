# RewardSense Data Card

> **Version:** 1.0  

---

## Overview

This document describes all data sources, schemas, and quality expectations for the RewardSense credit card recommendation system. It serves as the single source of truth for data contracts between the ingestion (Epic 2), cleaning (Epic 3), and ML pipeline (Epic 6) stages.

---

## Table of Contents

1. [Data Sources](#data-sources)
2. [Unified Schema](#unified-schema)
3. [Source-Specific Schemas](#source-specific-schemas)
4. [Field Mappings](#field-mappings)
5. [Data Quality & Validation](#data-quality--validation)
6. [Dataset Statistics](#dataset-statistics)
7. [Known Limitations](#known-limitations)
8. [Changelog](#changelog)

---

## Data Sources

### Primary Source: CreditCardBonuses API

| Property | Value |
|----------|-------|
| **Type** | REST API |
| **Status** | ✅ Active |
| **Cards Available** | ~172 |
| **Update Frequency** | Daily |
| **Authentication** | None required |
| **Rate Limits** | Unknown (use 1 req/sec to be safe) |

**Endpoint:** Internal API (see team documentation)

**Data Quality:** ⭐⭐⭐⭐⭐ Excellent
- Structured JSON responses
- Normalized issuer names
- Historical offer tracking
- Comprehensive reward details

---

### Secondary Sources: Web Scrapers

#### Chase (creditcards.chase.com)

| Property | Value |
|----------|-------|
| **Type** | Web Scraper (BeautifulSoup) |
| **Status** | ✅ Working |
| **URL** | `https://creditcards.chase.com/all-credit-cards` |
| **Cards Available** | ~15-20 |
| **Key Selector** | `div.cmp-cardsummary__inner-container` |

**Data Quality:** ⭐⭐⭐⭐ Good
- Accurate card names and annual fees
- Welcome bonuses captured
- Reward rates may include marketing copy fragments

---

#### Discover (discover.com)

| Property | Value |
|----------|-------|
| **Type** | Web Scraper (BeautifulSoup) |
| **Status** | ✅ Working |
| **URL** | `https://www.discover.com/credit-cards/` |
| **Cards Available** | ~5-8 |
| **Key Selector** | `div` with class containing "card" |

**Data Quality:** ⭐⭐⭐⭐ Good
- Clean card names
- Annual fees accurate (mostly $0)
- Limited reward rate details

---

#### NerdWallet (nerdwallet.com)

| Property | Value |
|----------|-------|
| **Type** | Web Scraper (BeautifulSoup) |
| **Status** | ⚠️ Partial |
| **URLs** | `https://www.nerdwallet.com/credit-cards` |
| **Cards Available** | Variable |
| **Key Selector** | JSON-LD + HTML fallback |

**Data Quality:** ⭐⭐ Fair
- May capture category labels instead of card names
- Issuer often null
- Useful for welcome bonus and annual fee data

**Known Issues:**
- Site structure changed (URLs updated 2026-02)
- Some pages require JavaScript rendering

---

#### American Express (americanexpress.com)

| Property | Value |
|----------|-------|
| **Type** | Web Scraper (Selenium required) |
| **Status** | TODO |
| **URL** | `https://www.americanexpress.com/us/credit-cards/` |
| **Cards Available** | ~20-25 |

**Notes:** JavaScript-rendered content. Requires Selenium implementation.

---

#### Citi (citi.com)

| Property | Value |
|----------|-------|
| **Type** | Web Scraper (Selenium required) |
| **Status** | TODO |
| **URL** | `https://www.citi.com/credit-cards/compare-credit-cards` |
| **Cards Available** | ~15-20 |

**Notes:** JavaScript-rendered content. May require cookie consent handling.

---

#### Capital One (capitalone.com)

| Property | Value |
|----------|-------|
| **Type** | Web Scraper (Selenium required) |
| **Status** | TODO |
| **URL** | `https://www.capitalone.com/credit-cards/` |
| **Cards Available** | ~15-20 |

**Notes:** JavaScript-rendered. Potential bot detection. Consider `undetected-chromedriver`.

---

## Unified Schema

This is the **target schema** that all sources are transformed into during Epic 3.

```yaml
CreditCard:
  # === Identity ===
  card_id: string           # Unique identifier (hash of name + issuer)
  card_name: string         # Official card name (required)
  issuer: string            # Normalized issuer name (required)
  network: string           # VISA, MASTERCARD, AMERICAN_EXPRESS, DISCOVER
  
  # === Fees ===
  annual_fee: float         # In USD, 0 if no fee
  annual_fee_waived_first_year: boolean
  foreign_transaction_fee: float  # Percentage, null if unknown
  
  # === Welcome Bonus ===
  welcome_bonus_amount: integer    # Points/miles/dollars
  welcome_bonus_currency: string   # POINTS, MILES, USD
  welcome_bonus_spend_requirement: float  # USD spend required
  welcome_bonus_time_limit_days: integer  # Days to meet spend
  welcome_bonus_value_usd: float   # Estimated USD value
  
  # === Rewards Structure ===
  base_reward_rate: float          # Universal earn rate (e.g., 1.0 = 1%)
  base_reward_currency: string     # POINTS, MILES, CASHBACK
  reward_categories: list[RewardCategory]
  
  # === Card Metadata ===
  is_business: boolean
  credit_score_required: string    # EXCELLENT, GOOD, FAIR, etc.
  card_url: string                 # Link to issuer's card page
  image_url: string                # Card image URL
  
  # === Tracking ===
  source: string                   # Data source identifier
  scraped_at: datetime             # When data was collected
  is_discontinued: boolean

RewardCategory:
  category_name: string      # e.g., "dining", "travel", "groceries"
  reward_rate: float         # e.g., 3.0 for 3x or 3%
  reward_type: string        # POINTS, MILES, CASHBACK, PERCENT
  annual_cap: float          # Max spend for bonus rate, null if unlimited
  details: string            # Additional terms/conditions
```

---

## Source-Specific Schemas

### CreditCardBonuses API Response

```json
{
  "source": "creditcardbonuses",
  "card_name": "string",
  "issuer": "string (AMERICAN_EXPRESS, CHASE, etc.)",
  "annual_fee": "float",
  "welcome_bonus": "string (human-readable)",
  "reward_rates": {
    "universal_base_rate": "float"
  },
  "categories": ["string"],
  "offer_url": "string",
  "card_id": "string (hash)",
  "network": "string",
  "currency": "string (DELTA, UNITED, CASHBACK, etc.)",
  "is_business": "boolean",
  "is_annual_fee_waived": "boolean",
  "universal_cashback_percent": "float",
  "image_url": "string",
  "discontinued": "boolean",
  "offers": [
    {
      "spend": "integer",
      "amount": [{"amount": "integer"}],
      "days": "integer",
      "credits": []
    }
  ],
  "historical_offers": ["...same as offers"]
}
```

---

### Chase Scraper Output

```json
{
  "source": "Chase",
  "issuer": "Chase",
  "scraped_at": "ISO 8601 datetime",
  "name": "string",
  "annual_fee": "integer",
  "welcome_bonus": "string (e.g., '75,000 points')",
  "bonus_value_usd": "float",
  "reward_rates": {
    "category_name": {
      "rate": "float",
      "type": "string (points/cashback)"
    }
  },
  "detail_url": "string",
  "image_url": "string"
}
```

---

### Discover Scraper Output

```json
{
  "source": "Discover",
  "issuer": "Discover",
  "scraped_at": "ISO 8601 datetime",
  "name": "string",
  "annual_fee": "integer",
  "reward_rates": {
    "category": "string (e.g., '5%')"
  },
  "detail_url": "string"
}
```

---

### NerdWallet Scraper Output

```json
{
  "source": "NerdWallet",
  "scraped_at": "ISO 8601 datetime",
  "name": "string",
  "issuer": "string | null",
  "detail_url": "string",
  "annual_fee": "integer",
  "base_reward_rate": "string (e.g., '1.5%')",
  "welcome_bonus": "string (e.g., '$200')"
}
```

---

## Field Mappings

### Issuer Normalization

| Raw Values | Canonical Value |
|------------|-----------------|
| `AMERICAN_EXPRESS`, `Amex`, `American Express`, `amex` | `American Express` |
| `CHASE`, `Chase`, `chase` | `Chase` |
| `CITI`, `Citi`, `Citibank`, `citi` | `Citi` |
| `CAPITAL_ONE`, `Capital One`, `CapitalOne` | `Capital One` |
| `DISCOVER`, `Discover` | `Discover` |
| `BANK_OF_AMERICA`, `Bank of America`, `BofA` | `Bank of America` |
| `WELLS_FARGO`, `Wells Fargo` | `Wells Fargo` |
| `BARCLAYS`, `Barclays` | `Barclays` |
| `US_BANK`, `U.S. Bank`, `US Bank` | `U.S. Bank` |

---

### Network Normalization

| Raw Values | Canonical Value |
|------------|-----------------|
| `VISA`, `Visa`, `visa` | `Visa` |
| `MASTERCARD`, `Mastercard`, `MC` | `Mastercard` |
| `AMERICAN_EXPRESS`, `Amex` | `American Express` |
| `DISCOVER`, `Discover` | `Discover` |

---

### Reward Currency Normalization

| Raw Values | Canonical Value |
|------------|-----------------|
| `CASHBACK`, `cash back`, `Cash Back`, `%` | `Cashback` |
| `POINTS`, `points`, `pts`, `Ultimate Rewards` | `Points` |
| `MILES`, `miles`, `SkyMiles`, `AAdvantage` | `Miles` |
| `DELTA`, `Delta SkyMiles` | `Delta Miles` |
| `UNITED`, `United MileagePlus` | `United Miles` |

---

### Category Normalization

| Raw Values | Canonical Value |
|------------|-----------------|
| `dining`, `restaurants`, `restaurant`, `Dining` | `dining` |
| `travel`, `Travel`, `travel purchases` | `travel` |
| `groceries`, `grocery`, `supermarkets`, `Groceries` | `groceries` |
| `gas`, `gas stations`, `Gas`, `fuel` | `gas` |
| `streaming`, `streaming services`, `Streaming` | `streaming` |
| `online shopping`, `online`, `Online Shopping` | `online_shopping` |
| `drugstores`, `pharmacy`, `Drugstores` | `drugstores` |
| `all purchases`, `everything else`, `other` | `base` |

---

## Data Quality & Validation

### Required Fields

Every record **must** have:

| Field | Validation Rule |
|-------|-----------------|
| `card_name` | Non-empty string, 3-100 characters |
| `issuer` | Must match known issuer list |
| `source` | Non-empty string |
| `scraped_at` | Valid ISO 8601 datetime |

### Numeric Validations

| Field | Rule |
|-------|------|
| `annual_fee` | >= 0, <= 1000 |
| `welcome_bonus_amount` | >= 0, <= 500000 |
| `welcome_bonus_spend_requirement` | >= 0, <= 50000 |
| `welcome_bonus_time_limit_days` | >= 0, <= 365 |
| `base_reward_rate` | >= 0, <= 10 |
| `reward_rate` (in categories) | >= 0, <= 25 |

### String Validations

| Field | Rule |
|-------|------|
| `card_name` | Must not be a category label (e.g., "Best for: cash back") |
| `issuer` | Must be in canonical issuer list after normalization |
| `network` | Must be Visa, Mastercard, American Express, or Discover |

### Deduplication Rules

Cards are considered duplicates if they match on:
1. **Exact match:** `card_name` + `issuer` (case-insensitive)
2. **Fuzzy match:** Levenshtein distance < 3 on normalized names + same issuer

**Priority order when merging duplicates:**
1. CreditCardBonuses API (most complete)
2. Issuer scrapers (Chase, Discover)
3. Aggregator scrapers (NerdWallet)

---

## Dataset Statistics

### Expected Volumes

| Source | Min Cards | Max Cards | Typical |
|--------|-----------|-----------|---------|
| CreditCardBonuses API | 150 | 200 | 172 |
| Chase Scraper | 10 | 25 | 15 |
| Discover Scraper | 3 | 10 | 6 |
| NerdWallet Scraper | 0 | 100 | 50 |
| **Total (deduplicated)** | **150** | **250** | **~180** |

### Data Completeness by Source

| Field | API | Chase | Discover | NerdWallet |
|-------|-----|-------|----------|------------|
| card_name | ✅ 100% | ✅ 100% | ✅ 100% | ⚠️ 80% |
| issuer | ✅ 100% | ✅ 100% | ✅ 100% | ⚠️ 60% |
| annual_fee | ✅ 100% | ✅ 100% | ✅ 100% | ✅ 95% |
| welcome_bonus | ✅ 95% | ✅ 90% | ❌ 10% | ⚠️ 70% |
| reward_rates | ✅ 90% | ⚠️ 70% | ⚠️ 50% | ⚠️ 40% |
| network | ✅ 100% | ❌ 0% | ❌ 0% | ❌ 0% |
| image_url | ✅ 100% | ✅ 80% | ❌ 0% | ❌ 0% |

---

## Known Limitations

### Scraper Limitations

1. **NerdWallet:** May capture category labels as card names due to ambiguous HTML structure
2. **Chase:** `reward_rates` extraction sometimes captures marketing copy fragments
3. **Amex/Citi/CapitalOne:** Not implemented (require Selenium)

### Data Freshness

- API data: Updated daily
- Scraped data: Updated weekly (Sunday 2 AM per `scraper_config.yaml`)
- Credit card terms can change without notice

### Coverage Gaps

- Credit union cards not included
- Store-branded cards (Target, Amazon) partially covered
- International cards not included


---

## Appendix: Source URLs

### Scraper Target URLs

```yaml
chase:
  - https://creditcards.chase.com/all-credit-cards

discover:
  - https://www.discover.com/credit-cards/

nerdwallet:
  - https://www.nerdwallet.com/credit-cards
  - https://www.nerdwallet.com/credit-cards/best
  - https://www.nerdwallet.com/credit-cards/cash-back
  - https://www.nerdwallet.com/credit-cards/travel
  - https://www.nerdwallet.com/credit-cards/balance-transfer
  - https://www.nerdwallet.com/credit-cards/business
  - https://www.nerdwallet.com/credit-cards/rewards
  - https://www.nerdwallet.com/credit-cards/no-annual-fee

american_express:  # TODO: Selenium
  - https://www.americanexpress.com/us/credit-cards/

citi:  # TODO: Selenium
  - https://www.citi.com/credit-cards/compare-credit-cards

capital_one:  # TODO: Selenium
  - https://www.capitalone.com/credit-cards/
```

---

## References

- [Scraper Configuration](/src/data_pipeline/scrapers/scraper_config.yaml)
- [Impelmentation Doc of the team - available on request]