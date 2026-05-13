"""
Tests for the ML synthetic session data generation logic.

Validates the data generation matches the notebook's
generate_synthetic_sessions() function at:
  databricks/notebooks/02_ml_purchase_propensity.py

Runs without PySpark — validates data shapes, distributions,
semantic invariants, and feature/target relationships.
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

import pytest

DEVICE_TYPES = ["desktop", "mobile", "tablet"]
CATEGORIES = ["Electronics", "Clothing", "Footwear", "Home & Garden", "Sports", "Accessories"]


def generate_synthetic_sessions(n_sessions=100, seed=42):
    """Exact copy of the function from 02_ml_purchase_propensity.py."""
    rng = np.random.default_rng(seed)
    n = n_sessions

    page_views = rng.poisson(8, n).clip(1, 60)
    unique_products = np.minimum(
        np.minimum(rng.poisson(page_views.astype(float) * 0.55), 20), page_views
    ).clip(1)
    add_to_cart = rng.poisson(page_views.astype(float) * 0.18).clip(0, 12)
    search_events = rng.poisson(1.8, n).clip(0, 10)
    session_duration = rng.lognormal(mean=4.2, sigma=0.7, size=n).clip(5, 5400)

    device_types = rng.choice(DEVICE_TYPES, n, p=[0.50, 0.38, 0.12])

    base_dt = datetime(2025, 6, 1)
    session_starts = [base_dt + timedelta(
        days=int(rng.integers(0, 30)),
        hours=int(rng.integers(0, 24)),
        minutes=int(rng.integers(0, 60))
    ) for _ in range(n)]

    prob = 0.025
    prob += add_to_cart * 0.09
    prob += np.log1p(page_views) * 0.018
    prob += np.log1p(session_duration) * 0.010
    prob += search_events * 0.012
    prob *= np.where(device_types == "mobile", 0.75, 1.0)
    prob *= np.where(device_types == "tablet", 0.85, 1.0)
    prob = np.clip(prob, 0.01, 0.90)
    converted = rng.binomial(1, prob)

    rows = []
    for i in range(n):
        n_cats = rng.integers(1, 4)
        sess_cats = list(rng.choice(CATEGORIES, n_cats, replace=False))
        n_devs = rng.integers(1, 3)
        sess_devs = list(rng.choice(DEVICE_TYPES, n_devs, replace=False))
        dt = session_starts[i]
        rows.append({
            "session_id": f"sess_{i:06d}",
            "user_id": f"user_{rng.integers(10000, 99999)}",
            "event_date": dt.strftime("%Y-%m-%d"),
            "event_hour": dt.hour,
            "day_of_week": dt.weekday(),
            "page_views": int(page_views[i]),
            "unique_products_viewed": int(unique_products[i]),
            "add_to_cart_events": int(add_to_cart[i]),
            "search_events": int(search_events[i]),
            "session_duration_seconds": float(round(session_duration[i], 1)),
            "device_type": device_types[i],
            "devices_used": sess_devs,
            "categories_viewed": sess_cats,
            "n_categories_viewed": len(sess_cats),
            "converted_to_purchase": bool(converted[i]),
        })

    return pd.DataFrame(rows)


class TestDataGeneration:
    """Core data generation invariants."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.df = generate_synthetic_sessions(n_sessions=500, seed=42)

    def test_row_count(self):
        assert len(self.df) == 500

    def test_required_columns(self):
        required = [
            "session_id", "user_id", "event_date", "event_hour",
            "page_views", "unique_products_viewed", "add_to_cart_events",
            "search_events", "session_duration_seconds", "device_type",
            "devices_used", "categories_viewed", "converted_to_purchase",
        ]
        for col in required:
            assert col in self.df.columns, f"Missing column: {col}"

    def test_no_null_critical(self):
        critical = [
            "session_id", "user_id", "page_views",
            "add_to_cart_events", "converted_to_purchase",
        ]
        assert self.df[critical].isnull().sum().sum() == 0

    def test_conversion_rate_in_range(self):
        rate = self.df["converted_to_purchase"].mean()
        assert 0.08 < rate < 0.30, (
            f"Conversion rate {rate:.2%} outside expected range"
        )

    def test_unique_products_never_exceeds_page_views(self):
        ok = (self.df["unique_products_viewed"] <= self.df["page_views"]).all()
        assert ok, "unique_products_viewed exceeds page_views"

    def test_page_views_positive(self):
        assert (self.df["page_views"] >= 1).all()

    def test_session_duration_positive(self):
        assert (self.df["session_duration_seconds"] > 0).all()

    def test_device_distribution(self):
        counts = self.df["device_type"].value_counts(normalize=True)
        assert counts.get("desktop", 0) > 0.30
        assert counts.get("mobile", 0) > 0.20
        assert counts.get("tablet", 0) > 0.03

    def test_hour_in_valid_range(self):
        assert self.df["event_hour"].between(0, 23).all()

    def test_day_of_week_in_valid_range(self):
        assert self.df["day_of_week"].between(0, 6).all()

    def test_categories_viewed_nonempty(self):
        assert (self.df["categories_viewed"].apply(len) >= 1).all()

    def test_devices_used_nonempty(self):
        assert (self.df["devices_used"].apply(len) >= 1).all()


class TestFeatureTargetRelationship:
    """The synthetic data must show realistic feature-target correlations."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.df = generate_synthetic_sessions(n_sessions=2000, seed=42)

    def test_add_to_cart_correlates_with_conversion(self):
        """Sessions with add-to-cart should have higher conversion."""
        with_cart = self.df[self.df["add_to_cart_events"] > 0]
        without_cart = self.df[self.df["add_to_cart_events"] == 0]
        rate_with = with_cart["converted_to_purchase"].mean()
        rate_without = without_cart["converted_to_purchase"].mean()
        assert rate_with > rate_without, (
            f"add_to_cart rate {rate_with:.2%} <= baseline {rate_without:.2%}"
        )

    def test_mobile_converts_lower_than_desktop(self):
        mobile = self.df[self.df["device_type"] == "mobile"]
        desktop = self.df[self.df["device_type"] == "desktop"]
        rate_m = mobile["converted_to_purchase"].mean()
        rate_d = desktop["converted_to_purchase"].mean()
        assert rate_m < rate_d, (
            f"Mobile {rate_m:.2%} not < desktop {rate_d:.2%}"
        )

    def test_higher_page_views_correlates_with_conversion(self):
        median = self.df["page_views"].median()
        high = self.df[self.df["page_views"] > median]
        low = self.df[self.df["page_views"] <= median]
        assert high["converted_to_purchase"].mean() > low["converted_to_purchase"].mean()

    def test_longer_sessions_correlate_with_conversion(self):
        median = self.df["session_duration_seconds"].median()
        long_sess = self.df[self.df["session_duration_seconds"] > median]
        short_sess = self.df[self.df["session_duration_seconds"] <= median]
        assert (
            long_sess["converted_to_purchase"].mean()
            > short_sess["converted_to_purchase"].mean()
        )


class TestReproducibility:
    """Same seed must produce identical data."""

    def test_same_seed_same_output(self):
        df1 = generate_synthetic_sessions(n_sessions=100, seed=123)
        df2 = generate_synthetic_sessions(n_sessions=100, seed=123)
        pd.testing.assert_frame_equal(df1, df2)

    def test_different_seed_different_output(self):
        df1 = generate_synthetic_sessions(n_sessions=100, seed=1)
        df2 = generate_synthetic_sessions(n_sessions=100, seed=2)
        assert not df1.equals(df2)
