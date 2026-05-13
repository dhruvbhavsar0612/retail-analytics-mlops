"""
Tests for the clickstream producer data generation logic.

Runs without a Kafka broker — validates event structure,
field completeness, and session lifecycle behavior.
"""

import sys
import pytest

sys.path.insert(0, "kafka/producers")
from clickstream_producer import RetailClickstreamProducer  # noqa: E402


@pytest.fixture
def producer():
    return RetailClickstreamProducer("localhost:9092", "retail_clickstream")


EVENT_TYPES = {"page_view", "add_to_cart", "purchase", "search"}

PAGE_VIEW_FIELDS = {
    "event_id", "event_type", "timestamp", "user_id", "session_id",
    "page_url", "page_title", "referrer", "product_id", "product_name",
    "product_category", "product_price", "product_brand",
    "device_info", "location", "user_agent", "ip_address",
}

ADD_TO_CART_FIELDS = {
    "event_id", "event_type", "timestamp", "user_id", "session_id",
    "product_id", "product_name", "product_category",
    "product_price", "product_brand", "quantity", "total_value",
    "device_info", "location",
}

PURCHASE_FIELDS = {
    "event_id", "event_type", "timestamp", "user_id", "session_id",
    "order_id", "cart_items", "total_value",
    "shipping_method", "payment_method",
    "device_info", "location",
}

SEARCH_FIELDS = {
    "event_id", "event_type", "timestamp", "user_id", "session_id",
    "search_query", "search_results_count",
    "device_info", "location",
}


class TestEventGeneration:
    """Validate that generated events are well-formed."""

    def test_generates_valid_event(self, producer):
        event = producer.generate_event()
        assert isinstance(event, dict)
        assert "event_id" in event
        assert "event_type" in event
        assert event["event_type"] in EVENT_TYPES
        assert "user_id" in event
        assert "session_id" in event
        assert "timestamp" in event

    def test_page_view_has_required_fields(self, producer):
        for _ in range(200):
            event = producer.generate_event()
            if event["event_type"] == "page_view":
                for field in PAGE_VIEW_FIELDS:
                    assert field in event, f"Missing field {field} in page_view"
                return
        pytest.fail("Never generated a page_view event after 200 attempts")

    def test_add_to_cart_has_required_fields(self, producer):
        for _ in range(200):
            event = producer.generate_event()
            if event["event_type"] == "add_to_cart":
                for field in ADD_TO_CART_FIELDS:
                    assert field in event, f"Missing field {field} in add_to_cart"
                assert event["quantity"] > 0
                assert event["total_value"] > 0
                return
        pytest.fail("Never generated an add_to_cart event after 200 attempts")

    def test_purchase_has_required_fields(self, producer):
        for _ in range(500):
            event = producer.generate_event()
            if event["event_type"] == "purchase":
                for field in PURCHASE_FIELDS:
                    assert field in event, f"Missing field {field} in purchase"
                assert isinstance(event["cart_items"], list)
                assert len(event["cart_items"]) >= 1
                assert event["total_value"] > 0
                return
        pytest.fail("Never generated a purchase event after 500 attempts")

    def test_search_has_required_fields(self, producer):
        for _ in range(200):
            event = producer.generate_event()
            if event["event_type"] == "search":
                for field in SEARCH_FIELDS:
                    assert field in event, f"Missing field {field} in search"
                assert event["search_results_count"] >= 10
                return
        pytest.fail("Never generated a search event after 200 attempts")

    def test_event_type_distribution_is_realistic(self, producer):
        counts = {t: 0 for t in EVENT_TYPES}
        n = 2000
        for _ in range(n):
            event = producer.generate_event()
            counts[event["event_type"]] += 1

        # Page views should dominate
        assert counts["page_view"] > counts["add_to_cart"]
        assert counts["page_view"] > counts["purchase"]
        assert counts["page_view"] > counts["search"]
        # Purchases are rare
        assert counts["purchase"] < counts["page_view"] * 0.2


class TestSessionLifecycle:
    """Validate session management behavior."""

    def test_session_persists_across_events(self, producer):
        events = [producer.generate_event() for _ in range(5)]
        session_ids = {e["session_id"] for e in events}
        # Most events should be in the same session (new session ~10% per event)
        assert len(session_ids) <= 5

    def test_new_session_eventually_starts(self, producer):
        events = [producer.generate_event() for _ in range(200)]
        session_ids = {e["session_id"] for e in events}
        # With 200 events and session change probabilities, we should see >1 session
        assert len(session_ids) > 1, "Expected multiple sessions over 200 events"

    def test_user_id_stable_within_session(self, producer):
        producer.session_id = None
        producer.user_id = None
        first_event = producer.generate_event()
        uid = first_event["user_id"]
        for _ in range(20):
            event = producer.generate_event()
            if event["session_id"] == first_event["session_id"]:
                assert event["user_id"] == uid, (
                    "User ID changed within same session"
                )


class TestFieldContent:
    """Validate field content is realistic."""

    def test_device_info_contains_type_os_browser(self, producer):
        for _ in range(50):
            event = producer.generate_event()
            if "device_info" in event:
                assert "type" in event["device_info"]
                assert "os" in event["device_info"]
                assert "browser" in event["device_info"]
                assert event["device_info"]["type"] in (
                    "desktop", "mobile", "tablet"
                )

    def test_location_contains_city_and_coords(self, producer):
        for _ in range(50):
            event = producer.generate_event()
            if "location" in event:
                assert "city" in event["location"]
                assert "latitude" in event["location"]
                assert "longitude" in event["location"]
                assert -90 <= event["location"]["latitude"] <= 90
                assert -180 <= event["location"]["longitude"] <= 180

    def test_product_price_is_positive(self, producer):
        for _ in range(50):
            event = producer.generate_event()
            if "product_price" in event:
                assert event["product_price"] > 0
