import hashlib
import hmac

import pytest
from src.ByBit.ByBitAPI import ByBitAPI


class TestByBitAPI:
    @pytest.fixture
    def setup_api(self):
        api = ByBitAPI(api_key="test_api_key", api_secret="test_api_secret")
        return api

    def test_generate_signature(self, setup_api):
        params = {"param1": "value1", "param2": "value2"}
        time_stamp = 1234567890
        expected_param_str = f"{time_stamp}test_api_keyNone{params}"
        expected_signature = hmac.new(
            bytes("test_api_secret", "utf-8"),
            expected_param_str.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

        signature = setup_api._generateSignature(params, time_stamp)

        assert signature == expected_signature

    def test_generate_signature_empty_params(self, setup_api):
        params = {}
        time_stamp = 1234567890
        expected_param_str = f"{time_stamp}test_api_keyNone{params}"
        expected_signature = hmac.new(
            bytes("test_api_secret", "utf-8"),
            expected_param_str.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

        signature = setup_api._generateSignature(params, time_stamp)

        assert signature == expected_signature

    def test_generate_signature_different_timestamp(self, setup_api):
        params = {"param1": "value1", "param2": "value2"}
        time_stamp = 9876543210
        expected_param_str = f"{time_stamp}test_api_keyNone{params}"
        expected_signature = hmac.new(
            bytes("test_api_secret", "utf-8"),
            expected_param_str.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

        signature = setup_api._generateSignature(params, time_stamp)

        assert signature == expected_signature
