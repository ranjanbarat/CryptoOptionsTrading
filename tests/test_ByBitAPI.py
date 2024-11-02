# test_ByBitAPI.py
from src.ByBit.ByBitAPI import ByBitAPI


def test_init_with_all_parameters():
    api = ByBitAPI(
        default_quantity=1.0,
        api_url='https://api-demo.bybit.com',
        api_key='6TzoOeOuyIDiJN9cOu',
        api_secret='JbTE9Z5OFFyxcw1mzqf6hLIpmGgt8dONmlSE',
        baseCoin='BTC'
    )

    assert api.api_url == 'https://api-demo.bybit.com'
    assert api.API_Key == '6TzoOeOuyIDiJN9cOu'
    assert api.API_Secret == 'JbTE9Z5OFFyxcw1mzqf6hLIpmGgt8dONmlSE'
    assert api.default_quantity == '1.0'
    assert api.baseCoin == "BTC" or  "ETH" or "SOL" or "XRP"
    assert api.endpoint_Place_Order == "/v5/order/create"
    assert api.endpoint_get_position_info == "/v5/position/list"
    assert api.min_qty_perpFutures == 0.001
    assert api.min_qty_Options == 0.01
    assert api.recv_window == '5000'
    assert api.category == {"spot": "spot", "linear": "linear", "inverse": "inverse", "option": "option"}
    assert api.side == {"Buy": "Buy", "Sell": "Sell"}
    assert api.orderType == {"Market": "Market", "Limit": "Limit"}
    assert api.timeInForce == {"IOC": "IOC", "GTC": "GTC"}
    assert api.perpetual_future == 'BTCPERP'
    assert api.optionParams == {}
    assert api.json_optionParams is None
    assert api.PerpFutureParams == {}
    assert api.jsonPerpFutureParams is None
    assert api.options_position_params == {}
    assert api.json_options_position_params is None
    assert api.perp_future_position_params == {}
    assert api.json_perp_future_position_params is None
    assert api.option_position_params is None
    assert api.json_option_position_params is None
    assert api.option_position_info_json is None
    assert api.options_position_dataframe is None


def test_init_with_missing_optional_parameters():
    api = ByBitAPI()

    assert api.api_url is None
    assert api.API_Key is None
    assert api.API_Secret is None
    assert api.default_quantity == 'None'
    assert api.baseCoin == "BTC" or "ETH" or "SOL" or "XRP"
    assert api.endpoint_Place_Order == "/v5/order/create"
    assert api.endpoint_get_position_info == "/v5/position/list"
    assert api.min_qty_perpFutures == 0.001
    assert api.min_qty_Options == 0.01
    assert api.recv_window == '5000'
    assert api.category == {"spot": "spot", "linear": "linear", "inverse": "inverse", "option": "option"}
    assert api.side == {"Buy": "Buy", "Sell":"Sell"}
    assert api.orderType == {"Market": "Market", "Limit": "Limit"}
    assert api.timeInForce == {"IOC": "IOC", "GTC": "GTC"}
    assert api.perpetual_future == 'BTCPERP'
    assert api.optionParams == {}
    assert api.json_optionParams is None
    assert api.PerpFutureParams == {}
    assert api.jsonPerpFutureParams is None
    assert api.options_position_params == {}
    assert api.json_options_position_params is None
    assert api.perp_future_position_params == {}
    assert api.json_perp_future_position_params is None
    assert api.option_position_params is None
    assert api.json_option_position_params is None
    assert api.option_position_info_json is None
    assert api.options_position_dataframe is None


def test_init_default_quantity_conversion():
    api = ByBitAPI(default_quantity=0.005)
    assert api.default_quantity == '0.005'
