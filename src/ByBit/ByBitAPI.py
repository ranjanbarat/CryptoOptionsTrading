"""

https://www.python-httpx.org/api/#asyncclient

Class httpx.AsyncClient(*, auth=None, params=None, headers=None, cookies=None, verify=True, cert=None, http1=True,
http2=False, proxy=None, proxies=None, mounts=None, timeout=Timeout(timeout=5.0), follow_redirects=False,
limits=Limits(max_connections=100, max_keepalive_connections=20, keepalive_expiry=5.0), max_redirects=20,
event_hooks=None, base_url='', transport=None, app=None, trust_env=True, default_encoding='utf-8')


Order management is Multi-Threading or Multi-Processing Program
Objective : Place the Order, Get it Executed ASAP & Get a Confirmation
Algorithm Flow:
    1. Invoked by Receiving the Symbol Name & other Mandatory Parameters
        1. Price
        2. Quantity
        3. Order Type
        4. BUY or SELL
        5. Delta
    2. Get the Authentication Headers
    3. Send the Order to ByBit Exchange
    4. Wait for Confirmation Msg
    5. Fetch the Open Order & Executed Order & Wait till Order Execution,
    6. Fetch the Order Book for that Symbol & Change the Price +/- 10% to get the Order Executed ASAP
    7. Get the Order Executed with +/- 10% of Delta Deviation,
        Raise Exception Notification if the Delta breaches the threshold

"""
import hashlib
import hmac
import json
import time
import uuid
from datetime import datetime

import httpx
import numpy as np
import pandas as pd


async def _get_time_stamp():
    """
        Asynchronously retrieves the current time stamp.

        This method has calculated the current time in milliseconds since the Unix epoch and returns it as a string.

        Returns:
            str: The current time stamp in milliseconds.
    """
    time_stamp: str = str(int(time.time() * 10 ** 3))
    return time_stamp


class ByBitAPI:
    """
    Args:
        default_quantity (float, optional): Default Short Quantity for Option Shorting
        api_url (str, optional): API URL for the Option Exchange
        api_key (str, optional): API Keys for the Exchange
        api_secret (str, optional): API Secret for the Option Exchange
        baseCoin (str, optional): Must be "BTC" or "ETH" or "XRP" or "SOL"
    """
    def __init__(self, default_quantity: float | None = None,
                 api_url: str | None = None,
                 api_key: str | None = None,
                 api_secret: str | None = None,
                 baseCoin: str | None = None,
                 settleCoin: str | None = None):

        """
        Args:
            default_quantity: The default quantity for orders, expressed as a float.
            It is converted to a string for parameter compatibility.
            api_url: The base URL for the API endpoint.
            api_key: The API key used for authentication.
            api_secret: The API secret used for authentication.
            baseCoin: The base cryptocurrency (e.g., BTC, ETH, XRP, SOL) for which perpetual futures are traded.
            settleCoin: The cryptocurrency used for settlement of trades.
        """
        self.json_option_position_params = None
        self.option_position_params = None
        self.json_option_position_params = None
        self.option_position_params = None
        self.perpfutures_position_info_json = None
        self.baseCoin:str | None  = None
        self.perpetual_future: str | None = None
        self.settleCoin: str = settleCoin

        if baseCoin == "BTC":
            # BTCPERP, ETHPERP, SOLPERP, XRPPERP
            self.perpetual_future: str = "BTCPERP"
            self.baseCoin: str = "BTC"

        elif baseCoin == "ETH":
            # BTCPERP, ETHPERP, SOLPERP, XRPPERP
            self.perpetual_future: str = "ETHPERP"
            self.baseCoin: str = "ETH"

        elif baseCoin == "XRP":
            # BTCPERP, ETHPERP, SOLPERP, XRPPERP
            self.perpetual_future: str = "XRPPERP"
            self.baseCoin: str = "XRP"

        elif baseCoin == "SOL":
            # BTCPERP, ETHPERP, SOLPERP, XRPPERP
            self.perpetual_future: str = "SOLPERP"
            self.baseCoin: str = "SOL"

        # Static Values for API URL
        self.api_url = api_url

        self.endpoint_Place_Order = "/v5/order/create"

        self.endpoint_get_position_info = "/v5/position/list"
        # Static Values for API Key
        self.API_Key = api_key
        # Static Values for API Secret
        # noinspection SpellCheckingInspection
        self.API_Secret = api_secret
        # Minimum Order Quantity for Perpetual Futures
        self.min_qty_perpFutures = 0.001
        # Minimum Order Quantity for Perpetual Futures
        self.min_qty_Options = 0.01
        # Convert the Options Base Shorting Quantity into String as it needs to be passed in Params as a string object;
        # it's expressed in Lot Size, Bitcoin lot size is 1.00 BTC for 1000 Contracts so its 0.001 Per Contract
        self.default_quantity: str = str(default_quantity)
        # Static Parameters
        self.recv_window: str = str(5000)

        self.category: dict = {"spot": "spot",
                               "linear": "linear",
                               "inverse": "inverse",
                               "option": "option"}

        self.side: dict = {"Buy": "Buy",
                           "Sell": "Sell"}

        self.orderType: dict = {"Market": "Market",
                                "Limit": "Limit"}

        self.timeInForce: dict = {"IOC": "IOC",
                                  "GTC": "GTC"}

        self.optionParams: dict = {}
        self.json_optionParams: str | None = None
        self.PerpFutureParams: dict = {}
        self.jsonPerpFutureParams: str | None = None

        self.options_position_params: dict = {}
        self.json_options_position_params: str | None = None

        self.perp_future_position_params: dict = {}
        self.json_perp_future_position_params: str | None = None
        ####
        self.option_position_info_json = None
        self.options_position_dataframe: pd.DataFrame | None = None

    async def _generateSignature(self, params, time_stamp):
        """
        Generates an HMAC SHA256 signature using the provided parameters and timestamp.

        Args:
            params: A dictionary of request parameters to include in the signature.
            time_stamp: A timestamp to be included in the signature computation.

        Returns:
            A hexadecimal string representing the generated signature.
        """
        param_str = str(time_stamp) + self.API_Key + self.recv_window + str(params)
        print(f"Parameter String is : {param_str}\n")
        ByBit_hash = hmac.new(bytes(self.API_Secret, "utf-8"), param_str.encode("utf-8"), hashlib.sha256)
        signature = ByBit_hash.hexdigest()
        print(f"Generated Signature is : {signature}\n")
        return signature

    async def create_Option_Order(self, direction: str = "SELL",
                                  symbol: str | None = None,
                                  quantity: float = 1.00):
        """
        Creates an order for an option trade with specified parameters.

        Args:
            direction: The direction of the order, either "SELL" or "BUY".
            Defaults to "SELL".
            symbol: The symbol for the option to be traded.
            Can be None.
            quantity: The quantity for the order.
            Defaults to 1.00.

        """
        order_side = None
        orderLinkId = uuid.uuid4().hex
        if direction == "SELL":
            order_side = self.side["Sell"]
        elif direction == "BUY":
            order_side = self.side["Buy"]

        # math is-close (quantity, self.default_quantity, rel_tol=1e-02, abs_tol=1e-02)

        self.optionParams = {"category": str(self.category["option"]),
                             "symbol": str(symbol),
                             "side": order_side,
                             "positionIdx": 0,
                             "orderType": str(self.orderType["Market"]),
                             "qty": str(quantity),
                             "timeInForce": str(self.timeInForce["IOC"]),
                             "orderLinkId": orderLinkId}

        print(f"Options API Parameter : {self.optionParams}\n")

        # self.json_optionParams = str(json.dumps(self.optionParams, separators=(',', ':')))
        self.json_optionParams = str(json.dumps(self.optionParams))
        print(f"Options API Parameter in Compact JSON Format : {self.json_optionParams}\n")

        time_stamp = await _get_time_stamp()

        api_signature = await self._generateSignature(params=self.json_optionParams, time_stamp=time_stamp)
        print(f"Options API Signature : {api_signature}\n")

        option_Headers: dict = {'X-BAPI-API-KEY': self.API_Key,
                                'X-BAPI-SIGN': api_signature,
                                'X-BAPI-SIGN-TYPE': '2',
                                'X-BAPI-TIMESTAMP': time_stamp,
                                'X-BAPI-RECV-WINDOW': self.recv_window,
                                'Content-Type': 'application/json'}

        print(f"API Header is : {option_Headers} \n")

        async with httpx.AsyncClient() as client:
            # Await Data from the "GET" Request
            r = await client.post(url=f"{self.api_url}" + f"{self.endpoint_Place_Order}",
                                  headers=option_Headers, data=self.json_optionParams)
            # Check if We have Received the Data or its Data Error
            if r.json()["retMsg"] == "OK":
                # Print the Length of the Option Chain that we have Received from the API Call
                print(f"Placed the Order for {symbol} \n")
                print(f"Order ID : {r.json()["result"]["orderId"]}")
                print(f"Order Link ID : {r.json()["result"]["orderLinkId"]} & Generated orderLinkId is : {orderLinkId}")
            else:
                # Print the Error Message if No Data is Received
                print(f"Error in Placing the Order for the Symbol : {symbol}")
            print(f"JSON Output : {r.json()} \n")

    async def create_PerpFutures_Order(self, direction: str, quantity: float):
        """
        Creates a perpetual futures order and submits it to the API.

        Args:
            direction: The direction of the order, either "BUY" or "SELL".
            quantity: The quantity of the perpetual futures to order.

        Returns:
            None
        """
        order_side = None
        orderLinkId = uuid.uuid4().hex
        if direction == "SELL":
            order_side = self.side["Sell"]
        elif direction == "BUY":
            order_side = self.side["Buy"]

        self.PerpFutureParams = {"category": str(self.category["linear"]),
                                    "symbol": str(self.perpetual_future),
                                    "side": str(order_side),
                                    "positionIdx": 0,
                                    "orderType": str(self.orderType["Market"]),
                                    "qty": str(quantity),
                                    "timeInForce": str(self.timeInForce["IOC"]),
                                    "orderLinkId": orderLinkId}

        print(f"Perpetual Future API Parameter : {self.PerpFutureParams}\n")

        # self.jsonPerpFutureParams = str(json.dumps(self.PerpFutureParams, separators=(',', ':')))
        self.jsonPerpFutureParams = str(json.dumps(self.PerpFutureParams))
        print(f"Options API Parameter in Compact JSON Format : {self.jsonPerpFutureParams}\n")

        time_stamp = await _get_time_stamp()

        api_signature = await self._generateSignature(params=self.jsonPerpFutureParams, time_stamp=time_stamp)
        print(f"Options API Signature : {api_signature}\n")

        perp_future_Headers: dict = {
            'X-BAPI-API-KEY': self.API_Key,
            'X-BAPI-SIGN': api_signature,
            'X-BAPI-SIGN-TYPE': '2',
            'X-BAPI-TIMESTAMP': time_stamp,
            'X-BAPI-RECV-WINDOW': self.recv_window,
            'Content-Type': 'application/json'
        }
        print(f"API Header is : {perp_future_Headers} \n")

        async with httpx.AsyncClient() as client:
            # Await Data from the "GET" Request
            r = await client.post(url=f"{self.api_url}" + f"{self.endpoint_Place_Order}",
                                  headers=perp_future_Headers, data=self.jsonPerpFutureParams)
            # Check if We have Received the Data or its Data Error
            if r.json()["retMsg"] == "OK":
                # Print the Length of the Option Chain that we have Received from the API Call
                print(f"Placed the {direction} Order for BTC/USDC Perpetual Futures Quantity : {quantity} \n")
                print(f"Order ID : {r.json()["result"]["orderId"]}")
                print(f"Order Link ID : {r.json()["result"]["orderLinkId"]} & Generated orderLinkId is : {orderLinkId}")
            else:
                # Print the Error Message if No Data is Received
                print(f"Error in Placing the {direction} Order for BTC/USDC Perpetual Futures Quantity : {quantity} \n")

            print(f"JSON Output : {r.json()} \n")

    async def get_PerpFutures_Position(self):
        """
        Fetch the perpetual futures position information.

        This asynchronous method constructs the necessary parameters and headers for
        the API request to retrieve the user's perpetual futures position information.
        The request is sent to the specified endpoint using an `httpx.AsyncClient` and
        the response is returned in JSON format.

        Returns:
            Response (json): The JSON response containing the perpetual futures position information.

        Raises:
            Exception: If there's an error in fetching the position info.
        """
        perpfutures_params = (f"category={self.category["linear"]}" +
                              f"&baseCoin={self.baseCoin}" +
                              f"&settleCoin={self.settleCoin}")

        time_stamp = await _get_time_stamp()

        perpfutures_api_sigs = await self._generateSignature(params=perpfutures_params,
                                                             time_stamp=time_stamp)

        print(f"Perpectual Futures Position Info API Signature : {perpfutures_api_sigs}\n")

        perpfutures_Headers: dict = {
            'X-BAPI-API-KEY': self.API_Key,
            'X-BAPI-SIGN': perpfutures_api_sigs,
            'X-BAPI-SIGN-TYPE': '2',
            'X-BAPI-TIMESTAMP': time_stamp,
            'X-BAPI-RECV-WINDOW': self.recv_window,
            'Content-Type': 'application/json'
        }

        print(f"API Header is : {perpfutures_Headers} \n")

        url = f"{self.api_url}" + f"{self.endpoint_get_position_info}" + f"?" + f"{perpfutures_params}"

        print(f"Final URL is : {url}")

        try:
            async with httpx.AsyncClient() as client:
                # Await Data from the "GET" Request
                self.perpfutures_position_info_json = await client.get(url=url, headers=perpfutures_Headers)
                # Check if We have Received the Data or its Data Error
            print(f"Positions Info Length is : {len(self.perpfutures_position_info_json.json()["result"]["list"])}")
        except Exception as e:
            # Print the Error Message if No Data is Received
            print(f"Error in Fetching the Positions Info, Error Details : {e}")

        return self.perpfutures_position_info_json

    async def format_perpfutures(self):
        """
        Formats perpetual futures position information obtained from a JSON response.

        The method processes data from `self.perpfutures_position_info_json` by:
        1. Normalizing the JSON data into a pandas DataFrame.
        2. Dropping unnecessary columns related to position details.
        3. Ensuring that specific columns have the correct data types.
        4. Converting timestamp columns from milliseconds to datetime objects.

        Returns:
            DataFrame: A pandas DataFrame containing formatted perpetual futures position data.
        """
        PerFutures_df = pd.json_normalize(data=self.perpfutures_position_info_json.json()["result"]["list"])
        positions_labels = ["leverage", "autoAddMargin", "riskLimitValue", "takeProfit", "isReduceOnly",
                            "tpslMode", "leverageSysUpdatedTime", "mmrSysUpdatedTime", "stopLoss", "tradeMode",
                            "sessionAvgPrice", "trailingStop", "bustPrice", "positionBalance", "positionIdx",
                            "positionIM", "positionMM", "adlRankIndicator", "cumRealisedPnl", "curRealisedPnl", "riskId",
                            "liqPrice"]

        PerFutures_df.drop(labels=positions_labels, axis=1, inplace=True)

        PerFutures_df['symbol'] = PerFutures_df['symbol'].astype(dtype="string")
        PerFutures_df['avgPrice'] = PerFutures_df['avgPrice'].astype(dtype="float")
        PerFutures_df['positionValue'] = PerFutures_df['positionValue'].astype(dtype="float")
        PerFutures_df['unrealisedPnl'] = PerFutures_df['unrealisedPnl'].astype(dtype="float")
        PerFutures_df['markPrice'] = PerFutures_df['markPrice'].astype(dtype="float")

        PerFutures_df['createdTime'] = PerFutures_df['createdTime'].astype(dtype="float")
        PerFutures_df['createdTime'] = pd.to_datetime(PerFutures_df['createdTime'],unit='ms')

        PerFutures_df['updatedTime'] = PerFutures_df['updatedTime'].astype(dtype="float")
        PerFutures_df['updatedTime'] = pd.to_datetime(PerFutures_df['updatedTime'],unit='ms')

        PerFutures_df['side'] = PerFutures_df['side'].astype(dtype="string")
        PerFutures_df['size'] = PerFutures_df['size'].astype(dtype="float")
        PerFutures_df['markPrice'] = PerFutures_df['markPrice'].astype(dtype="float")

        return PerFutures_df

    async def get_option_positions(self):
        """
        Fetches and returns the option positions for a given cryptocurrency.

        Builds the request parameters and headers required for the API call, generates
        the necessary signatures, and constructs the final URL for the API endpoint.
        Sends an asynchronous GET request to the constructed URL and fetches the option
        positions data.
        Handles and prints errors in case the request fails.

        Args:

        Returns:
            dict: A JSON object containing the option positions information or an error message.
        """
        # option_position_params = "category=option&baseCoin=BTC&settleCoin=USDC"
        option_position_params = (f"category={self.category["option"]}" + f"&baseCoin={self.baseCoin}" +
                                      f"&settleCoin={self.settleCoin}")
        # json_option_position_params = str(json.dumps(option_position_params))
        # print(f"BTC Options Position API Parameter in JSON Format : {json_option_position_params}\n")

        time_stamp = await _get_time_stamp()

        option_api_signature = await self._generateSignature(params=option_position_params,
                                                             time_stamp=time_stamp)

        print(f"Option Position Info API Signature : {option_api_signature}\n")

        option_Headers: dict = {
            'X-BAPI-API-KEY': self.API_Key,
            'X-BAPI-SIGN': option_api_signature,
            'X-BAPI-SIGN-TYPE': '2',
            'X-BAPI-TIMESTAMP': time_stamp,
            'X-BAPI-RECV-WINDOW': self.recv_window,
            'Content-Type': 'application/json'
        }

        print(f"API Header is : {option_Headers} \n")

        url = f"{self.api_url}" + f"{self.endpoint_get_position_info}" + f"?" + f"{option_position_params}"

        print(f"Final URL is : {url}")

        try:
            async with httpx.AsyncClient() as client:
                # Await Data from the "GET" Request
                self.option_position_info_json = await client.get(url=url, headers=option_Headers)
                # Check if We have Received the Data or its Data Error
            print(f"Positions Info Length is : {len(self.option_position_info_json.json()["result"]["list"])}")
        except Exception as e:
            # Print the Error Message if No Data is Received
            print(f"Error in Fetching the Positions Info, Error Details : {e}")

        return self.option_position_info_json

    async def format_option_position_dataframe(self):
        """
        Format the JSON data containing option position information into a structured pandas DataFrame.

        This method performs the following tasks:
        1. Flatten JSON data and normalize it into a DataFrame.
        2. Drop specific labels that are not required for analysis.
        3. Initialize multiple numpy arrays to hold various computed values and metadata.
        4. Merge these numpy arrays with the DataFrame.
        5. Split the symbol into option type, expiry date, and strike price and compute delta values.
        6. Convert specific DataFrame columns to their correct data types.

        Raises:
            Exception: General exception when splitting the symbol data.
            Exception: General exception when processing data types of specific columns.

        """
        try:
            if self.option_position_info_json is not None:
                self.options_position_dataframe = pd.json_normalize(
                    data=self.option_position_info_json.json()["result"]["list"])

                # noinspection SpellCheckingInspection
                positions_labels = ["leverage", "autoAddMargin", "riskLimitValue", "takeProfit", "isReduceOnly",
                                    "tpslMode", "leverageSysUpdatedTime", "mmrSysUpdatedTime", "stopLoss", "tradeMode",
                                    "sessionAvgPrice", "trailingStop", "bustPrice", "positionBalance", "positionIdx",
                                    "positionIM", "positionMM", "adlRankIndicator", "cumRealisedPnl", "riskId",
                                    "liqPrice"]

                ByBit_date_format = "%d%b%y"  # Date Format as (DD)(MMM)(YY)

                self.options_position_dataframe.drop(labels=positions_labels, axis=1, inplace=True)
                # We are Initialing Filling "UNKNOWN" as String Value, Later it will be replaced by "CALL" or "PUT"
                option_type = np.array(["UNKNOWN" for _ in range(len(self.options_position_dataframe))])

                # Initialize Strike_Price Numpy Array, we need to fill it with actual Values after Splitting the
                # Symbol String We are Initialing Filling "-5000" as Float64 Value, Later it will be replaced by
                # Actual Strike Price
                strike_price = np.array([-5000.00 for _ in range(len(self.options_position_dataframe))])

                # Initialize Expiry_Date Numpy Array; we need to fill it with actual Values after Splitting the
                # Symbol String We are Initialing Filling "1-1-2000" as datetime Value, Later it will be replaced by
                # Actual Expiry Dates
                expiry = np.array(
                    [datetime(year=2000, month=1, day=1).date() for _ in range(len(self.options_position_dataframe))])

                # Initialize Positional_Delta Numpy Array,
                # we need to fill it with actual Values after Computing the Delta of PUT & CALL Options
                # We are Initialing Filling "-5000" as Float64 Value, Later it will be replaced by Computed Delta Value
                total_delta = np.array([-5000.00 for _ in range(len(self.options_position_dataframe))])

                # We are Initialing Filling "-5000" as Float64 Value, Later it will be replaced by Computed Delta Value
                expiry_delta = np.array([-5000.00 for _ in range(len(self.options_position_dataframe))])

                # Define the NP Array for the Type of Expiry "Daily", "Weekly", "Monthly", "Quarterly"
                expiry_type = np.array(["UNKNOWN" for _ in range(len(self.options_position_dataframe))])

                # Define the Tolerance Limit of Delta before Executing the Hedging
                delta_hedging = np.array([-5000.00 for _ in range(len(self.options_position_dataframe))])

                # Define the Bool Array to Signify the Delta Hedging Requirements
                hedging_required = np.array([False for _ in range(len(self.options_position_dataframe))])

                # Define the NP Array to Compute Perpectual Futures Required to fully Delta Hedge this Position
                PerpFutureQty = np.array([-5000.00 for _ in range(len(self.options_position_dataframe))])

                # Merge the Numpy Array of "Option Type" with Primary DataFrame
                self.options_position_dataframe['option_type'] = option_type

                # Merge the Numpy Array of "Strike_Price" with Primary DataFrame
                self.options_position_dataframe['strike_price'] = strike_price

                # Merge the Numpy Array of "Expiry Dates" with Primary DataFrame
                self.options_position_dataframe['expiry'] = expiry

                # Merge the Numpy Array of "Expiry Dates" with Primary DataFrame
                self.options_position_dataframe['total_delta'] = total_delta

                # Merge the Numpy Array of "Expiry Delta" with Primary DataFrame
                self.options_position_dataframe['expiry_delta'] = expiry_delta

                # Define the NP Array for the Type of Expiry "Daily", "Weekly", "Monthly", "Quarterly"
                self.options_position_dataframe['expiry_type'] = expiry_type

                # Define the Tolerance Limit of Delta before Executing the Hedging
                self.options_position_dataframe['delta_hedging'] = delta_hedging

                # Define the Bool Array to Signify the Delta Hedging Requirements
                self.options_position_dataframe['hedging_required'] = hedging_required

                # Define the NP Array to Compute Perpectual Futures Required to fully Delta Hedge this Position
                self.options_position_dataframe['PerpFutureQty'] = PerpFutureQty

                try:
                    # Split the Symbol into Option_Type, Expiry_Date, & Strike Price
                    for i in range(len(self.options_position_dataframe)):
                        split_symbol = self.options_position_dataframe.loc[i, "symbol"].split("-")

                        self.options_position_dataframe.loc[i, "expiry"] = datetime.strptime(str(split_symbol[1]),
                                                                                             ByBit_date_format).date()

                        self.options_position_dataframe.loc[i, "strike_price"] = float(split_symbol[2])

                        if split_symbol[3] == "P":
                            self.options_position_dataframe.loc[i, "option_type"] = "PUT"

                        elif split_symbol[3] == "C":
                            self.options_position_dataframe.loc[i, "option_type"] = "CALL"

                        self.options_position_dataframe.loc[i, "total_delta"] = (
                                    float(self.options_position_dataframe.iloc[i]["size"]) *
                                    float(self.options_position_dataframe.iloc[i]["delta"]))

                        print(f"For Index Position {i}, Symbol Data was Split Correctly : {split_symbol}",
                              f"Expiry Data: {self.options_position_dataframe.loc[i, "expiry"]}",
                              f"Strike Price: {self.options_position_dataframe.loc[i, "strike_price"]}",
                              f"Options Type Data: {self.options_position_dataframe.loc[i, "option_type"]},"
                              f"Total Delta : {self.options_position_dataframe.loc[i, "total_delta"]} ,"
                              f"Delta : {self.options_position_dataframe.loc[i,"delta"]} \n")

                except Exception as e:
                    print(f"Error in Splitting the Symbol Data, Error Details : {e}")

                try:
                    self.options_position_dataframe['symbol'] = self.options_position_dataframe['symbol'].astype(
                        dtype="string")
                except Exception as e:
                    print(f"Error in processing 'symbol' dataType, Error Code : {e}")

                try:
                    self.options_position_dataframe['avgPrice'] = self.options_position_dataframe['avgPrice'].astype(
                        dtype="float64")
                except Exception as e:
                    print(f"Error in processing 'avgPrice' dataType, Error Code : {e}")

                try:
                    self.options_position_dataframe['delta'] = self.options_position_dataframe['delta'].astype(
                        dtype="float64")
                except Exception as e:
                    print(f"Error in processing 'delta' dataType, Error Code : {e}")

                try:
                    self.options_position_dataframe['theta'] = self.options_position_dataframe['theta'].astype(
                        dtype="float64")
                except Exception as e:
                    print(f"Error in processing 'theta' dataType, Error Code : {e}")

                try:
                    self.options_position_dataframe['positionValue'] = self.options_position_dataframe[
                        'positionValue'].astype(dtype="float64")
                except Exception as e:
                    print(f"Error in processing 'positionValue' dataType, Error Code : {e}")

                try:
                    self.options_position_dataframe['unrealisedPnl'] = self.options_position_dataframe[
                        'unrealisedPnl'].astype(dtype="float64")
                except Exception as e:
                    print(f"Error in processing 'unrealisedPnl' dataType, Error Code : {e}")

                try:
                    self.options_position_dataframe['markPrice'] = self.options_position_dataframe['markPrice'].astype(
                        dtype="float64")
                except Exception as e:
                    print(f"Error in processing 'markPrice' dataType, Error Code : {e}")

                try:
                    self.options_position_dataframe['createdTime'] = self.options_position_dataframe[
                        'createdTime'].astype(dtype="float64")
                    self.options_position_dataframe['createdTime'] = pd.to_datetime(
                        self.options_position_dataframe['createdTime'], unit='ms')
                except Exception as e:
                    print(f"Error in processing 'createdTime' dataType, Error Code : {e}")

                try:
                    self.options_position_dataframe['seq'] = self.options_position_dataframe['seq'].astype(
                        dtype="int64")
                except Exception as e:
                    print(f"Error in processing 'seq' dataType, Error Code : {e}")

                try:
                    self.options_position_dataframe['updatedTime'] = self.options_position_dataframe[
                        'updatedTime'].astype(dtype="float64")
                    self.options_position_dataframe['updatedTime'] = pd.to_datetime(
                        self.options_position_dataframe['updatedTime'], unit='ms')
                except Exception as e:
                    print(f"Error in processing 'updatedTime' dataType, Error Code : {e}")

                try:
                    self.options_position_dataframe['side'] = self.options_position_dataframe['side'].astype(
                        dtype="string")
                except Exception as e:
                    print(f"Error in processing 'side' dataType, Error Code : {e}")

                try:
                    self.options_position_dataframe['curRealisedPnl'] = self.options_position_dataframe[
                        'curRealisedPnl'].astype(dtype="float64")
                except Exception as e:
                    print(f"Error in processing 'curRealisedPnl' dataType, Error Code : {e}")

                try:
                    self.options_position_dataframe['size'] = self.options_position_dataframe['size'].astype(
                        dtype="float64")
                except Exception as e:
                    print(f"Error in processing 'size' dataType, Error Code : {e}")

                try:
                    self.options_position_dataframe['positionStatus'] = self.options_position_dataframe[
                        'positionStatus'].astype(dtype="string")
                except Exception as e:
                    print(f"Error in processing 'positionStatus' dataType, Error Code : {e}")

                try:
                    self.options_position_dataframe['gamma'] = self.options_position_dataframe['gamma'].astype(
                        dtype="float64")
                except Exception as e:
                    print(f"Error in processing 'gamma' dataType, Error Code : {e}")

                try:
                    self.options_position_dataframe['vega'] = self.options_position_dataframe['vega'].astype(
                        dtype="float64")
                except Exception as e:
                    print(f"Error in processing 'vega' dataType, Error Code : {e}")

                try:
                    self.options_position_dataframe['option_type'] = self.options_position_dataframe[
                        'option_type'].astype(dtype="string")
                except Exception as e:
                    print(f"Error in processing 'option_type' dataType, Error Code : {e}")

                try:
                    self.options_position_dataframe['strike_price'] = self.options_position_dataframe[
                        'strike_price'].astype(dtype="float64")
                except Exception as e:
                    print(f"Error in processing 'strike_price' dataType, Error Code : {e}")

                try:
                    self.options_position_dataframe['total_delta'] = self.options_position_dataframe[
                        'total_delta'].astype(dtype="float64")
                except Exception as e:
                    print(f"Error in processing 'total_delta' dataType, Error Code : {e}")

                try:
                    self.options_position_dataframe['expiry_delta'] = self.options_position_dataframe[
                        'expiry_delta'].astype(dtype="float64")
                except Exception as e:
                    print(f"Error in processing 'expiry_delta' dataType, Error Code : {e}")

                try:
                    self.options_position_dataframe['expiry_type'] = self.options_position_dataframe[
                        'expiry_type'].astype(dtype="string")
                except Exception as e:
                    print(f"Error in processing 'expiry_type' dataType, Error Code : {e}")

                try:
                    self.options_position_dataframe['delta_hedging'] = self.options_position_dataframe[
                        'delta_hedging'].astype(dtype="float64")
                except Exception as e:
                    print(f"Error in processing 'delta_hedging' dataType, Error Code : {e}")

                try:
                    self.options_position_dataframe['hedging_required'] = self.options_position_dataframe[
                        'hedging_required'].astype(dtype="bool")
                except Exception as e:
                    print(f"Error in processing 'hedging_required' dataType, Error Code : {e}")

                try:
                    self.options_position_dataframe['PerpFutureQty'] = self.options_position_dataframe[
                        'PerpFutureQty'].astype(dtype="float64")
                except Exception as e:
                    print(f"Error in processing 'PerpFutureQty' dataType, Error Code : {e}")

                positions_expiry_list = self.options_position_dataframe["expiry"].drop_duplicates().copy()

                # numpy_positions_expiry_list = self.options_position_dataframe["expiry"].drop_duplicates().to_numpy()
                try:
                    for i in range(len(positions_expiry_list)):
                        index_value = int(positions_expiry_list.index[i])
                        print(f"The Index Value Type is : {type(index_value)} , & the Index Value is : {index_value}")
                        cumulative_delta = sum(self.options_position_dataframe[
                                                   self.options_position_dataframe.expiry ==
                                                                               positions_expiry_list[
                                                                                   index_value]].total_delta)
                        print(f"Cumulative_Delta is : {cumulative_delta}")
                        self.options_position_dataframe.loc[
                            self.options_position_dataframe.expiry == positions_expiry_list[
                                index_value], "delta_hedging"] = cumulative_delta


                except Exception as e:
                    print(f"Error in processing 'Cumulative Delta', Error Code : {e}")

            else:
                print(f"None Object was Passed, NO JSON Data is Returned\n")

        except Exception as e:
            print(f"Error in Formatting Option Position Dataframe : {e}")

        finally:
            return self.options_position_dataframe
