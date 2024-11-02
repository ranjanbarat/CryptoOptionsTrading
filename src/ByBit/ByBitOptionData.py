"""
This is a Public API Needs NO API Credentials
We will fetch the Ticker Data
Process the Ticker to Add the Following : Option_type, Strike_price, Expiry_Dates
as it will help us in further processing to refine the Options Data to take Required Positions

Algorithm as Follows:
    1. Fetch the Data from API Endpoints
    2. Convert the JSON Data into Panda DataFrame
        a. Segregate the Data Types from "Objects" into Strings, Float64, DataTime(ns64)
    3. Add these Additional Parameters into DataFrame
        a. Option_type
        b. Strike_price
        c. Expiry_Dates
    4. Generate Expiry Dates
    5. Segregate the DataFrame into Expiry Dates
        1. Daily Expiry
            a. Current Daily
            b. Next Daily
            c. Next To Next Daily
        2. Weekly Expiry
            a. Current Weekly
            b. Next Weekly
            c. Next To Next Weekly
        3. Monthly Expiry
            a. Current Monthly
            b. Next Monthly
            c. Next To Next Monthly
        4. Quarterly Expiry
            a. Current Quarterly
            b. Next Quarterly
            c. Next To Next Quarterly
"""

import datetime
import time
from datetime import datetime

import httpx
import numpy as np
import pandas as pd

from .ByBitExpiry import ByBitExpiry


class ByBitOptionData(object):
    """
    class ByBitOptionData(object):

    def __init__(self, api_url: str | None = None,
                 api_endpoint: str | None = None,
                 api_parameters: dict | None = None):
    """

    def __init__(self, api_url: str | None = None,
                 api_endpoint: str | None = None,
                 api_parameters: dict | None = None):
        """
        Args:
            api_url: The base URL for the API.
            api_endpoint: The specific API endpoint to be accessed.
            api_parameters: A dictionary of parameters required for the API call.
        """
        # API URL is a Mandatory Parameter to Initialize the ByBitOptionData Class
        self.api_url = api_url

        # API EndPoints is a Mandatory Parameter to Initialize the ByBitOptionData Class
        self.api_endpoint = api_endpoint

        # API Parameters for Fetching the API-Specific Data
        self.api_parameters = api_parameters

        # Initialize the Blank DataFrame Object for storing the JSON Data into Pandas DataFrame Object
        self.dataframe: pd.DataFrame | None = None

        # We need to strip the Symbol into Different Elements with the following details:
        # Base Coin Expiry Dates, Strike Price & Option Type
        # https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior
        self.split_symbol: str | None = None

        # ByBit Date Format to Extract the Date from Splitting the Symbol Value
        self.ByBit_date_format = "%d%b%y"  # Date Format as (DD)(MMM)(YY)

        # Initialize the Default Numpy Array for Storing the Option_Type String Format
        self.option_type: np.ndarray | None = None
        # Initialize the Default Numpy Array for Storing the Strike_Price in Float64 Format
        self.strike_price: np.ndarray | None = None
        # Initialize the Default Numpy Array for Storing the Expiry Date in DateTime.Date Object Format
        self.expiry: np.ndarray | None = None

        # Dictionary Contains Current, Next & Next-to-Next Daily Expiry Dates
        # Initialize the Dictionary to Store the Daily Expiry Dates
        self.daily_expiry: dict | None = None

        # Initialize the Dictionary to Store the Weekly Expiry Dates
        # Dictionary Contains Current, Next & Next-to-Next Weekly Expiry Dates
        self.weekly_expiry: dict | None = None

        # Initialize the Dictionary to Store the Monthly Expiry Dates
        # Dictionary Contains Current, Next & Next-to-Next Monthly Expiry Dates
        self.monthly_expiry: dict | None = None

        # Initialize the Dictionary to Store the Quarterly Expiry Dates
        # Dictionary Contains Current, Next & Next-to-Next Quarterly Expiry Dates
        self.quarterly_expiry: dict | None = None

        # DateFrame for holding the Expiry Dates extracted from the Primary DataFrame
        self.expiry_dates: pd.DataFrame | None = None

        # Dictionary for Holding the Expiry Dates & its Existence
        self.expiry: dict = {}

        # Initialize the Pandas DataFrame for holding the Extracted Expiry-Wise Data #
        # Initialize the Pandas DataFrame for holding the Current Daily Extracted Data
        self.current_daily: pd.DataFrame | None = None
        # Initialize the Pandas DataFrame for holding the Next Daily Extracted Data
        self.next_daily: pd.DataFrame | None = None
        # Initialize the Pandas DataFrame for holding the Next-TO-Next Daily Extracted Data
        self.next_to_next_daily: pd.DataFrame | None = None

        # Initialize the Pandas DataFrame for holding the Current Weekly Extracted Data
        self.current_weekly: pd.DataFrame | None = None
        # Initialize the Pandas DataFrame for holding the Next Weekly Extracted Data
        self.next_weekly: pd.DataFrame | None = None
        # Initialize the Pandas DataFrame for holding the Next-TO-Next Weekly-Extracted Data
        self.next_to_next_weekly: pd.DataFrame | None = None

        # Initialize the Pandas DataFrame for holding the Current Monthly Extracted Data
        self.current_monthly: pd.DataFrame | None = None
        # Initialize the Pandas DataFrame for holding the Next Monthly Extracted Data
        self.next_monthly: pd.DataFrame | None = None
        # Initialize the Pandas DataFrame for holding the Next-TO-Next Monthly Extracted Data
        self.next_to_next_monthly: pd.DataFrame | None = None

        # Initialize the Pandas DataFrame for holding the Current Quarterly Extracted Data
        self.current_quarterly: pd.DataFrame | None = None
        # Initialize the Pandas DataFrame for holding the Next Quarterly Extracted Data
        self.next_quarterly: pd.DataFrame | None = None
        # Initialize the Pandas DataFrame for holding the Next-TO-Next Quarterly Extracted Data
        self.next_to_next_quarterly: pd.DataFrame | None = None

        # NOTE: ANY NEW VARIABLE OR PARAMETER NEEDS TO RE-INITIATED SHOULD BE ADDED INTO THE FUNCTION
        # "_reinitialise_class_variables" AS WE NEED TO CLEAR THE DATA FROM THE PYTHON CLASS OBJECTS
        # FOR THESE VARIABLES FOR THE NEXT DATCH OF DATAFRAME PROCESSING

    async def _reinitialise_class_variables(self):
        """
        Reinitializes the class variables for data processing.

        This method is responsible for the following tasks:
        1. Initializes a blank DataFrame object for storing JSON data into a Pandas DataFrame object.
        2. Initializes variables to handle symbol details including base coin expiry dates,
        strike price, and option type.
        3. Sets up the ByBit date format for extracting dates from the symbol.
        4. Initializes numpy arrays for storing option types, strike prices, and expiry dates.
        5. Sets up dictionaries to store daily, weekly, monthly, and quarterly expiry dates,
        each containing current, next, and next-to-next expiry dates.
        6. Initializes a DataFrame for holding expiry dates extracted from the primary DataFrame.
        7. Prepares DataFrames for storing extracted expiry-wise data for daily, weekly,
        monthly, and quarterly periods, each with current, next, and next-to-next subsets.
        """
        print("Class Variables Reinitialization Initiated")
        # Initialize the Blank DataFrame Object for storing the JSON Data into Pandas DataFrame Object
        self.dataframe: pd.DataFrame | None = None

        # We need to strip the Symbol into Different Elements with the following details:
        # Base Coin Expiry Dates, Strike Price & Option Type
        # https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior
        self.split_symbol: str | None = None

        # ByBit Date Format to Extract the Date from Splitting the Symbol Value
        self.ByBit_date_format = "%d%b%y"  # Date Format as (DD)(MMM)(YY)

        # Initialize the Default Numpy Array for Storing the Option_Type String Format
        self.option_type: np.ndarray | None = None
        # Initialize the Default Numpy Array for Storing the Strike_Price in Float64 Format
        self.strike_price: np.ndarray | None = None
        # Initialize the Default Numpy Array for Storing the Expiry Date in DateTime.Date Object Format
        self.expiry: np.ndarray | None = None

        # Dictionary Contains Current, Next & Next-to-Next Daily Expiry Dates
        # Initialize the Dictionary to Store the Daily Expiry Dates
        self.daily_expiry: dict | None = None

        # Initialize the Dictionary to Store the Weekly Expiry Dates
        # Dictionary Contains Current, Next & Next-to-Next Weekly Expiry Dates
        self.weekly_expiry: dict | None = None

        # Initialize the Dictionary to Store the Monthly Expiry Dates
        # Dictionary Contains Current, Next & Next-to-Next Monthly Expiry Dates
        self.monthly_expiry: dict | None = None

        # Initialize the Dictionary to Store the Quarterly Expiry Dates
        # Dictionary Contains Current, Next & Next-to-Next Quarterly Expiry Dates
        self.quarterly_expiry: dict | None = None

        # DateFrame for holding the Expiry Dates extracted from the Primary DataFrame
        self.expiry_dates: pd.DataFrame | None = None

        # Dictionary for Holding the Expiry Dates & its Existence
        self.expiry: dict | None = None

        # Initialize the Pandas DataFrame for holding the Extracted Expiry-Wise Data #
        # Initialize the Pandas DataFrame for holding the Current Daily Extracted Data
        self.current_daily: pd.DataFrame | None = None
        # Initialize the Pandas DataFrame for holding the Next Daily Extracted Data
        self.next_daily: pd.DataFrame | None = None
        # Initialize the Pandas DataFrame for holding the Next-TO-Next Daily Extracted Data
        self.next_to_next_daily: pd.DataFrame | None = None

        # Initialize the Pandas DataFrame for holding the Current Weekly Extracted Data
        self.current_weekly: pd.DataFrame | None = None
        # Initialize the Pandas DataFrame for holding the Next Weekly Extracted Data
        self.next_weekly: pd.DataFrame | None = None
        # Initialize the Pandas DataFrame for holding the Next-TO-Next Weekly-Extracted Data
        self.next_to_next_weekly: pd.DataFrame | None = None

        # Initialize the Pandas DataFrame for holding the Current Monthly Extracted Data
        self.current_monthly: pd.DataFrame | None = None
        # Initialize the Pandas DataFrame for holding the Next Monthly Extracted Data
        self.next_monthly: pd.DataFrame | None = None
        # Initialize the Pandas DataFrame for holding the Next-TO-Next Monthly Extracted Data
        self.next_to_next_monthly: pd.DataFrame | None = None

        # Initialize the Pandas DataFrame for holding the Current Quarterly Extracted Data
        self.current_quarterly: pd.DataFrame | None = None
        # Initialize the Pandas DataFrame for holding the Next Quarterly Extracted Data
        self.next_quarterly: pd.DataFrame | None = None
        # Initialize the Pandas DataFrame for holding the Next-TO-Next Quarterly Extracted Data
        self.next_to_next_quarterly: pd.DataFrame | None = None

    async def fetch_ByBit_ticker_data(self):
        """
        Fetch ByBit option ticker data asynchronously.

        This method reinitializes the class variables to ensure previous data is not stored
        and makes an async API call to fetch the full option data table from ByBit.
        The data is then converted into a pandas DataFrame for further processing and analysis.

        Returns:
            None
        """
        # Reinitialise the ByBitOptionData Class variables to ensure no previous Data is stored into Memory;
        # Need to Re-Utilizing these Same Variables to Fetch & Store the Next Set of Option Data

        start = time.perf_counter()

        await self._reinitialise_class_variables()

        # Make an Async API Call to fetch the Full Option Table
        async with httpx.AsyncClient() as client:
            # Await Data from GET Request
            r = await client.get(url=f"{self.api_url}" + f"{self.api_endpoint}", params=self.api_parameters)
            # Check if We have Received the Data or its Data Error
            if r.status_code == 200:
                # Print the Length of the Option Chain that we have Received from the API Call
                print(f"Tickers Data Retrieval Success : Data Count : {len(r.json()['result']['list'])}")
                # Convert the JSON Data into Pandas DataFrame for Further Processing
                self.dataframe = pd.json_normalize(data=r.json()['result']['list'])
            else:
                # Print the Error Message if No Data is Received
                print(f"Error in Retrieving the Tickers Data : Error Code : {r.status_code}")

        end = time.perf_counter()
        print(f"Data Fetching from ByBit API is Completed in : {(end - start) * 10 ** 3} ms \n")

    async def format_the_dataframe(self):
        """
        Format the dataframe by converting columns to appropriate data types and initializing auxiliary arrays.

        This method converts the columns of the dataframe to their respective required data types.
        It initializes three numpy arrays (`option_type`, `strike_price`, `expiry`) with default values,
        which will later be updated based on the processed symbol strings.

        Conversion Steps:
        1. Convert columns with appropriate data types.
        2. Initialize numpy arrays with default values.
        3. Split the 'symbol' column to extract and update `option_type`, `strike_price`, and `expiry`.


        Exceptions are handled
        to ensure
        that datatype conversion errors and string splitting issues are logged without interrupting the process.
        """
        start = time.perf_counter()

        # Convert the "DataFrame" dtype: "Object" into their Correct dtypes
        try:
            self.dataframe['symbol'] = self.dataframe['symbol'].astype(dtype="string")
        except Exception as e:
            print(f"Error in formatting 'symbol', Error Code : {e}")

        try:
            self.dataframe['bid1Price'] = self.dataframe['bid1Price'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'bid1Price', Error Code : {e}")

        try:
            self.dataframe['bid1Size'] = self.dataframe['bid1Size'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'bid1Size', Error Code : {e}")

        try:
            self.dataframe['bid1Iv'] = self.dataframe['bid1Iv'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'bid1Iv', Error Code : {e}")

        try:
            self.dataframe['ask1Price'] = self.dataframe['ask1Price'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'ask1Price', Error Code : {e}")

        try:
            self.dataframe['ask1Size'] = self.dataframe['ask1Size'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'ask1Size', Error Code : {e}")

        try:
            self.dataframe['ask1Iv'] = self.dataframe['ask1Iv'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'ask1Iv', Error Code : {e}")

        try:
            self.dataframe['lastPrice'] = self.dataframe['lastPrice'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'lastPrice', Error Code : {e}")

        try:
            self.dataframe['highPrice24h'] = self.dataframe['highPrice24h'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'highPrice24h', Error Code : {e}")

        try:
            self.dataframe['lowPrice24h'] = self.dataframe['lowPrice24h'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'lowPrice24h', Error Code : {e}")

        try:
            self.dataframe['markPrice'] = self.dataframe['markPrice'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'markPrice', Error Code : {e}")

        try:
            self.dataframe['underlyingPrice'] = self.dataframe['underlyingPrice'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'underlyingPrice', Error Code : {e}")

        try:
            self.dataframe['indexPrice'] = self.dataframe['indexPrice'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'indexPrice', Error Code : {e}")

        try:
            self.dataframe['markIv'] = self.dataframe['markIv'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'markIv', Error Code : {e}")

        try:
            self.dataframe['underlyingPrice'] = self.dataframe['underlyingPrice'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'underlyingPrice', Error Code : {e}")

        try:
            self.dataframe['openInterest'] = self.dataframe['openInterest'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'openInterest', Error Code : {e}")

        try:
            self.dataframe['turnover24h'] = self.dataframe['turnover24h'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'turnover24h', Error Code : {e}")

        try:
            self.dataframe['volume24h'] = self.dataframe['volume24h'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'volume24h', Error Code : {e}")

        try:
            self.dataframe['totalVolume'] = self.dataframe['totalVolume'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'totalVolume', Error Code : {e}")

        try:
            self.dataframe['totalTurnover'] = self.dataframe['totalTurnover'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'totalTurnover', Error Code : {e}")

        try:
            self.dataframe['delta'] = self.dataframe['delta'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'delta', Error Code : {e}")

        try:
            self.dataframe['gamma'] = self.dataframe['gamma'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'gamma', Error Code : {e}")

        try:
            self.dataframe['vega'] = self.dataframe['vega'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'vega', Error Code : {e}")

        try:
            self.dataframe['theta'] = self.dataframe['theta'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'theta', Error Code : {e}")

        try:
            self.dataframe['predictedDeliveryPrice'] = self.dataframe['predictedDeliveryPrice'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'predictedDeliveryPrice', Error Code : {e}")

        try:
            self.dataframe['change24h'] = self.dataframe['change24h'].astype(dtype="float64")
        except Exception as e:
            print(f"Error in formatting 'change24h', Error Code : {e}")

        # Initialize Options_Type Numpy Array, we need to fill it with actual Values after Splitting the Symbol String
        # We are Initialing Filling "UNKNOWN" as String Value, Later it will be replaced by "CALL" or "PUT"
        self.option_type = np.array(["UNKNOWN" for _ in range(len(self.dataframe))])

        # Initialize Strike_Price Numpy Array, we need to fill it with actual Values after Splitting the Symbol String
        # We are Initialing Filling "-5000" as Float64 Value, Later it will be replaced by Actual Strike Price
        self.strike_price = np.array([-5000.00 for _ in range(len(self.dataframe))])

        # Initialize Expiry_Date Numpy Array; we need to fill it with actual Values after Splitting the Symbol String
        # We are Initialing Filling "1-1-2000" as datetime Value, Later it will be replaced by Actual Expiry Dates
        self.expiry = np.array([datetime(year=2000, month=1, day=1).date() for _ in range(len(self.dataframe))])

        for i in range((len(self.dataframe))):
            # Try to Split the String using "-" as the Spliter
            try:
                self.split_symbol = self.dataframe.symbol[i].split("-")
                # Confirm the Data by Printing the Outcome as Logs
                # print(f"Symbol Data {i} was Split Correctly: {self.split_symbol}")
            # Catch the arising Exception if any, then Print it as Log
            except Exception as e:
                print(f"Error Occurred while splitting the Symbol Value : {self.dataframe.symbol[i]} ")
                print(f"Error Details : {e}")

            # Convert the Split String Value into Expiry Date in ByBit Specific Date Format
            # It will remain "1-1-2000" for error Cases
            try:
                self.expiry[i] = datetime.strptime(str(self.split_symbol[1]), self.ByBit_date_format).date()
                # print(f"Successfully Updated the {i} Expiry Date Type as: {self.expiry[i]}")
            # Catch the arising Exception if any, then Print it as Log
            except Exception as e:
                print(f"Error Details : {e}")
                print(f"Error Occurred while Updating the Options Expiry Date : {self.split_symbol[1]} ")
                print(f"Finally processing Expiry Date as : {self.expiry[i]}")

            # Convert the Split String Value into Option Strike Price in Float64 dtype
            # It will remain "-5000.00" for error Cases
            try:
                self.strike_price[i] = float(self.split_symbol[2])
                # print(f"Successfully Updated the {i} Strike Price as: {self.strike_price[i]}")
            # Catch the arising Exception if any, then Print it as Log
            except Exception as e:
                print(f"Error Details : {e}")
                print(f"Error Occurred while Updating the Options Strike Price : {self.split_symbol[2]} ")
                print(f"Finally processing Strike_Price as : {self.strike_price[i]}")

            # Convert the Split String Value into Option Type: Either "PUT" or "CALL"
            # It will remain "UNKNOWN" for error Cases
            try:
                if self.split_symbol[3] == "P":
                    self.option_type[i] = "PUT"
                elif self.split_symbol[3] == "C":
                    self.option_type[i] = "CALL"

                # print(f"Successfully Updated the {i} Option Type as: {self.option_type[i]}")
            # Catch the arising Exception if any, then Print it as Log
            except Exception as e:
                print(f"Error Details : {e}")
                print(f"Error Occurred while Updating the Options Type : {self.split_symbol[3]} ")
                print(f"Finally processing Options Type as : {self.option_type[i]}")

        # Merge the Numpy Array of "Option Type" with Primary DataFrame
        self.dataframe['option_type'] = self.option_type

        # Merge the Numpy Array of "Strike_Price" with Primary DataFrame
        self.dataframe['strike_price'] = self.strike_price

        # Merge the Numpy Array of "Expiry Dates" with Primary DataFrame
        self.dataframe['expiry'] = self.expiry

        # Confirm the dtype of the Primary DataFrame Column "option_type" as String Object
        self.dataframe['option_type'] = self.dataframe['option_type'].astype(dtype="string")

        # Confirm the dtype of the Primary DataFrame Column "strike_price" as Float64 Object
        self.dataframe['strike_price'] = self.dataframe['strike_price'].astype(dtype="float64")

        # Confirm the dtype of the Primary DataFrame Column "expiry" as DataTime Object in Specific Date Format
        # TODO: Removing the DateTime Conversion so that we can process the filtering using it as Object Variable
        # TODO: To Process this Object we need always change the Field into Numpy Array then test for validation
        self.dataframe['expiry'] = pd.to_datetime(self.dataframe['expiry'], format='%y%m%d')

        # Sort the DataFrame using the Column "expiry" & "strike_price" in ascending Order Permanently
        self.dataframe.sort_values(by=["expiry", "strike_price"], ascending=True, inplace=True)

        # Convert the "Symbol" Column into DataFrame Index, then Drop the Duplicate "Symbol" Column Permanently
        # self.dataframe.set_index(keys='symbol', drop=True, inplace=True)
        # Don't Drop the "symbol" key as it's required in the Order management, so replacing "drop=True" to False
        self.dataframe.set_index(keys='symbol', drop=False, inplace=True)

        end = time.perf_counter()
        print(f"Formatting & Processing of DataFrame is Completed in : {(end - start) * 10 ** 3} ms \n")

    async def segregate_options_expiry(self,
                                       weekly_count_inputs: int = 5,
                                       monthly_count_inputs: int = 4,
                                       quarterly_count_inputs: int = 5):
        """
        Args:
            weekly_count_inputs: Number of weekly expiry dates to be processed.
            monthly_count_inputs: Number of monthly expiry dates to be processed.
            quarterly_count_inputs: Number of quarterly expiry dates to be processed.

        """
        start = time.perf_counter()

        # Extracting Expiry Dates from the Primary DataFrame Object
        self.expiry_dates = self.dataframe['expiry'].drop_duplicates().sort_values().copy()

        # Initialize the ByBit Expiry Class to compute the Expiry Dates
        compute_bybit_expiry = ByBitExpiry()

        # Fetch the Daily, Weekly, Monthly & Quarterly Expiry Dates Dictionary
        self.expiry = await (compute_bybit_expiry.create_options_expiry(expiry_dataframe=self.expiry_dates,
                                                                        weekly_count=weekly_count_inputs,
                                                                        monthly_count=monthly_count_inputs,
                                                                        quarterly_count=quarterly_count_inputs))

        # Compute the Current, Next & Next-to-Next Daily DataFrame
        await self._compute_daily_dataframe()

        # Compute the Current, Next & Next-to-Next Weekly Expiry DataFrame
        await self._compute_weekly_expiry()

        # Compute the Current, Next & Next-to-Next Monthly Expiry DataFrame
        await self._compute_monthly_expiry()

        # Compute the Current, Next & Next-to-Next Monthly Quarterly DataFrame
        await self._compute_quarterly_expiry()

        end = time.perf_counter()
        print(f"Segregate Options data as per Expiry is Completed in : {(end - start) * 10 ** 3} ms \n")

        # Return the Expiry-Wise Dataframes in Tuples
        return (self.current_daily, self.next_daily, self.next_to_next_daily,
                self.current_weekly, self.next_weekly, self.next_to_next_weekly,
                self.current_monthly, self.next_monthly, self.next_to_next_monthly,
                self.current_quarterly, self.next_quarterly, self.next_to_next_quarterly, self.expiry)

    async def _compute_daily_dataframe(self):
        """
        Asynchronously computes the DataFrame for daily expiry options.

        This method filters the primary DataFrame based on expiry dates specified in the
        daily expiry dictionary and extracts three DataFrames:
        current daily, next daily, and next-to-next daily.

        Retrieves:
            current_daily: DataFrame filtered by the current daily expiry date.
            next_daily: DataFrame filtered by the next daily expiry date.
            next_to_next_daily: DataFrame filtered by the next-to-next daily expiry date.

        In case of errors during these operations, the method logs the exception details.

        Exceptions:
            Handles and logs any exceptions that occur while processing each daily DataFrame extraction.
        """
        print(f"Computing the Daily Expiry DataFrame ")
        # Try if we can extract the Current_Daily Options Chain from the Primary DataFrame
        # Filtering the DataFrame using Date from Daily_Expiry Dictionary
        try:
            self.current_daily = self.dataframe[
                self.dataframe["expiry"] == str(self.expiry["daily"]["current"]["date"])].copy()
            print(f"Daily Expiry DataFrame Length : {len(self.current_daily)}, for Date : "
                  f"{str(self.expiry["daily"]["current"]["date"])}")
        # Catch the Exception Error & Print out the same for Logging Purposes
        except Exception as e:
            print(f"Error in Processing the Daily Expiry {str(self.expiry["daily"]["current"]["date"])}"
                  f" : Error Details : {e}")
        # Try if we can extract the Next_Daily Options Chain from the Primary DataFrame
        # Filtering the DataFrame using Date from Daily_Expiry Dictionary

        try:
            self.next_daily = self.dataframe[self.dataframe["expiry"] == str(self.expiry["daily"]["next"]["date"])].copy()
            print(f"Next Daily Expiry DataFrame Length : {len(self.next_daily)}, for Date : "
                  f"{str(self.expiry["daily"]["next"]["date"])}")
        # Catch the Exception Error & Print out the same for Logging Purposes
        except Exception as e:
            print(f"Error in Processing the Next Daily Expiry {str(self.expiry["daily"]["next"]["date"])} : "
                  f"Error Details : {e}")

        # Try if we can extract the Next-to-Next_Daily Options Chain from the Primary DataFrame
        # Filtering the DataFrame using Date from Daily_Expiry Dictionary
        try:
            self.next_to_next_daily = self.dataframe[self.dataframe["expiry"] == str(self.expiry["daily"]
                                                                                     ["next_to_next"]["date"])].copy()
            print(f"Next to Next Daily Expiry DataFrame Length : {len(self.next_to_next_daily)}, for Date : "
                  f"{str(self.expiry["daily"]["next_to_next"]["date"])}")
        # Catch the Exception Error & Print out the same for Logging Purposes
        except Exception as e:
            print(f"Error in Processing Next to Next Daily Expiry {str(self.expiry["daily"]["next_to_next"]["date"])}: "
                  f"Error Details : {e}")

    async def _compute_weekly_expiry(self):
        """
        Compute the weekly expiry data and store in corresponding class members.

        This method performs the following tasks:
        1. Computes the current weekly expiry DataFrame by filtering the primary DataFrame
           using the current weekly expiry date.
           - Logs the length of the resulting DataFrame.
           - Logs any errors encountered during this operation.

        2. Computes the next weekly expiry DataFrame by filtering the primary DataFrame
           using the next weekly expiry date.
           - Logs the length of the resulting DataFrame.
           - Logs any errors encountered during this operation.

        3. Computes the next-to-next weekly expiry DataFrame by filtering the primary DataFrame
           using the next-to-next weekly expiry date.
           - Logs the length of the resulting DataFrame.
           - Logs any errors encountered during this operation.
        """
        print(f"Computing the Weekly Expiry DataFrame ")
        # Try if we can extract the Current_Weekly Options Chain from the Primary DataFrame
        # Filtering the DataFrame using Date from Weekly_Expiry Dictionary
        try:
            self.current_weekly = self.dataframe[self.dataframe["expiry"] == str(self.expiry["weekly"]
                                                                                 ["current"]["date"])].copy()
            print(f"Weekly Expiry DataFrame Length : {len(self.current_weekly)}, for Date : "
                  f"{str(self.expiry["weekly"]["current"]["date"])}")
        # Catch the Exception Error & Print out the same for Logging Purposes
        except Exception as e:
            print(f"Error in Processing the Weekly Expiry {str(self.expiry["weekly"]["current"]["date"])} : "
                  f"Error Details : {e}")

        # Try if we can extract the Next_Weekly Options Chain from the Primary DataFrame
        # Filtering the DataFrame using Date from Weekly_Expiry Dictionary
        try:
            self.next_weekly = self.dataframe[self.dataframe["expiry"] == str(
                self.expiry["weekly"]["next"]["date"])].copy()
            print(f"Next Weekly Expiry DataFrame Length : {len(self.next_weekly)}, for Date : "
                  f"{str(self.expiry["weekly"]["next"]["date"])}")
        # Catch the Exception Error & Print out the same for Logging Purposes
        except Exception as e:
            print(f"Error in Processing the Next Weekly Expiry {str(self.expiry["weekly"]["next"]["date"])} : "
                  f"Error Details : {e}")

        # Try if we can extract the next_to_next_weekly Options Chain from the Primary DataFrame
        # Filtering the DataFrame using Date from Weekly_Expiry Dictionary
        try:
            self.next_to_next_weekly = self.dataframe[self.dataframe["expiry"] == str(self.expiry["weekly"]
                                                                                      ["next_to_next"]["date"])].copy()
            print(f"Next to Next Weekly Expiry DataFrame Length : {len(self.next_to_next_weekly)}, for Date : "
                  f"{str(self.expiry["weekly"]["next_to_next"]["date"])}")
        # Catch the Exception Error & Print out the same for Logging Purposes
        except Exception as e:
            print(f"Error in Processing the Next to Next Weekly Expiry {str(self.expiry["weekly"]
                                                                            ["next_to_next"]["date"])}"
                  f" : Error Details : {e}")

    async def _compute_monthly_expiry(self):
        """
        Compute the monthly expiry dataframes for the current, next, and next-to-next months from the primary dataframe.

        This method performs the following steps:
        1. Attempts to filter the primary dataframe for the current month's expiry date
        and assigns it to self.current_monthly.
           Logs the length of the resulting dataframe and the date used for filtering.
           If an error occurs during this process, it logs the error details.
        2. Attempts to filter the primary dataframe for the next month's expiry date
        and assigns it to self.next_monthly.
           Logs the length of the resulting dataframe and the date used for filtering.
           If an error occurs during this process, it logs the error details.
        3. Attempts to filter the primary dataframe for the next-to-next month's expiry date
        and assigns it to self.next_to_next_monthly.
           Logs the length of the resulting dataframe and the date used for filtering.
           If an error occurs during this process, it logs the error details.
        """
        print(f"Computing the Monthly Expiry DataFrame ")
        # Try if we can extract the Current_Monthly Options Chain from the Primary DataFrame
        # Filtering the DataFrame using Date from Monthly_Expiry Dictionary
        try:
            self.current_monthly = self.dataframe[self.dataframe["expiry"] == str(self.expiry["monthly"]
                                                                                  ["current"]["date"])].copy()
            print(f"Monthly Expiry DataFrame Length : {len(self.current_monthly)}, for Date :"
                  f" {str(self.expiry["monthly"]["current"]["date"])}")
        # Catch the Exception Error & Print out the same for Logging Purposes
        except Exception as e:
            print(f"Error in Processing the Current Monthly Expiry {str(self.expiry["monthly"]["current"]["date"])} : "
                  f"Error Details : {e}")

        # Try if we can extract the Next_Monthly Options Chain from the Primary DataFrame
        # Filtering the DataFrame using Date from Monthly_Expiry Dictionary
        try:
            self.next_monthly = self.dataframe[
                self.dataframe["expiry"] == str(self.expiry["monthly"]["next"]["date"])].copy()
            print(f"Next Monthly Expiry DataFrame Length : {len(self.next_monthly)}, for Date :"
                  f" {str(self.expiry["monthly"]["next"]["date"])}")
        # Catch the Exception Error & Print out the same for Logging Purposes
        except Exception as e:
            print(f"Error in Processing the Next Monthly Expiry {str(self.expiry["monthly"]["next"]["date"])} : "
                  f"Error Details : {e}")

        # Try if we can extract the Next_to_Next_Monthly Options Chain from the Primary DataFrame
        # Filtering the DataFrame using Date from Monthly_Expiry Dictionary
        try:
            self.next_to_next_monthly = self.dataframe[self.dataframe["expiry"] == str(self.expiry["monthly"]
                                                                                       ["next_to_next"]["date"])].copy()
            print(f"Monthly Expiry DataFrame Length : {len(self.next_to_next_monthly)}, for Date :"
                  f" {str(self.expiry["monthly"]["next_to_next"]["date"])}")
        # Catch the Exception Error & Print out the same for Logging Purposes
        except Exception as e:
            print(f"Error in Processing Next to Next Monthly Expiry {str(self.expiry["monthly"]
                                                                         ["next_to_next"]["date"])}"
                  f" : Error Details : {e}")

    async def _compute_quarterly_expiry(self):
        """
        Asynchronously computes and assigns quarterly expiry data frames for the current,
        next, and next-to-next quarters.
        This function uses the expiry dates from the expiry dictionary
        to filter the primary options chain data frame and
        create separate data frames for each of the specified quarters.

        Each step tries to create a data frame for a specific quarter by filtering the primary data frame
        based on the relevant expiry date.
        It logs the length of the resulting data frame or an error message if an
        exception occurs.


        Exceptions:
            Catches and logs any exceptions that occur during the data frame filtering process.
        """
        print(f"Computing the Quarterly Expiry DataFrame ")

        # Try if we can extract the Current_Quarterly Options Chain from the Primary DataFrame
        # Filtering the DataFrame using Date from Quarterly_Expiry Dictionary
        try:
            self.current_quarterly = self.dataframe[self.dataframe["expiry"] == str(self.expiry["quarterly"]
                                                                                    ["current"]["date"])].copy()

            print(f"Quarterly Expiry DataFrame Length : {len(self.current_quarterly)}, for Date : "
                  f"{str(self.expiry["quarterly"]["current"]["date"])}")
        # Catch the Exception Error & Print out the same for Logging Purposes
        except Exception as e:
            print(f"Error in Processing Currently Quarterly Expiry {str(self.expiry["quarterly"]["current"]["date"])}"
                  f" : Error Details : {e}")

        # Try if we can extract the Current_Quarterly Options Chain from the Primary DataFrame
        # Filtering the DataFrame using Date from Quarterly_Expiry Dictionary
        try:
            self.next_quarterly = self.dataframe[self.dataframe["expiry"] == str(self.expiry["quarterly"]
                                                                                 ["next"]["date"])].copy()
            print(f"Next Quarterly Expiry DataFrame Length : {len(self.next_quarterly)}, for Date :"
                  f" {str(self.expiry["quarterly"]["next"]["date"])}")
        # Catch the Exception Error & Print out the same for Logging Purposes
        except Exception as e:
            print(f"Error in Processing Next Quarterly Expiry {str(self.expiry["quarterly"]["next"]["date"])} : "
                  f"Error Details : {e}")

        # Try if we can extract the Current_Quarterly Options Chain from the Primary DataFrame
        # Filtering the DataFrame using Date from Quarterly_Expiry Dictionary
        try:
            self.next_to_next_quarterly = self.dataframe[self.dataframe["expiry"] == str(
                self.expiry["quarterly"]["next_to_next"]["date"])].copy()

            print(f"Next to Next Quarterly Expiry DataFrame Length : {len(self.next_to_next_quarterly)}, for Date :"
                  f" {str(self.expiry["quarterly"]["next_to_next"]["date"])}")
        # Catch the Exception Error & Print out the same for Logging Purposes
        except Exception as e:
            print(f"Error in Processing the Next to Next Quarterly Expiry "
                  f"{str(self.expiry["quarterly"]["next_to_next"]["date"])} : Error Details : {e}")
