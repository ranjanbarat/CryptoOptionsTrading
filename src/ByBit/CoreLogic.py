"""
We need to Identify the Correct Options Positions Depending on the Delta's

Choosing the Correct Options Trade Positions for ALL Expires:
NOTE: Check with Latest Options Positions: if the Position for the current Expiry exists, then take NO Action

1. Find the CALL Options with Delta Less Than 0.7 & PUT Options with Delta Greater than 0.7
2. Ensure that BID Exists for these Options : BID Is Not Blank
3. Match the Delta for Both CALL & PUT, Means if we Opt for a CALL Option with 0.06 Delta,
    we should have PUT Options with -0.06 Delta, NOTE: "Mismatch Tolerance Limit is of ~0.01 only"
4. Send the Selected "Symbols" for Order Processing
5. DON'T TAKE ANY POSITIONS IF THE REQUIRED DELTA PARAMETERS ARE NOT MET, WAIT FOR THE DATA FETCHING CYCLE

DELTA HEDGING:
1. Fetch the Current List of ALL Positions
2. Compute the Delta of these Positions including USDC Perpetual Futures
3. Check if it's within the Range of +/- 0.10 Delta,
    if Yes, then Ignore, If Not, then Re-Balance the Delta using USDC Perpetual Futures
    Compute the Required USDC Futures Position to Balance the Delta & Initiate the Order Processing for the Same
"""
import numpy as np
import pandas as pd

from .ByBitAPI import ByBitAPI
from .ByBitOptionData import ByBitOptionData
from .RecommendOptionPosition import recommend_option_position


class CoreLogic(object):
    """
    Core Logic Class for the ByBit Trading Bot
    """
    def __init__(self, api_url: str = "https://api-demo.bybit.com",  # API URL
                 api_endpoint: str = "/v5/market/tickers",  # Endpoint URI
                 api_keys: str | None = "6TzoOeOuyIDiJN9cOu",  # API Keys
                 api_secret: str | None = "JbTE9Z5OFFyxcw1mzqf6hLIpmGgt8dONmlSE",  # API Secret
                 baseCoin: str | None = None,  # BaseCoin neets be either BTC, ETH, SOL Only
                 settleCoin:str | None = None,  # Settle Coin
                 default_quantity: np.float64 = 1.00,  # Default Short Quantity

                 daily: bool = False,  # Play on Daily Options: True or False
                 weekly: bool = False,  # Play on Weekly Options: True or False
                 monthly: bool = False,  # Play on Monthly Options: True or False
                 quarterly: bool = False,  # Play on Quarterly Options: True or False

                 delta_value: float = 0.07,  # Delta Value beyond which we would take our Positions
                 min_bid_price: float = 0.01,  # Minumum Bid Price to eliminate the Blank Bid Strikes
                 initial_mark_price_diff: float = 0.05,  # The Difference Between Mark & Bid Price 0.05 means 5%
                 max_mark_price_diff: float = 0.3,  # Maximum Difference between Mark & Bid Price
                 mark_price_diff_steps: float = 0.05,  # Mark & Ask Difference Percentage Incremental Step Size
                 # Tolarated Delta Deavation Limit, after which we Negate the Delta Deavation
                 # by hedging with Perpectual Futuers
                 daily_delta_limit: float = 0.10,  # Daily Delta Deviation Limit
                 weekly_delta_limit: float = 0.10,  # Weekly Delta Deviation Limit
                 monthly_delta_limit: float = 0.10,  # Monthly Delta Deviation Limit
                 quarterly_delta_limit: float = 0.10,  # Quarterly Delta Deviation Limit
                ):
        """
        :parameter api_url :type :string : ByBit API Url to fetch the details
        :parameter api_endpoint :type :string : Not Required // Need to remove
        :parameter api_keys :type :string : API Key for the ByBit Account
        :parameter api_secret :type :string : API Secret for the ByBit API

        :parameter baseCoin :type :string : We need the Coin Type Only "BTC", "ETH", ""
        :parameter default_quantity :type :float : Default Options Short Quantity

        :parameter daily :type: Bool : To Indicate if we are taking Daily Options Position
        :parameter weekly :type: Bool : To Indicate if we are taking Weekly Options Position
        :parameter monthly :type: Bool : To Indicate if we are taking Monthly Options Position
        :parameter quarterly :type: Bool : To Indicate if we are taking Quarterly Options Position

        :parameter delta_value :type :float : Delta Value beyond which we would take our Positions
        :parameter min_bid_price :type :float : Minumum Bid Price to eliminate the Blank Bid Strikes
        :parameter initial_mark_price_diff :type :float : The Difference Between Mark & Bid Price 0.05 means 5%
        :parameter max_mark_price_diff :type :float : Maximum Difference between Mark & Bid Price
        :parameter mark_price_diff_steps :type :float : Mark & Ask Difference Percentage Incremental Step Size

        # Tolarated Delta Deavation Limit, after which we Negate the Delta Deavation
        # by hedging with Perpectual Futuers

        :parameter daily_delta_limit :type :float # Daily Delta Deviation Limit
        :parameter weekly_delta_limit :type :float = 0.10, # Weekly Delta Deviation Limit
        :parameter monthly_delta_limit :type :float = 0.10, # Monthly Delta Deviation Limit
        :parameter quarterly_delta_limit :type :float = 0.10, # Quarterly Delta Deviation Limit

        """
        # USDC Perpetual : BTCPERP, ETHPERP, SOLPERP
        if baseCoin == "BTC":
            self.api_params = {'category': "option",
                               'baseCoin': "BTC"}
        elif baseCoin == "ETH":
            self.api_params = {'category': "option",
                               'baseCoin': "ETH"}
        elif baseCoin == "SOL":
            self.api_params = {'category': "option",
                               'baseCoin': "SOL"}
        else:
            self.api_params = None

        """
            optionPeriod
            BTC: 7,14,21,30,60,90,180,270days
            ETH: 7,14,21,30,60,90,180,270days
            SOL: 7,14,21,30,60,90days
        """
        self.baseCoin = baseCoin
        self.settleCoin = settleCoin
        self.api_url = api_url
        self.api_endpoint = api_endpoint
        self.api_keys = api_keys
        self.api_secret = api_secret

        self.quantity = default_quantity
        
        self.daily = daily
        self.weekly = weekly
        self.monthly = monthly
        self.quarterly = quarterly

        self.current_daily = None
        self.next_daily = None
        self.next_to_next_daily = None

        self.current_weekly = None
        self.next_weekly = None
        self.next_to_next_weekly = None

        self.current_monthly = None
        self.next_monthly = None
        self.next_to_next_monthly = None

        self.current_quarterly = None
        self.next_quarterly = None
        self.next_to_next_quarterly = None

        self.expiry: dict | None = None
        self.ByBitAPI = ByBitAPI(default_quantity = self.quantity,
                                 api_url = self.api_url,
                                 api_key = self.api_keys,
                                 api_secret = self.api_secret,
                                 baseCoin=self.baseCoin,
                                 settleCoin=self.settleCoin)

        self.delta_value = delta_value
        self.min_bid_price = min_bid_price
        self.initial_mark_price_diff = initial_mark_price_diff
        self.max_mark_price_diff = max_mark_price_diff
        self.mark_price_diff_steps = mark_price_diff_steps

        # Delta Allocation for Hedging
        self.daily_delta_limit = daily_delta_limit  # Daily Delta Deviation Limit
        self.weekly_delta_limit = weekly_delta_limit  # Weekly Delta Deviation Limit
        self.monthly_delta_limit = monthly_delta_limit  # Monthly Delta Deviation Limit
        self.quarterly_delta_limit = quarterly_delta_limit  # Quarterly Delta Deviation Limit

        self.position_df: pd.DataFrame | None = None
        self.position_json_data: dict | None = None

        self.PerpFutures_df: pd.DataFrame | None = None
        self.PerpFutures_json_data: dict | None = None

        self.expiry_df: pd.DataFrame | None = None
        self.delta_risk_multiplier:float = 2.0
        self.PerpFutures_position:float = 0.0
        self.total_perpFutures:float = 0.0
        self.total_delta_risk_magnitude:float = 0.0
        self.required_PerfFutures_position: float = 0.0

    async def core_logic_computation(self):
        ByBit = ByBitOptionData(api_url=self.api_url, api_endpoint=self.api_endpoint, api_parameters=self.api_params)
        # Fetch the Ticker Data
        await ByBit.fetch_ByBit_ticker_data()
        await ByBit.format_the_dataframe()

        (self.current_daily, self.next_daily, self.next_to_next_daily,
         self.current_weekly, self.next_weekly, self.next_to_next_weekly,
         self.current_monthly, self.next_monthly, self.next_to_next_monthly,
         self.current_quarterly, self.next_quarterly, self.next_to_next_quarterly,
         self.expiry) = await ByBit.segregate_options_expiry()

        # Fetch the Current Positions Data
        self.position_json_data = await self.ByBitAPI.get_option_positions()
        self.position_df = await self.ByBitAPI.format_option_position_dataframe()

        # Update the Expiry Dates in to the Expiry Dictionary
        await self.set_expiry_delta_in_dictionary()

        # Check the Available Expiry Positions, It will take Process NEW Positions if it doesn't exist
        await self.check_existing_position_and_take_new_positions()

        # Check if the Delta Computation is correctly done for the Position DataFrame
        await self.check_delta_computation()

        # Update the Delta Limits into the Positions DataFrame & Compute the Delta Hedging Requirements
        await self.set_delta_limit_and_compute_hedging()

        # Fetch the Perpectual Futures Position AND
        # Hedge the Option Position with Required Perpectual Futures Position AND
        # Compute & Execute Delta Hedging
        await self.compute_delta_hedging()

    async def compute_delta_hedging(self):

        excess_columns = ["symbol", "avgPrice", "delta", "theta", "positionValue", "unrealisedPnl", "size",
                          "markPrice","createdTime", "seq", "updatedTime","side","curRealisedPnl","positionStatus",
                          "gamma","vega", "option_type","strike_price" ]
        # Reinitialize the Perpectual Futures & Other Variables to ZERO
        self.PerpFutures_position: float = 0.0
        self.total_perpFutures: float = 0.0
        self.total_delta_risk_magnitude: float = 0.0

        try :
            self.expiry_df = self.position_df.drop(columns=excess_columns).copy()
            self.expiry_df.drop_duplicates("expiry", inplace=True, ignore_index=True)
            # Compute the Perpectual Futures Requirements, Total of All Expiries
            # Compute the Total Hedging Requirements for the Open Options Positions
            self.total_perpFutures = round(sum(
                self.expiry_df.loc[self.expiry_df.hedging_required == True, "PerpFutureQty"]), 6)
            print(f"Totol Perpectual Futures Requirements : {self.total_perpFutures}")

        except Exception as e:
            print(f"Error Occoured While computing the Expiry DataFrame, Error Details : {e}")

        # Check the Risk Parameters for Individual Expiries
        ## Checking the Risk Magnitude for Daily Expiry
        try:
            daily_delta_risk = round(sum(self.expiry_df[self.expiry_df.expiry_type == "daily"].PerpFutureQty), 6)
            daily_delta_risk_magnitude = self.delta_risk_multiplier * self.daily_delta_limit * self.quantity

            if daily_delta_risk > daily_delta_risk_magnitude:
                self.total_delta_risk_magnitude = self.total_delta_risk_magnitude + daily_delta_risk
                print(f"Attention !!!! : Daily Delta Risk Paramater Exceeded : {daily_delta_risk_magnitude}, "
                      f"Risk Magnitude : {daily_delta_risk}, Total Risk Magnitude : {self.total_delta_risk_magnitude}")
            else:
                print(f"Daily Delta Risk Paramater is within Limit : {daily_delta_risk} ",
                      f"Risk Magnitude : {daily_delta_risk_magnitude}, "
                      f"Total Risk Magnitude : {self.total_delta_risk_magnitude}")
        except Exception as e:
            print(f"Error Occoured While Computing the Daily Delta Risk magnitude, Error Details : {e}")

        ## Checking the Risk Magnitude for Weekly Expiry
        try:
            weekly_delta_risk = round(sum(self.expiry_df[self.expiry_df.expiry_type == "weekly"].PerpFutureQty), 6)
            weekly_delta_risk_magnitude = self.delta_risk_multiplier * self.weekly_delta_limit * self.quantity

            if weekly_delta_risk > weekly_delta_risk_magnitude:
                self.total_delta_risk_magnitude = self.total_delta_risk_magnitude + weekly_delta_risk

                print(f"Attention !!!! : Weekly Delta Risk Paramater Exceeded : {weekly_delta_risk}, "
                      f"Risk Magnitude : {weekly_delta_risk_magnitude}, "
                      f"Total Risk Magnitude : {self.total_delta_risk_magnitude}")
            else:
                print(f"Weekly Delta Risk Paramater is within Limit : {weekly_delta_risk} ",
                      f"Risk Magnitude : {weekly_delta_risk_magnitude}, "
                      f"Total Risk Magnitude : {self.total_delta_risk_magnitude}")
        except Exception as e:
            print(f"Error Occoured While Computing the Weekly Delta Risk magnitude, Error Details : {e}")

        ## Checking the Risk Magnitude for Monthly Expiry
        try :
            monthly_delta_risk = round(sum(self.expiry_df[self.expiry_df.expiry_type == "monthly"].PerpFutureQty), 6)
            monthly_delta_risk_magnitude = self.delta_risk_multiplier * self.monthly_delta_limit * self.quantity

            if monthly_delta_risk > monthly_delta_risk_magnitude:
                self.total_delta_risk_magnitude = self.total_delta_risk_magnitude + monthly_delta_risk

                print(f"Attention !!!! : Monthly Delta Risk Paramater Exceeded : {monthly_delta_risk}, "
                      f"Risk Magnitude : {monthly_delta_risk_magnitude}, "
                      f"Total Risk Magnitude : {self.total_delta_risk_magnitude}")
            else:
                print(f"Monthly Delta Risk Paramater is within Limit : {monthly_delta_risk} ",
                      f"Risk Magnitude : {monthly_delta_risk_magnitude}, "
                      f"Total Risk Magnitude : {self.total_delta_risk_magnitude}")
        except Exception as e:
            print(f"Error Occoured While Computing the Monthly Delta Risk magnitude, Error Details : {e}")

        ## Checking the Risk Magnitude for Quarterly Expiry
        try:
            quarterly_delta_risk = round(sum(self.expiry_df[self.expiry_df.expiry_type == "quarterly"].PerpFutureQty), 6)
            quarterly_delta_risk_magnitude = self.delta_risk_multiplier * self.quarterly_delta_limit * self.quantity

            if quarterly_delta_risk > quarterly_delta_risk_magnitude:
                self.total_delta_risk_magnitude = self.total_delta_risk_magnitude + quarterly_delta_risk
                print(f"Attention !!!! : Quarterly Delta Risk Paramater Exceeded : {quarterly_delta_risk}, "
                      f"Risk Magnitude : {quarterly_delta_risk_magnitude}, "
                      f"Total Risk Magnitude : {self.total_delta_risk_magnitude}")
            else:
                print(f"Quarterly Delta Risk Paramater is within Limit : {quarterly_delta_risk} ",
                      f"Risk Magnitude : {quarterly_delta_risk_magnitude}, "
                      f"Total Risk Magnitude : {self.total_delta_risk_magnitude}")
        except Exception as e:
            print(f"Error Occoured While Computing the Quarterly Delta Risk magnitude, Error Details : {e}")

        # Fetch the Perpectuals Futures Position and Compute the Exact Position Size Value
        try:
            self.PerpFutures_json_data = await self.ByBitAPI.get_PerpFutures_Position()
            if len(self.PerpFutures_json_data.json()["result"]["list"]) > 0:
                self.PerpFutures_df = await self.ByBitAPI.format_perpfutures()

                if (self.PerpFutures_df.loc[0, "side"] == "Buy") or (self.PerpFutures_df.loc[0, "side"] == "BUY"):
                    self.PerpFutures_position = float(self.PerpFutures_df.loc[0, "size"])
                elif (self.PerpFutures_df.loc[0, "side"] == "Sell") or (self.PerpFutures_df.loc[0, "side"] == "Sell"):
                    self.PerpFutures_position = float(self.PerpFutures_df.loc[0, "size"]) * (-1.0)

                print(f"Existing Perpetual Futures Position Size : {self.PerpFutures_position}")

            else:
                print(f"No {self.baseCoin}, Perpectual Futires position Exists, "
                      f"Setting Value for 'PerpFutures_position'  :  {self.PerpFutures_position}")
        except Exception as e:
            print(f"Error in Fetcting the Perpectual Futures Position Error Details : {e}")

        # Compute the Perpectual Future Position Requirements AND Take the Required Position
        try:
            if self.total_perpFutures >= self.total_delta_risk_magnitude:
                # Take the Perpectual Futuers Position
                self.required_PerfFutures_position = round(self.total_perpFutures, 3)

            elif self.total_perpFutures < self.total_delta_risk_magnitude:
                # Take the Perpectual Futuers Position
                self.required_PerfFutures_position = round(self.total_delta_risk_magnitude, 3)

            # Take the Perpectual Futures Positions Based on the Computations
            await self._check_PerpFutures_Quantity_and_take_PerpFutures_position(
                PerpFut_Position=self.PerpFutures_position,
                PerpFut_Requirement=self.required_PerfFutures_position)

        except Exception as e:
            print(f"Error in Computing & {self.baseCoin} Taking Perpectual Futuers Position  the Perpectual Futures "
                  f"Error Details : {e}")

    async def _check_PerpFutures_Quantity_and_take_PerpFutures_position(self, PerpFut_Position,
                                                                        PerpFut_Requirement):

        adjustment_quantity = 0.0
        if PerpFut_Position == 0.0:

            if PerpFut_Requirement == 0:
                print(f"No PerpFuture Position is Required : {PerpFut_Requirement}, No Order Processing is Required")

            elif PerpFut_Requirement > 0:
                print(f"Placing a 'BUY' Order for PerpFuture with following Quantity : {PerpFut_Requirement}")
                await self.ByBitAPI.create_PerpFutures_Order(direction="BUY", quantity=abs(PerpFut_Requirement))

            elif PerpFut_Requirement < 0:
                print(f"Placing a 'SELL' Order for PerpFuture with following Quantity : {PerpFut_Requirement}")
                await self.ByBitAPI.create_PerpFutures_Order(direction="SELL", quantity=abs(PerpFut_Requirement))

        elif PerpFut_Position > 0.0:

            if PerpFut_Requirement == 0:
                adjustment_quantity = round((PerpFut_Requirement - PerpFut_Position), 3)

                print(
                    f"Since Existing PerpFuture Position Exist : {PerpFut_Position}, and PerpFuture Requirements : {PerpFut_Requirement},"
                    f"So Futures Adjustment 'SELL' Quantity is : {adjustment_quantity}")
                await self.ByBitAPI.create_PerpFutures_Order(direction="SELL", quantity=abs(adjustment_quantity))

            elif PerpFut_Requirement > 0:

                if PerpFut_Position > PerpFut_Requirement:
                    adjustment_quantity = -round((PerpFut_Position - PerpFut_Requirement), 3)
                elif PerpFut_Position < PerpFut_Requirement:
                    adjustment_quantity = round((PerpFut_Requirement - PerpFut_Position), 3)

                if adjustment_quantity < 0:

                    print(
                        f"Since Existing PerpFuture Position Exist : {PerpFut_Position}, and PerpFuture Requirements : {PerpFut_Requirement},"
                        f"So Futures Adjustment 'SELL' Quantity is : {adjustment_quantity}")

                    await self.ByBitAPI.create_PerpFutures_Order(direction="SELL", quantity=abs(adjustment_quantity))

                else:

                    print(
                        f"Since Existing PerpFuture Position Exist : {PerpFut_Position}, and PerpFuture Requirements : {PerpFut_Requirement},"
                        f"So Futures Adjustment 'BUY' Quantity is : {adjustment_quantity}")

                    await self.ByBitAPI.create_PerpFutures_Order(direction="BUY", quantity=abs(adjustment_quantity))


            elif PerpFut_Requirement < 0:
                adjustment_quantity = round((-PerpFut_Position + PerpFut_Requirement), 3)

                if adjustment_quantity < 0:
                    print(
                        f"Since Existing PerpFuture Position Exist : {PerpFut_Position}, and PerpFuture Requirements : {PerpFut_Requirement},"
                        f"So Futures Adjustment 'SELL' Quantity is : {adjustment_quantity}")
                    await self.ByBitAPI.create_PerpFutures_Order(direction="SELL", quantity=abs(adjustment_quantity))

                else:
                    print(
                        f"Since Existing PerpFuture Position Exist : {PerpFut_Position}, and PerpFuture Requirements : {PerpFut_Requirement},"
                        f"So Futures Adjustment 'BUY' Quantity is : {adjustment_quantity}")
                    await self.ByBitAPI.create_PerpFutures_Order(direction="BUY", quantity=abs(adjustment_quantity))

        elif PerpFut_Position < 0.0:

            if PerpFut_Requirement == 0:
                adjustment_quantity = round((PerpFut_Requirement - PerpFut_Position), 3)

                print(
                    f"Since Existing PerpFuture Position Exist : {PerpFut_Position}, and PerpFuture Requirements : {PerpFut_Requirement},"
                    f"So Futures Adjustment 'BUY' Quantity is : {adjustment_quantity}")
                await self.ByBitAPI.create_PerpFutures_Order(direction="BUY", quantity=abs(adjustment_quantity))

            elif PerpFut_Requirement > 0:
                adjustment_quantity = round((-PerpFut_Position + PerpFut_Requirement), 3)

                print(
                    f"Since Existing PerpFuture Position Exist : {PerpFut_Position}, and PerpFuture Requirements : {PerpFut_Requirement},"
                    f"So Futures Adjustment 'BUY' Quantity is : {adjustment_quantity}")

                await self.ByBitAPI.create_PerpFutures_Order(direction="BUY", quantity=abs(adjustment_quantity))


            elif PerpFut_Requirement < 0:
                if PerpFut_Position > PerpFut_Requirement:
                    adjustment_quantity = -round((PerpFut_Position - PerpFut_Requirement), 3)
                elif PerpFut_Position < PerpFut_Requirement:
                    adjustment_quantity = round((PerpFut_Requirement - PerpFut_Position), 3)

                if adjustment_quantity < 0:
                    print(
                        f"Since Existing PerpFuture Position Exist : {PerpFut_Position}, and PerpFuture Requirements : {PerpFut_Requirement},"
                        f"So Futures Adjustment 'SELL' Quantity is : {adjustment_quantity}")
                    await self.ByBitAPI.create_PerpFutures_Order(direction="SELL", quantity=abs(adjustment_quantity))

                else:
                    print(
                        f"Since Existing PerpFuture Position Exist : {PerpFut_Position}, and PerpFuture Requirements : {PerpFut_Requirement},"
                        f"So Futures Adjustment 'BUY' Quantity is : {adjustment_quantity}")
                    await self.ByBitAPI.create_PerpFutures_Order(direction="BUY", quantity=abs(adjustment_quantity))

        return adjustment_quantity


    async def set_expiry_delta_in_dictionary(self):
        try :
            # Assigne the Delta Hedging Values into the Expiry Dictionary for each Expiry
            self.expiry["daily"]["delta_limit"] = self.daily_delta_limit
            self.expiry["weekly"]["delta_limit"] = self.weekly_delta_limit
            self.expiry["monthly"]["delta_limit"] = self.monthly_delta_limit
            self.expiry["quarterly"]["delta_limit"] = self.quarterly_delta_limit
        except Exception as e:
            print(f"Error in processing the Options Delta Limit into Expiry Dictionary !!!!  ",
                  f"Error Details : {e}")

    async def set_delta_limit_and_compute_hedging(self):
        try:
            # Assigne the Delta Limit Values into the Positions DataFrame for each Expiry
            self.position_df.loc[self.position_df.expiry_type == "daily", "expiry_delta"] = self.daily_delta_limit
            self.position_df.loc[self.position_df.expiry_type == "weekly", "expiry_delta"] = self.weekly_delta_limit
            self.position_df.loc[self.position_df.expiry_type == "monthly", "expiry_delta"] = self.monthly_delta_limit
            self.position_df.loc[self.position_df.expiry_type == "quarterly", "expiry_delta"] = self.quarterly_delta_limit
        except Exception as e:
            print(f"Error in Setting the Default Delta Values into Options DataFrame !!!!  ",
                  f"Error Details : {e}")

        try:
            for i in range(len(self.position_df)):
                print(f"Delta Hedging : {abs(round(self.position_df.loc[i, "delta_hedging"], 6))}",
                      f"Expiry Delta {abs(round(self.position_df.loc[i, "expiry_delta"], 6))}, "
                      f"Expiry Date : {self.position_df.loc[i, "expiry"]}")
                if abs(round(self.position_df.loc[i, "delta_hedging"], 6)) > abs(round(self.position_df.loc[i, "expiry_delta"], 6)):
                    print(f"Attention !! Hedging Required !!! for Expiry Position {self.position_df.loc[i, "expiry"]}")
                    self.position_df.loc[i, "hedging_required"] = True
                    self.position_df.loc[i, "PerpFutureQty"] = self.position_df.loc[i, "delta_hedging"]
                else:
                    print(f"Hedging Not Required, for Expiry Position {self.position_df.loc[i, "expiry"]} ")
                    self.position_df.loc[i, "hedging_required"] = False
                    self.position_df.loc[i, "PerpFutureQty"] = self.position_df.loc[i, "delta_hedging"]
        except Exception as e:
            print(f"Error in Computing the Delta Hedging Required and PerpFutureQty into Options DataFrame !!!!  ",
                  f"Error Details : {e}")

    async def check_delta_computation(self):
        if round(sum(self.position_df.total_delta), 6) == round(sum(self.position_df.delta_hedging) / 2, 6):
            print(f"Total Delta is Computed Correctly !!! , Total Options Open Positional Detla is : ",
                  f"{sum(self.position_df.total_delta)}",
                  f"Total Delta Computed for Hedging : {sum(self.position_df.delta_hedging) / 2}")

        else:
            print(f"ERROR !!!! Delta Computed In-Correctly",
                  f"Total Delta : {sum(self.position_df.total_delta)}",
                  f"Total Delta Computed for Hedging : {sum(self.position_df.delta_hedging) / 2}")

    async def check_existing_position_and_take_new_positions(self):

        await self._process_daily_expiry()
        await self._process_weekly_expiry()
        await self._process_monthly_expiry()
        await self._process_quarterly_expiry()

    async def _process_expiry(self, option_chain: pd.DataFrame | None =None,
                              position_direction: str | None = None,
                              quantity: np.float64 | None = None):

        # new_options_position:bool = False

        call, put = await recommend_option_position(delta_value = self.delta_value,
                                                    min_bid_price = self.min_bid_price,
                                                    initial_mark_price_diff = self.initial_mark_price_diff,
                                                    max_mark_price_diff = self.max_mark_price_diff,
                                                    mark_price_diff_steps = self.mark_price_diff_steps,
                                                    option_chain = option_chain)

        if call is not None and put is not None:
            await self.ByBitAPI.create_Option_Order(direction=position_direction,
                                                    symbol=call.iloc[0]["symbol"],
                                                    quantity=quantity)

            await self.ByBitAPI.create_Option_Order(direction=position_direction,
                                                    symbol=put.iloc[0]["symbol"],
                                                    quantity=quantity)
            # new_options_position = True
        else:
            print(f"Cannot Place Orders for Option Chain as NO Matching Criteria Exist")
            # new_options_position = False

        # return new_options_position

    async def _process_daily_expiry(self):
        # Daily: Current Daily Position
        ## Update the Daily Expiry Dictionary
        # Update the Daily: Current Daily
        try:
            if len(self.position_df.loc[
                       self.position_df.expiry == self.expiry["daily"]["current"]["date"], "expiry_type"]) > 0:
                print(f"Position Exist for the Expiry Date : {self.expiry["daily"]["current"]["date"]}, "
                      f"Updating the Expiry Dictionary")
                self.expiry["daily"]["current"]["position_exist"] = True

            elif self.expiry["daily"]["current"]["position_exist"] is False and self.daily is True:

                print(f"Options Position DONNT Exist for the the Expiry : "
                      f"{self.expiry["daily"]["current"]["date"]} and Daily is set as : {self.daily},"
                      f"Processing the Options Order for this Expiry")

                await self._process_expiry(option_chain=self.current_daily,
                                           position_direction="SELL",
                                           quantity=self.quantity)
                self.expiry["daily"]["current"]["position_exist"] = True

            else:
                print(f"Cannot Place Orders for the Expiry : {self.expiry["daily"]["current"]["date"]} , "
                      f"Current Daily Position Exists : {self.expiry["daily"]["current"]["position_exist"]}")
                self.expiry["daily"]["current"]["position_exist"] = False

            self.position_df.loc[
                self.position_df.expiry == self.expiry["daily"]["current"]["date"], "expiry_type"] = "daily"

        except Exception as e:
            print(f"Error in Processing Current Daily Expiry, Error Details :{e}")

        # Daily: Next Daily Position
        # Update the Daily: Next Daily
        try:
            if len(self.position_df.loc[
                       self.position_df.expiry == self.expiry["daily"]["next"]["date"], "expiry_type"]) > 0:
                print(f"Position Exist for the Expiry Date : {self.expiry["daily"]["next"]["date"]}, "
                      f"Updating the Expiry Dictionary")

                self.expiry["daily"]["next"]["position_exist"] = True

            elif self.expiry["daily"]["next"]["position_exist"] is False and self.daily is True:

                print(f"Options Position DONNT Exist for the the Expiry : "
                      f"{self.expiry["daily"]["next"]["date"]} and Daily is set as : {self.daily}",
                      f"Processing the Options Order for this Expiry")

                await self._process_expiry(option_chain=self.next_daily,
                                           position_direction="SELL",
                                           quantity=self.quantity)

                self.expiry["daily"]["next"]["position_exist"] = True

            else:
                print(f"Cannot Place Orders for the Expiry : {self.expiry["daily"]["next"]["date"]} , "
                      f"Next Daily Position Exists : {self.expiry["daily"]["next"]["position_exist"]}",
                      f"Processing the Options Order for this Expiry")

                self.expiry["daily"]["next"]["position_exist"] = False

            self.position_df.loc[
                self.position_df.expiry == self.expiry["daily"]["next"]["date"], "expiry_type"] = "daily"

        except Exception as e:
            print(f"Error in Processing Next Daily Expiry, Error Details :{e}")

        # Daily: Next To Next Daily Position
        # Update the Daily: Next_to_Next Daily
        try:
            if len(self.position_df.loc[
                       self.position_df.expiry == self.expiry["daily"]["next_to_next"]["date"], "expiry_type"]) > 0:
                print(f"Position Exist for the Expiry Date : {self.expiry["daily"]["next_to_next"]["date"]}, "
                      f"Updating the Expiry Dictionary")

                self.expiry["daily"]["next_to_next"]["position_exist"] = True

            elif self.expiry["daily"]["next_to_next"]["position_exist"] is False and self.daily is True:

                print(f"Options Position DONNT Exist for the the Expiry : "
                      f"{self.expiry["daily"]["next_to_next"]["date"]} and Daily is set as : {self.daily}",
                      f"Processing the Options Order for this Expiry")

                await self._process_expiry(option_chain=self.next_to_next_daily,
                                           position_direction="SELL",
                                           quantity=self.quantity)
                self.expiry["daily"]["next_to_next"]["position_exist"] = True

            else:
                print(f"Cannot Place Orders for the Expiry : {self.expiry["daily"]["next_to_next"]["date"]} , "
                      f"Next To Next Daily Position Exists : {self.expiry["daily"]["next_to_next"]["position_exist"]}")
                self.expiry["daily"]["next_to_next"]["position_exist"] = False

            self.position_df.loc[
                self.position_df.expiry == self.expiry["daily"]["next_to_next"]["date"], "expiry_type"] = "daily"

        except Exception as e:
            print(f"Error in Processing Next To Next Daily Expiry, Error Details :{e}")

    async def _process_weekly_expiry(self):

        # Weekly: Current Weekly Position
        ## Update the Weekly Expiry Dictionary
        # Update the Weekly: Current Weekly
        if len(self.position_df.loc[
                   self.position_df.expiry == self.expiry["weekly"]["current"]["date"], "expiry_type"]) > 0:
            print(f"Position Exist for the Expiry Date : {self.expiry["weekly"]["current"]["date"]}, "
                  f"Updating the Expiry Dictionary")
            self.position_df.loc[
                self.position_df.expiry == self.expiry["weekly"]["current"]["date"], "expiry_type"] = "weekly"
            self.expiry["weekly"]["current"]["position_exist"] = True

        elif self.expiry["weekly"]["current"]["position_exist"] is False and self.weekly is True:

            print(f"Options Position DONNT Exist for the the Expiry : "
                  f"{self.expiry["weekly"]["current"]["date"]} and Weekly is set as : {self.weekly}",
                  f"Processing the Options Order for this Expiry")

            await self._process_expiry(option_chain=self.current_weekly,
                                       position_direction="SELL",
                                       quantity=self.quantity)
            self.expiry["weekly"]["current"]["position_exist"] = True

        else:
            print(f"Cannot Place Orders for the Expiry : {self.expiry["weekly"]["current"]["date"]} , "
                  f"Current Weekly Position Exists : {self.expiry["weekly"]["current"]["position_exist"]}")
            self.expiry["weekly"]["current"]["position_exist"] = False

        # Weekly: Next Weekly Position
        # Update the Weekly: Next Weekly
        if len(self.position_df.loc[
                   self.position_df.expiry == self.expiry["weekly"]["next"]["date"], "expiry_type"]) > 0:
            print(f"Position Exist for the Expiry Date : {self.expiry["weekly"]["next"]["date"]}, "
                  f"Updating the Expiry Dictionary")
            self.position_df.loc[
                self.position_df.expiry == self.expiry["weekly"]["next"]["date"], "expiry_type"] = "weekly"
            self.expiry["weekly"]["next"]["position_exist"] = True

        elif self.expiry["weekly"]["next"]["position_exist"] is False and self.weekly is True:

            print(f"Options Position DONNT Exist for the the Expiry : "
                  f"{self.expiry["weekly"]["next"]["date"]} and Weekly is set as : {self.weekly}",
                  f"Processing the Options Order for this Expiry")

            await self._process_expiry(option_chain=self.next_weekly,
                                       position_direction="SELL",
                                       quantity=self.quantity)
            self.expiry["weekly"]["next"]["position_exist"] = True

        else:
            print(f"Cannot Place Orders for the Expiry : {self.expiry["weekly"]["next"]["date"]} , "
                  f"Next Weekly Position Exists : {self.expiry["weekly"]["next"]["position_exist"]}")
            self.expiry["weekly"]["next"]["position_exist"] = False


        # Weekly: Next To Next Weekly Positions
        # Update the Weekly: Next_to_Next Weekly
        if len(self.position_df.loc[
                   self.position_df.expiry == self.expiry["weekly"]["next_to_next"]["date"], "expiry_type"]) > 0:
            print(f"Position Exist for the Expiry Date : {self.expiry["weekly"]["next_to_next"]["date"]}, "
                  f"Updating the Expiry Dictionary")
            self.position_df.loc[
                self.position_df.expiry == self.expiry["weekly"]["next_to_next"]["date"], "expiry_type"] = "weekly"
            self.expiry["weekly"]["next_to_next"]["position_exist"] = True

        elif self.expiry["weekly"]["next_to_next"]["position_exist"] is False and self.weekly is True:

            print(f"Options Position DONNT Exist for the the Expiry : "
                  f"{self.expiry["weekly"]["next_to_next"]["date"]} and Weekly is set as : {self.weekly}",
                  f"Processing the Options Order for this Expiry")

            await self._process_expiry(option_chain=self.next_to_next_weekly,
                                       position_direction="SELL",
                                       quantity=self.quantity)
            self.expiry["weekly"]["next_to_next"]["position_exist"] = True

        else:
            print(f"Cannot Place Orders for the Expiry : {self.expiry["weekly"]["next_to_next"]["date"]} , "
                  f"Next To Next Weekly Position Exists : {self.expiry["weekly"]["next_to_next"]["position_exist"]}")
            self.expiry["weekly"]["next_to_next"]["position_exist"] = False

    async def _process_monthly_expiry(self):

        # Monthly: Currently Monthly Positions
        ## Update the Monthly Expiry Dictionary
        # Update the Monthly: Current Monthly
        if len(self.position_df.loc[
                   self.position_df.expiry == self.expiry["monthly"]["current"]["date"], "expiry_type"]) > 0:
            print(f"Position Exist for the Expiry Date : {self.expiry["monthly"]["current"]["date"]}, "
                  f"Updating the Expiry Dictionary")
            self.position_df.loc[
                self.position_df.expiry == self.expiry["monthly"]["current"]["date"], "expiry_type"] = "monthly"

        elif self.expiry["monthly"]["current"]["position_exist"] is False and self.monthly is True:

            print(f"Options Position DONNT Exist for the the Expiry : "
                  f"{self.expiry["monthly"]["current"]["date"]} and Monthly is set as : {self.monthly}",
                  f"Processing the Options Order for this Expiry")

            await self._process_expiry(option_chain=self.current_monthly,
                                       position_direction="SELL",
                                       quantity=self.quantity)

            self.expiry["monthly"]["current"]["position_exist"] = True

        else:
            print(f"Cannot Place Orders for the Expiry : {self.expiry["monthly"]["current"]["date"]} , "
                  f"Current Monthly Position Exists : {self.expiry["monthly"]["current"]["position_exist"]}")
            self.expiry["monthly"]["current"]["position_exist"] = False


        # Monthly: Next Monthly Positions
        # Update the Monthly: Next Monthly
        if len(self.position_df.loc[
                   self.position_df.expiry == self.expiry["monthly"]["next"]["date"], "expiry_type"]) > 0:
            print(f"Position Exist for the Expiry Date : {self.expiry["monthly"]["next"]["date"]}, "
                  f"Updating the Expiry Dictionary")
            self.position_df.loc[
                self.position_df.expiry == self.expiry["monthly"]["next"]["date"], "expiry_type"] = "monthly"
            self.expiry["monthly"]["next"]["position_exist"] = True

        elif self.expiry["monthly"]["next"]["position_exist"] is False and self.monthly is True:

            print(f"Options Position DONNT Exist for the the Expiry : "
                  f"{self.expiry["monthly"]["next"]["date"]} and Monthly is set as : {self.monthly}",
                  f"Processing the Options Order for this Expiry")

            await self._process_expiry(option_chain=self.next_monthly,
                                       position_direction="SELL",
                                       quantity=self.quantity)
            self.expiry["monthly"]["next"]["position_exist"] = True

        else:
            print(f"Cannot Place Orders for the Expiry : {self.expiry["monthly"]["next"]["date"]} , "
                  f"Next Monthly Position Exists : {self.expiry["monthly"]["next"]["position_exist"]}")
            self.expiry["monthly"]["next"]["position_exist"] = False



        # Monthly: Next to Next Monthly Positions
        # Update the Monthly: Next_to_Next Monthly
        if len(self.position_df.loc[
                   self.position_df.expiry == self.expiry["monthly"]["next_to_next"]["date"], "expiry_type"]) > 0:
            print(f"Position Exist for the Expiry Date : {self.expiry["monthly"]["next_to_next"]["date"]}, "
                  f"Updating the Expiry Dictionary")
            self.position_df.loc[self.position_df.expiry == self.expiry["monthly"]["next_to_next"][
                "date"], "expiry_type"] = "monthly"
            self.expiry["monthly"]["next_to_next"]["position_exist"] = True

        elif self.expiry["monthly"]["next_to_next"]["position_exist"] is False and self.monthly is True:

            print(f"Options Position DONNT Exist for the the Expiry : "
                  f"{self.expiry["monthly"]["next_to_next"]["date"]} and Monthly is set as : {self.monthly}",
                  f"Processing the Options Order for this Expiry")

            await self._process_expiry(option_chain=self.next_to_next_monthly,
                                       position_direction="SELL",
                                       quantity=self.quantity)

            self.expiry["monthly"]["next_to_next"]["position_exist"] = True

        else:
            print(f"Cannot Place Orders for the Expiry : {self.expiry["monthly"]["next_to_next"]["date"]} , "
                  f"Next to Next Monthly Position Exists : {self.expiry["monthly"]["next_to_next"]["position_exist"]}")
            self.expiry["monthly"]["next_to_next"]["position_exist"] = False

    async def _process_quarterly_expiry(self):

        # Quarterly: Current Quarterly Positions
        ## Update the Quarterly Expiry Dictionary
        # Update the Quarterly: Current Quarterly
        if len(self.position_df.loc[
                   self.position_df.expiry == self.expiry["quarterly"]["current"]["date"], "expiry_type"]) > 0:
            print(f"Position Exist for the Expiry Date : {self.expiry["quarterly"]["current"]["date"]}, "
                  f"Updating the Expiry Dictionary")
            self.position_df.loc[
                self.position_df.expiry == self.expiry["quarterly"]["current"]["date"], "expiry_type"] = "quarterly"

        elif self.expiry["quarterly"]["current"]["position_exist"] is False and self.quarterly is True:

            print(f"Options Position DONNT Exist for the the Expiry : "
                  f"{self.expiry["quarterly"]["current"]["date"]} and Quarterly is set as : {self.quarterly}",
                  f"Processing the Options Order for this Expiry")

            await self._process_expiry(option_chain=self.current_quarterly,
                                       position_direction="SELL",
                                       quantity=self.quantity)
            self.expiry["quarterly"]["current"]["position_exist"] = True

        else:
            print(f"Cannot Place Orders for the Expiry : {self.expiry["quarterly"]["current"]["date"]} , "
                  f"Current Quarterly Position Exists : {self.expiry["quarterly"]["current"]["position_exist"]}")
            self.expiry["quarterly"]["current"]["position_exist"] = False


        # Quarterly: Next Quarterly Positions
        # Update the Quarterly: Next Quarterly
        if len(self.position_df.loc[
                   self.position_df.expiry == self.expiry["quarterly"]["next"]["date"], "expiry_type"]) > 0:
            print(f"Position Exist for the Expiry Date : {self.expiry["quarterly"]["next"]["date"]}, "
                  f"Updating the Expiry Dictionary")
            self.position_df.loc[
                self.position_df.expiry == self.expiry["quarterly"]["next"]["date"], "expiry_type"] = "quarterly"
            self.expiry["quarterly"]["next"]["position_exist"] = True

        elif self.expiry["quarterly"]["next"]["position_exist"] is False and self.quarterly is True:

            print(f"Options Position DONNT Exist for the the Expiry : "
                  f"{self.expiry["quarterly"]["next"]["date"]} and Quarterly is set as : {self.quarterly}",
                  f"Processing the Options Order for this Expiry")

            await self._process_expiry(option_chain=self.next_quarterly,
                                       position_direction="SELL",
                                       quantity=self.quantity)
            self.expiry["quarterly"]["next"]["position_exist"] = True

        else:
            print(f"Cannot Place Orders for the Expiry : {self.expiry["quarterly"]["next"]["date"]} , "
                  f"Next Quarterly Position Exists : {self.expiry["quarterly"]["next"]["position_exist"]}")
            self.expiry["quarterly"]["next"]["position_exist"] = False

        # Quarterly: Next To Next Quarterly Positions
        # Update the Quarterly: Next_to_Next Quarterly
        if len(self.position_df.loc[
                   self.position_df.expiry == self.expiry["quarterly"]["next_to_next"]["date"], "expiry_type"]) > 0:
            print(f"Position Exist for the Expiry Date : {self.expiry["quarterly"]["next_to_next"]["date"]}, "
                  f"Updating the Expiry Dictionary")
            self.position_df.loc[self.position_df.expiry == self.expiry["quarterly"]["next_to_next"][
                "date"], "expiry_type"] = "quarterly"
            self.expiry["quarterly"]["next_to_next"]["position_exist"] = True

        elif self.expiry["quarterly"]["next_to_next"]["position_exist"] is False and self.quarterly is True:

            print(f"Options Position DONNT Exist for the the Expiry : "
                  f"{self.expiry["quarterly"]["next_to_next"]["date"]} and Quarterly is set as : {self.quarterly}",
                  f"Processing the Options Order for this Expiry")

            await self._process_expiry(option_chain=self.next_to_next_quarterly,
                                       position_direction="SELL",
                                       quantity=self.quantity)
            self.expiry["quarterly"]["next_to_next"]["position_exist"] = True

        else:
            print(f"Cannot Place Orders for the Expiry : {self.expiry["quarterly"]["next_to_next"]["date"]} , "
                  f"Next To Next Quarterly Position Exists : {self.expiry["quarterly"]["next_to_next"]["position_exist"]}")
            self.expiry["quarterly"]["next_to_next"]["position_exist"] = False


