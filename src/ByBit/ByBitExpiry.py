"""
Class to manage and track the expiry dates of options contracts across various timeframes.
"""

import datetime
from datetime import timedelta

import pandas as pd

from common_functions import generate_expiry_dates


# TODO : NEW File Needs to be INCORPORATED Everywhere

class ByBitExpiry(object):
    """
    Initializes the object with several attributes meant to track the expiry dates of options contracts
    across multiple timeframes (daily, weekly, monthly, quarterly).
    The attributes include expiry dates, lists, and dictionaries to maintain current and future expiry details.
    """
    def __init__(self):
        """
        Initializes the object with several attributes meant to track the expiry dates of options contracts
        across multiple timeframes (daily, weekly, monthly, quarterly).
        The attributes include expiry dates, lists, and dictionaries to maintain current and future expiry details.
        """
        # This Value is Set Everytime The Options Expiry Create Function is Called
        self.expiry_dates: pd.DataFrame or None = None

        # Initialize the Expiry List Variable to hold the Expiry Dates, as extracted from Panda DataFrame
        self.expiry_list: list = []

        # Initialize the Master Dictionary to Hold ALL the Values
        self.expiry: dict = {
            "daily": {
                "delta_limit": None,
                "current": {
                    "date": None,
                    "exist": False,
                    "position_exist": False,
                    "symbol_list": []
                },
                "next": {
                    "date": None,
                    "exist": False,
                    "position_exist": False,
                    "symbol_list": []
                },
                "next_to_next": {
                    "date": None,
                    "exist": False,
                    "position_exist": False,
                    "symbol_list": []
                },
            },
            "weekly": {
                "delta_limit": None,
                "current": {
                    "date": None,
                    "exist": False,
                    "position_exist": False,
                    "symbol_list": []
                },
                "next": {
                    "date": None,
                    "exist": False,
                    "position_exist": False,
                    "symbol_list": []
                },
                "next_to_next": {
                    "date": None,
                    "exist": False,
                    "position_exist": False,
                    "symbol_list": []
                },
            },
            "monthly": {
                "delta_limit": None,
                "current": {
                    "date": None,
                    "exist": False,
                    "position_exist": False,
                    "symbol_list": []
                },
                "next": {
                    "date": None,
                    "exist": False,
                    "position_exist": False,
                    "symbol_list": []
                },
                "next_to_next": {
                    "date": None,
                    "exist": False,
                    "position_exist": False,
                    "symbol_list": []
                },
            },
            "quarterly": {
                "delta_limit": None,
                "current": {
                    "date": None,
                    "exist": False,
                    "position_exist": False,
                    "symbol_list": []
                },
                "next": {
                    "date": None,
                    "exist": False,
                    "position_exist": False,
                    "symbol_list": []
                },
                "next_to_next": {
                    "date": None,
                    "exist": False,
                    "position_exist": False,
                    "symbol_list": []
                },
            }
        }

        # This is to Hold the Current Daily Date Value for Today based on UTC timezone
        self.today: datetime.date or None = None

        # This is to Hold the Next Daily Date Value for Tomorrow based on UTC timezone
        self.tomorrow: datetime.date or None = None

        # This is to Hold the Next To Next Daily Date Value for Day After Tomorrow based on UTC timezone
        self.day_after_tomorrow: datetime.date or None = None
        self.weekly_expiry_dates: datetime.date or None = None
        self.monthly_expiry_dates: datetime.date or None = None
        self.quarterly_expiry_dates: datetime.date or None = None

    async def _reinitialize_default_values(self):
        """
        Reinitializes the default values for expiry timeframes and their corresponding positions.

        Initializes:
          - `self.expiry_list` to an empty list to hold expiry dates extracted from a pandas DataFrame.
          - `self.expiry` dictionary to store data for various timeframes including 'daily',
          'weekly', 'monthly', and 'quarterly'.
            For each timeframe, it sets:
            - `delta_limit` to None.
            - `current` to hold the date, existence status, position existence status, and a list of symbols.
            - `next` with similar structure to 'current'.
            - `next_to_next` with similar structure to 'current' and 'next'.
          - Daily date values `self.today`, `self.tomorrow`, and `self.day_after_tomorrow`
          to None representing the current day, next day,
          and day after next, respectively, based on UTC timezone.
          - Expiry date variables `self.weekly_expiry_dates`, `self.monthly_expiry_dates`,
          and `self.quarterly_expiry_dates` to None for storing specific expiry dates.
        """
        # Initialize the Expiry List Variable to hold the Expiry Dates, as extracted from Panda DataFrame
        self.expiry_list: list = []

        # Initialize the Master Dictionary to Hold ALL the Values
        self.expiry: dict = {
            "daily": {
                "delta_limit":None,
                "current": {
                    "date": None,
                    "exist": False,
                    "position_exist": False,
                    "symbol_list":[]
                },
                "next": {
                    "date": None,
                    "exist": False,
                    "position_exist": False,
                    "symbol_list": []
                },
                "next_to_next": {
                    "date": None,
                    "exist": False,
                    "position_exist": False,
                    "symbol_list": []
                },
            },
            "weekly": {
                "delta_limit": None,
                "current": {
                    "date": None,
                    "exist": False,
                    "position_exist": False,
                    "symbol_list": []
                },
                "next": {
                    "date": None,
                    "exist": False,
                    "position_exist": False,
                    "symbol_list": []
                },
                "next_to_next": {
                    "date": None,
                    "exist": False,
                    "position_exist": False,
                    "symbol_list": []
                },
            },
            "monthly": {
                "delta_limit": None,
                "current": {
                    "date": None,
                    "exist": False,
                    "position_exist": False,
                    "symbol_list": []
                },
                "next": {
                    "date": None,
                    "exist": False,
                    "position_exist": False,
                    "symbol_list": []
                },
                "next_to_next": {
                    "date": None,
                    "exist": False,
                    "position_exist": False,
                    "symbol_list": []
                },
            },
            "quarterly": {
                "delta_limit": None,
                "current": {
                    "date": None,
                    "exist": False,
                    "position_exist": False,
                    "symbol_list": []
                },
                "next": {
                    "date": None,
                    "exist": False,
                    "position_exist": False,
                    "symbol_list": []
                },
                "next_to_next": {
                    "date": None,
                    "exist": False,
                    "position_exist": False,
                    "symbol_list": []
                },
            }
        }

        # This is to Hold the Current Daily Date Value for Today based on UTC timezone
        self.today: datetime.date or None = None

        # This is to Hold the Next Daily Date Value for Tomorrow based on UTC timezone
        self.tomorrow: datetime.date or None = None

        # This is to Hold the Next To Next Daily Date Value for Day After Tomorrow based on UTC timezone
        self.day_after_tomorrow: datetime.date or None = None
        self.weekly_expiry_dates: datetime.date or None = None
        self.monthly_expiry_dates: datetime.date or None = None
        self.quarterly_expiry_dates: datetime.date or None = None

    async def create_options_expiry(self, expiry_dataframe: pd.DataFrame,
                                    weekly_count: int = 4,
                                    monthly_count: int = 4,
                                    quarterly_count: int = 4):
        """
        Args:
            expiry_dataframe: DataFrame containing expiry dates.
            weekly_count: Number of weekly expiry dates to generate.
            monthly_count: Number of monthly expiry dates to generate.
            quarterly_count: Number of quarterly expiry dates to generate.
        """
        self.expiry_dates = expiry_dataframe

        await self._reinitialize_default_values()

        for i in range(len(self.expiry_dates)):
            self.expiry_list.append(self.expiry_dates.iloc[i].date())
            print(f"{self.expiry_dates.iloc[i].date()}")

        (self.weekly_expiry_dates,
         self.monthly_expiry_dates,
         self.quarterly_expiry_dates) = (generate_expiry_dates(weekly_count=weekly_count,
                                                               monthly_count=monthly_count,
                                                               quarterly_count=quarterly_count))

        await self._create_daily_expiry_dates()
        # await self._create_weekly_expiry_dates()
        # await self._create_monthly_expiry_dates()
        # await self._create_quarterly_expiry_dates()

        await self._create_other_expiry_dates()

        return self.expiry

    async def _create_quarterly_expiry_dates(self):
        """
        Creates and sets quarterly expiry dates based on the existing expiry list.

        Iterates through the `quarterly_expiry_dates`
        in reverse order and checks if each date exists in the `expiry_list`.
        If a date exists, it updates the nested dictionaries within `self.expiry`
        for "current", "next", and "next_to_next" expiry dates.
        Prints status messages for each date update.
        Handles exceptions by printing an error message.

        Raises:
            Exception: If an error occurs during the processing of quarterly expiry dates.
        """
        try:
            for i in range(1, len(self.quarterly_expiry_dates) + 1):
                # print(f"Quarterly Expiry Dates Index : {-i}, Date : {self.quarterly_expiry_dates[-i]}")
                if self.quarterly_expiry_dates[-i] in self.expiry_list:
                    print(f"Quarterly Expiry Dates :{self.quarterly_expiry_dates[-i]} Exist in Expiry List")

                    if self.expiry["quarterly"]["next_to_next"]["date"] is None:
                        self.expiry["quarterly"]["next_to_next"]["date"] = self.quarterly_expiry_dates[-i]
                        self.expiry["quarterly"]["next_to_next"]["exist"] = True
                        print(f"Setting expiry[quarterly][next_to_next][date] as : "
                              f"{self.expiry["quarterly"]["next_to_next"]["date"]}",
                              f"Setting expiry[quarterly][next_to_next][exist] as : "
                              f"{self.expiry["quarterly"]["next_to_next"]["exist"]}")

                    elif self.expiry["quarterly"]["next"]["date"] is None:
                        self.expiry["quarterly"]["next"]["date"] = self.quarterly_expiry_dates[-i]
                        self.expiry["quarterly"]["next"]["exist"] = True
                        print(f"Setting expiry[quarterly][next][date] as : "
                              f"{self.expiry["quarterly"]["next"]["date"]}",
                              f"Setting expiry[quarterly][next][exist] as : "
                              f"{self.expiry["quarterly"]["next"]["exist"]}")

                    elif self.expiry["quarterly"]["current"]["date"] is None:
                        self.expiry["quarterly"]["current"]["date"] = self.quarterly_expiry_dates[-i]
                        self.expiry["quarterly"]["current"]["exist"] = True
                        print(f"Setting expiry[quarterly][current][date] as : "
                              f"{self.expiry["quarterly"]["current"]["date"]}",
                              f"Setting expiry[quarterly][current][exist] as : "
                              f"{self.expiry["quarterly"]["current"]["exist"]}")
                else:
                    print(f"Quarterly Expiry Dates :{self.quarterly_expiry_dates[-i]} 'DO-NOT' Exist in Expiry List")

        except Exception as e:
            print(f"Error Occurred while Formatting the Quarterly Expiry Dates, Error Code : {e}")

    async def _create_monthly_expiry_dates(self):
        """
        Creates and updates monthly expiry dates.

        This asynchronous function iterates through the list of monthly expiry dates in reverse order.
        For each date, it updates the `expiry` dictionary with "current", "next", and "next_to_next" expiry dates
        if they are found in the `expiry_list` and are not yet set.
        Prints diagnostic messages during its execution.

        Raises:
            Exception: If there is an error during the formatting of monthly expiry dates.
        """
        try:
            for i in range(1, len(self.monthly_expiry_dates) + 1):
                # print(f"Monthly Expiry Dates Index : {-i}, Date : {self.monthly_expiry_dates[-i]}")

                if self.monthly_expiry_dates[-i] in self.expiry_list:
                    print(f"Monthly Expiry Dates :{self.monthly_expiry_dates[-i]} Exist in Expiry List")

                    if self.expiry["monthly"]["next_to_next"]["date"] is None:
                        self.expiry["monthly"]["next_to_next"]["date"] = self.monthly_expiry_dates[-i]
                        self.expiry["monthly"]["next_to_next"]["exist"] = True
                        print(f"Setting expiry[monthly][next_to_next][date] as : "
                              f"{self.expiry["monthly"]["next_to_next"]["date"]}",
                              f"Setting expiry[monthly][next_to_next][exist] as : "
                              f"{self.expiry["monthly"]["next_to_next"]["exist"]}")

                    elif self.expiry["monthly"]["next"]["date"] is None:
                        self.expiry["monthly"]["next"]["date"] = self.monthly_expiry_dates[-i]
                        self.expiry["monthly"]["next"]["exist"] = True
                        print(f"Setting expiry[monthly][next][date] as : "
                              f"{self.expiry["monthly"]["next"]["date"]}",
                              f"Setting expiry[monthly][next][exist] as : "
                              f"{self.expiry["monthly"]["next"]["exist"]}")

                    elif self.expiry["monthly"]["current"]["date"] is None:
                        self.expiry["monthly"]["current"]["date"] = self.monthly_expiry_dates[-i]
                        self.expiry["monthly"]["current"]["exist"] = True
                        print(f"Setting expiry[monthly][current][date] as : "
                              f"{self.expiry["monthly"]["current"]["date"]}",
                              f"Setting expiry[monthly][current][exist] as : "
                              f"{self.expiry["monthly"]["current"]["exist"]}")

                else:
                    print(f"Monthly Expiry Dates :{self.monthly_expiry_dates[-i]} 'DO NOT' Exist in Expiry List")

        except Exception as e:
            print(f"Error Occurred while Formatting the Monthly Expiry Dates, Error Code : {e}")

    async def _create_weekly_expiry_dates(self):
        """
        Populates the weekly expiry dates in the expiry dictionary.

        This method iterates through `weekly_expiry_dates` in reverse order and checks if each date exists
        in `expiry_list`.
        Depending on the current state of the `expiry` dictionary, it will set the dates
        for "current", "next", and "next_to_next" weekly expiry dates if they are not already set.

        Raises:
            Exception: If an error occurs during the processing of weekly expiry dates.

        Logs:
            Logs the setting of expiry dates and whether expiry dates exist in the expiry list.
        """
        try:
            for i in range(1, len(self.weekly_expiry_dates) + 1):
                # print(f"Weekly Expiry Dates Index : {-i}, Date : {self.weekly_expiry_dates[-i]}")

                if self.weekly_expiry_dates[-i] in self.expiry_list:
                    print(f"Weekly Expiry Dates :{self.weekly_expiry_dates[-i]} Exist in Expiry List")

                    if self.expiry["weekly"]["next_to_next"]["date"] is None:
                        self.expiry["weekly"]["next_to_next"]["date"] = self.weekly_expiry_dates[-i]
                        self.expiry["weekly"]["next_to_next"]["exist"] = True
                        print(f"Setting expiry[weekly][next_to_next][date] as : "
                              f"{self.expiry["weekly"]["next_to_next"]["date"]}",
                              f"Setting expiry[weekly][next_to_next][exist] as : "
                              f"{self.expiry["weekly"]["next_to_next"]["exist"]}")

                    elif self.expiry["weekly"]["next"]["date"] is None:
                        self.expiry["weekly"]["next"]["date"] = self.weekly_expiry_dates[-i]
                        self.expiry["weekly"]["next"]["exist"] = True
                        print(f"Setting expiry[weekly][next][date] as : "
                              f"{self.expiry["weekly"]["next"]["date"]}",
                              f"Setting expiry[weekly][next][exist] as : "
                              f"{self.expiry["weekly"]["next"]["exist"]}")

                    elif self.expiry["weekly"]["current"]["date"] is None:
                        self.expiry["weekly"]["current"]["date"] = self.weekly_expiry_dates[-i]
                        self.expiry["weekly"]["current"]["exist"] = True
                        print(f"Setting expiry[weekly][current][date] as : "
                              f"{self.expiry["weekly"]["current"]["date"]}",
                              f"Setting expiry[weekly][current][exist] as : "
                              f"{self.expiry["weekly"]["current"]["exist"]}")

                else:
                    print(f"Weekly Expiry Dates :{self.weekly_expiry_dates[-i]} 'DO NOT' Exist in Expiry List")

        except Exception as e:
            print(f"Error Occurred while Formatting the Weekly Expiry Dates, Error Code : {e}")

    async def _create_daily_expiry_dates(self):
        """
        Creates daily expiry dates for the current, next, and day after next days
        based on the current UTC time.
        It updates the expiry dictionary with the
        calculated dates if they are present in the expiry_list.

        The function determines the 'today', 'tomorrow', and 'day after tomorrow'
        dates based on the current UTC time and whether the current hour is before
        or after 8 AM.

        Updates:
            - Sets 'today', 'tomorrow', 'day after tomorrow' properties based on current UTC time.
            - Checks if the calculated dates exist in 'expiry_list'.
            - Updates the 'expiry' dictionary with the 'date' and 'exist' status for
              'current', 'next', and 'next to next' days if the date is found in 'expiry_list'.

        Errors:
            - Prints an error message if any of the calculated dates are not found
              in 'expiry_list'.
        """
        # current_utc_datetime = datetime.now(tz=pytz.utc)
        current_utc_datetime = datetime.datetime.now(datetime.UTC)
        if current_utc_datetime.time().hour >= 8:
            self.today = current_utc_datetime.date() + timedelta(days=1)
            self.tomorrow = current_utc_datetime.date() + timedelta(days=2)
            self.day_after_tomorrow = current_utc_datetime.date() + timedelta(days=3)

        elif current_utc_datetime.time().hour <= 8:
            self.today = current_utc_datetime.date()
            self.tomorrow = current_utc_datetime.date() + timedelta(days=1)
            self.day_after_tomorrow = current_utc_datetime.date() + timedelta(days=2)

        if self.today in self.expiry_list:
            self.expiry["daily"]["current"]["date"] = self.today
            self.expiry["daily"]["current"]["exist"] = True
        else:
            print(f"Error : {self.today} is not in Expiry List")

        if self.tomorrow in self.expiry_list:
            self.expiry["daily"]["next"]["date"] = self.tomorrow
            self.expiry["daily"]["next"]["exist"] = True
        else:
            print(f"Error : {self.tomorrow} is not in Expiry List")

        if self.day_after_tomorrow in self.expiry_list:
            self.expiry["daily"]["next_to_next"]["date"] = self.day_after_tomorrow
            self.expiry["daily"]["next_to_next"]["exist"] = True
        else:
            print(f"Error : {self.day_after_tomorrow} is not in Expiry List")

    async def _create_other_expiry_dates(self):
        """
        Populates the `expiry` dictionary with dates from the `expiry_dates` DataFrame.
        The dates are categorized into weekly, monthly, and quarterly expiry dates.

        Iterates over the `expiry_dates` DataFrame and checks if each date belongs to the `weekly_expiry_dates`,
        `monthly_expiry_dates`, or `quarterly_expiry_dates` lists.
        Based on the category, it updates the appropriate fields in the `expiry`
        dictionary (`weekly`, `monthly`, `quarterly`) with the current, next, and next_to_next expiry dates.

        Raises:
            Exception:
            If there is an error during the processing of expiry dates,
            the exception is caught, and an error message is printed.
        """
        try:
            for i in range(len(self.expiry_dates)):
                # Check in the Weekly Expiry List
                if self.expiry_dates.iloc[i].date() in self.weekly_expiry_dates:
                    if self.expiry["weekly"]["current"]["date"] is None:
                        self.expiry["weekly"]["current"]["date"] = self.expiry_dates.iloc[i].date()
                        self.expiry["weekly"]["current"]["exist"] = True
                        print(f"Setting expiry[weekly][current][date] as : "
                              f"{self.expiry["weekly"]["current"]["date"]}",
                              f"Setting expiry[weekly][current][exist] as : "
                              f"{self.expiry["weekly"]["current"]["exist"]}")

                    elif self.expiry["weekly"]["next"]["date"] is None:
                        self.expiry["weekly"]["next"]["date"] = self.expiry_dates.iloc[i].date()
                        self.expiry["weekly"]["next"]["exist"] = True
                        print(f"Setting expiry[weekly][next][date] as : "
                              f"{self.expiry["weekly"]["next"]["date"]}",
                              f"Setting expiry[weekly][next][exist] as : "
                              f"{self.expiry["weekly"]["next"]["exist"]}")

                    elif self.expiry["weekly"]["next_to_next"]["date"] is None:
                        self.expiry["weekly"]["next_to_next"]["date"] = self.expiry_dates.iloc[i].date()
                        self.expiry["weekly"]["next_to_next"]["exist"] = True
                        print(f"Setting expiry[weekly][next_to_next][date] as : "
                              f"{self.expiry["weekly"]["next_to_next"]["date"]}",
                              f"Setting expiry[weekly][next_to_next][exist] as : "
                              f"{self.expiry["weekly"]["next_to_next"]["exist"]}")

                # Check in the Monthly Expiry List
                elif self.expiry_dates.iloc[i].date() in self.monthly_expiry_dates:

                    if self.expiry["monthly"]["current"]["date"] is None:
                        self.expiry["monthly"]["current"]["date"] = self.expiry_dates.iloc[i].date()
                        self.expiry["monthly"]["current"]["exist"] = True
                        print(f"Setting expiry[monthly][current][date] as : "
                              f"{self.expiry["monthly"]["current"]["date"]}",
                              f"Setting expiry[monthly][current][exist] as : "
                              f"{self.expiry["monthly"]["current"]["exist"]}")

                    elif self.expiry["monthly"]["next"]["date"] is None:
                        self.expiry["monthly"]["next"]["date"] = self.expiry_dates.iloc[i].date()
                        self.expiry["monthly"]["next"]["exist"] = True
                        print(f"Setting expiry[monthly][next][date] as : "
                              f"{self.expiry["monthly"]["next"]["date"]}",
                              f"Setting expiry[monthly][next][exist] as : "
                              f"{self.expiry["monthly"]["next"]["exist"]}")

                    elif self.expiry["monthly"]["next_to_next"]["date"] is None:
                        self.expiry["monthly"]["next_to_next"]["date"] = self.expiry_dates.iloc[i].date()
                        self.expiry["monthly"]["next_to_next"]["exist"] = True
                        print(f"Setting expiry[monthly][next_to_next][date] as : "
                              f"{self.expiry["monthly"]["next_to_next"]["date"]}",
                              f"Setting expiry[monthly][next_to_next][exist] as : "
                              f"{self.expiry["monthly"]["next_to_next"]["exist"]}")

                # Check in the Quarterly Expiry List
                elif self.expiry_dates.iloc[i].date() in self.quarterly_expiry_dates:

                    if self.expiry["quarterly"]["current"]["date"] is None:
                        self.expiry["quarterly"]["current"]["date"] = self.expiry_dates.iloc[i].date()
                        self.expiry["quarterly"]["current"]["exist"] = True
                        print(f"Setting expiry[quarterly][current][date] as : "
                              f"{self.expiry["quarterly"]["current"]["date"]}",
                              f"Setting expiry[quarterly][current][exist] as : "
                              f"{self.expiry["quarterly"]["current"]["exist"]}")

                    elif self.expiry["quarterly"]["next"]["date"] is None:
                        self.expiry["quarterly"]["next"]["date"] = self.expiry_dates.iloc[i].date()
                        self.expiry["quarterly"]["next"]["exist"] = True
                        print(f"Setting expiry[quarterly][next][date] as : "
                              f"{self.expiry["quarterly"]["next"]["date"]}",
                              f"Setting expiry[quarterly][next][exist] as : "
                              f"{self.expiry["quarterly"]["next"]["exist"]}")

                    elif self.expiry["quarterly"]["next_to_next"]["date"] is None:
                        self.expiry["quarterly"]["next_to_next"]["date"] = self.expiry_dates.iloc[i].date()
                        self.expiry["quarterly"]["next_to_next"]["exist"] = True
                        print(f"Setting expiry[quarterly][next_to_next][date] as : "
                              f"{self.expiry["quarterly"]["next_to_next"]["date"]}",
                              f"Setting expiry[quarterly][next_to_next][exist] as : "
                              f"{self.expiry["quarterly"]["next_to_next"]["exist"]}")

        except Exception as e:
            print(f"Error In Processing the Expiry Date, Error Details : {e}")
