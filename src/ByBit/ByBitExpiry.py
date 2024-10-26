from common_functions import generate_expiry_dates
from datetime import timedelta
import pytz
import datetime
import pandas as pd


# TODO : NEW File Needs to be INCORPORATED Everywhere

class ByBitExpiry(object):

    def __init__(self):
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
