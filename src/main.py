""""
# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


packaging_tutorial/
├── LICENSE
├── pyproject.toml
├── README.md
├── src/
│   └── example_package_YOUR_USERNAME_HERE/
│       ├── __init__.py
│       └── example.py
└── tests/

"""

from ByBit import CoreLogic
import datetime
import asyncio

async def mainApp():
    btcCoreLogic = CoreLogic(api_url="https://api-demo.bybit.com",  # API URL
                             api_endpoint="/v5/market/tickers",  # Endpoint URI
                             api_keys="6TzoOeOuyIDiJN9cOu",  # API Keys
                             api_secret="JbTE9Z5OFFyxcw1mzqf6hLIpmGgt8dONmlSE",  # API Secret
                             baseCoin="BTC",  # BaseCoin neets be either BTC, ETH, SOL Only
                             settleCoin="USDC",
                             default_quantity=1.00,  # Default Short Quantity

                             daily=True,  # Play on Daily Options: True or False
                             weekly=True,  # Play on Weekly Options: True or False
                             monthly=True,  # Play on Monthly Options: True or False
                             quarterly=True,  # Play on Quarterly Options: True or False

                             delta_value=0.07,  # Delta Value beyond which we would take our Positions
                             min_bid_price=0.01,  # Minumum Bid Price to eliminate the Blank Bid Strikes
                             initial_mark_price_diff=0.05,  # The Difference Between Mark & Bid Price 0.05 means 5%
                             max_mark_price_diff=0.3,  # Maximum Difference between Mark & Bid Price
                             mark_price_diff_steps=0.05,  # Mark & Ask Difference Percentage Incremental Step Size
                             # Tolarated Delta Deavation Limit, after which we Negate the Delta Deavation
                             # by hedging with Perpectual Futuers
                             daily_delta_limit=0.10,  # Daily Delta Deviation Limit
                             weekly_delta_limit=0.10,  # Weekly Delta Deviation Limit
                             monthly_delta_limit=0.10,  # Monthly Delta Deviation Limit
                             quarterly_delta_limit=0.10,  # Quarterly Delta Deviation Limit
                             )
    try :
        while True:
            current_utc_time = datetime.datetime.now(datetime.UTC)
            if (current_utc_time.minute % 5) == 0:
                print(f"Executimg Function {str(current_utc_time)},"
                      f"Wait Timer : {(4 - (current_utc_time.minute % 5)) * 60 + (60 - current_utc_time.second)}")
                try:
                    await btcCoreLogic.core_logic_computation()
                except Exception as e:
                    print(f"Error Occoured in Main IF Loop, Error Details {e}")
                finally:
                    await asyncio.sleep(60)
            else:
                print(f"Entering Sleep at {str(current_utc_time)}",
                      f"Wait Timer : {(4 - (current_utc_time.minute % 5)) * 60 + (60 - current_utc_time.second)}")
                await asyncio.sleep((4 - (current_utc_time.minute % 5)) * 60 + (60 - current_utc_time.second))
    except Exception as e:
        print(f"ALERT !!!! ERROR Occoured , !!!! Exitting the Main While Loop, RELAUNCH THE MAIN FUNCTION, "
              f"ERROR DETAILS {e}" )


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    asyncio.run(mainApp())

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
