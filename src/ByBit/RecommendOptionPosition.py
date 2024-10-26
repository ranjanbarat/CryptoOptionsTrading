import pandas as pd
import time


async def recommend_option_position(delta_value: float = 0.07,
                                    min_bid_price: float = 0.01,
                                    initial_mark_price_diff: float = 0.05,
                                    max_mark_price_diff: float = 0.3,
                                    mark_price_diff_steps: float = 0.05,
                                    option_chain: pd.DataFrame | None = None):
    """
    This function Option Chain Expiry, then Filters the Symbol with the Following Conditions:
        1. Delta : It Should be below or equal to the Provided Delta Value for Both CALL & PUT Options
            As Delta is Negative for PUT Option & Positive for CALL Option
        2. The Position should have some Existing BID Values
        3. We should Recommend a Position that has a Near ZERO i.e., CALL Delta + PUT Delta =. ~0.00X
        4. Return the CALL & PUT Options for Placing an Order

    :parameter delta_value: Maximum Delta Value beyond which we will take the position
    :parameter min_bid_price: Minimum Bid Price Amount to Avoid Blank Bids & Asks
    :parameter max_mark_price_diff: Maximum Difference between Mark & Bid Price i.e., 0.2 repreents 20 %
    :parameter mark_price_diff_steps:
    :parameter initial_mark_price_diff:
    :parameter option_chain:

    :returns call_strike:
    :returns put_strike:
    """
    start = time.perf_counter()
    # Initialize the Static String Variable for CALL & PUT
    call: str = "CALL"
    put: str = "PUT"
    # Initialize the CALL & PUT Options DataFrame for Filtering the Original DataFrame Values as per Recommendation
    call_option: pd.DataFrame | None = None
    put_option: pd.DataFrame | None = None
    # Initialize the CALL & PUT Strike Recommendations for Filtering the Original DataFrame Values as per Recommendation
    call_strike: pd.DataFrame | None = None
    put_strike: pd.DataFrame | None = None
    # Try to Extract the Required Symbols matching the Above Criteria
    mark_price_diff = initial_mark_price_diff
    try:
        while mark_price_diff <= max_mark_price_diff:
            # Filter the Primary DataFrame for CALL Options, CALL Delta Value & Minimum Bid Price
            call_option = option_chain[(option_chain.option_type == call) & # FIlter by Option Type
                                       (option_chain.delta <= delta_value) & # Filter by Delta Value
                                       (option_chain.bid1Price > min_bid_price) & # Filter by Minimum Bid Price
                                       (abs(option_chain.markPrice - option_chain.bid1Price) <= (
                                               mark_price_diff * option_chain.markPrice))].copy() # Filter by Max Mark Price Difference

            # Filter the Primary DataFrame for PUT Options, PUT Delta Value & Minimum Bid Price
            put_option = option_chain[(option_chain.option_type == put) & # FIlter by Option Type
                                      (option_chain.delta >= (-1 * delta_value)) & # Filter by Delta Value
                                      (option_chain.bid1Price > min_bid_price) & # Filter by Minimum Bid Price
                                      (abs(option_chain.markPrice - option_chain.bid1Price) <= (
                                                  mark_price_diff * option_chain.markPrice))].copy() # Filter by Max Mark Price Difference

            print(f"Length of Call Option is : {len(call_option)}",
                  f"Length of Call Option is : {len(put_option)}",
                  f"Current mark_price_diff is : {mark_price_diff}")

            if (len(call_option) == 0) or (len(put_option) == 0):

                mark_price_diff = mark_price_diff + mark_price_diff_steps
                mark_price_diff = round(mark_price_diff, 3)
                print(f"New mark_price_diff is : {mark_price_diff}")

            else:
                break
        # Raise an Exception in case of eny Error
    # Catch the Exception as the Exception Variable "e"
    except Exception as e:
        # Print the Exception Variable "e" for Logging Purposes
        print(f" ### Following Error Occurred while Filtering the CALL & PUT Options : {e}")

    # Check if the Length of the Filtered CALL & PUT Options DataFrame is greater than or equal to 1
    # It will imply that we have some DATA into Extracted DataFrame or Else Exit the Function by Printing Error Code
    if len(call_option) and len(put_option) >= 1:
        # Initiate Exceptional Handling Using Try Block, as we may encounter Data Error
        # As we have Multiple Data Manipulations & Extractions
        try:
            # Print the Total Length of the CALL & PUT Options Blocks Extracted from the Primary DataFrame
            print(f"Length of Call Option is : {len(call_option)}, Length of Put Option is : {len(put_option)} \n")

            # Compute the Minimum PUT Delta and Maximum CALL Delta as PUT Delta is Negative & CALL Delta is Positive
            put_delta = abs(put_option.delta.min())
            call_delta = abs(call_option.delta.max())

            # Print teh PUT & CALL Delta Values
            print(f"Minimum PUT Delta : {put_option.delta.min()} , Absolute Value : {put_delta}")
            print(f"Maximum CALL Delta : {call_option.delta.max()} , Absolute Value : {call_delta} \n")

            # Extract PUT Options with Minimum CALL Delta Values
            # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.abs.html
            temp_put_primary = put_option.iloc[(put_option['delta'] + call_delta).abs().argsort()[:1]]
            # Extract CALL Options with Minimum CALL Delta Values
            temp_call_primary = call_option.iloc[(call_option['delta'] - call_delta).abs().argsort()[:1]]

            # Print the Extracted Primary PUT Options Details
            print(f"Primary PUT Strike : {temp_put_primary.iloc[0]['strike_price']}, "
                  f"with Delta Value : {temp_put_primary.iloc[0]['delta']} ")
            # Print the Extracted Primary CALL Options Details
            print(f"Primary CALL Strike : {temp_call_primary.iloc[0]['strike_price']}, "
                  f"with Delta Value : {temp_call_primary.iloc[0]['delta']}  \n")

            # Extract PUT Options with Minimum PUT Delta Values
            temp_put_secondary = put_option.iloc[(put_option['delta'] + put_delta).abs().argsort()[:1]]
            # Extract CALL Options with Minimum PUT Delta Values
            temp_call_secondary = call_option.iloc[(call_option['delta'] - put_delta).abs().argsort()[:1]]

            # Print the Extracted Secondary PUT Options Details
            print(f"Secondary PUT Strike : {temp_put_secondary.iloc[0]['strike_price']}, "
                  f"with Delta Value : {temp_put_secondary.iloc[0]['delta']} ")
            # Print the Extracted Secondary CALL Options Details
            print(f"Secondary CALL Strike : {temp_call_secondary.iloc[0]['strike_price']}, "
                  f"with Delta Value : {temp_call_secondary.iloc[0]['delta']}  \n")

            # Compute the Delta Difference between the Primary Candidates
            primary_delta_deviation = abs(abs(temp_put_primary.iloc[0]['delta']) -
                                          abs(temp_call_primary.iloc[0]['delta']))
            # Print the Primary Delta Deviation
            print(f"Primary Delta Deviation : {primary_delta_deviation}")

            # Compute the Delta Difference between the Secondary Candidates
            secondary_delta_deviation = abs(abs(temp_put_secondary.iloc[0]['delta']) -
                                            abs(temp_call_secondary.iloc[0]['delta']))
            # Print the Secondary Delta Deviation
            print(f"Secondary Delta Deviation : {secondary_delta_deviation}")

            # Compare if the Primary Delta Deviation is Greater than Secondary Delta Deviation
            if primary_delta_deviation > secondary_delta_deviation:
                print(f"Best PUT SYMBOL Recommendation : {temp_put_secondary.index} \n")

                print(f"Best CALL SYMBOL Recommendation : {temp_call_secondary.index} \n")

                print(f"PUT Strike : {temp_put_secondary.iloc[0]['strike_price']}, "
                      f"with Delta Value : {temp_put_secondary.iloc[0]['delta']}, "
                      f"Expected Options Premium : {temp_put_secondary.iloc[0]['bid1Price']} \n")

                print(f"CALL Strike : {temp_call_secondary.iloc[0]['strike_price']}, "
                      f"with Delta Value : {temp_call_secondary.iloc[0]['delta']}, "
                      f"Expected Options Premium : {temp_call_secondary.iloc[0]['bid1Price']} \n")

                call_strike = temp_call_secondary
                put_strike = temp_put_secondary

            # Compare if the Secondary Delta Deviation is Greater than Primary Delta Deviation
            else:
                print(f"Best PUT SYMBOL Recommendation : {temp_put_primary.index} \n")

                print(f"Best CALL SYMBOL Recommendation : {temp_call_primary.index} \n")

                print(f"PUT Strike : {temp_put_primary.iloc[0]['strike_price']}, "
                      f"with Delta Value : {temp_put_primary.iloc[0]['delta']} , "
                      f"Expected Options Premium : {temp_put_primary.iloc[0]['bid1Price']}  \n")

                print(f"CALL Strike : {temp_call_primary.iloc[0]['strike_price']}, "
                      f"with Delta Value : {temp_call_primary.iloc[0]['delta']}, "
                      f"Expected Options Premium : {temp_call_primary.iloc[0]['bid1Price']} \n")

                call_strike = temp_call_secondary
                put_strike = temp_put_secondary

            end = time.perf_counter()
            print(f"Executing Time of Options Position for : {call_strike.iloc[0]["expiry"].date()} "
                  f"Recommendations is : {(end - start) * 10 ** 3} ms \n")
        except Exception as e:
            print(f"Error Occurred with Following Exception : {e}")
    else:
        print(f"### Cannot Process the Options Chain for Position Recommendation For Expiry : "
              f"as there are NO Positions with matching criteria ### \n")
        # f"{option_chain.iloc[0]["expiry"].date()}, "
        end = time.perf_counter()
        print(f"Executing Time of Exiting the Position Recommendation is : {(end - start) * 10 ** 3} ms \n")

    return call_strike, put_strike
