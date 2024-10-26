from datetime import timedelta, date, datetime
from typing import Any


def generate_expiry_dates(weekly_count: int = 4,
                          monthly_count: int = 4,
                          quarterly_count: int = 4,
                          today: datetime.date = date.today(),
                          duplicates:bool = False,):
    """

    :param duplicates:
    :param weekly_count: Number of Weekly Expiry Dates that to be Returned
    :param monthly_count: Number of Monthly Expiry Dates that to be Returned
    :param quarterly_count: Number of Quarterly Expiry Dates that to be Returned

    :param today: The Current Date From which the Weekly, Monthly & Quarterly Expiry Dates to be Calculated

    :return: weekly_expiry_dates: List of Weekly Expires dates
    :return: monthly_expiry_dates: List of Monthly Expires dates
    :return: quarterly_expiry_date_list: List of Quarterly Expires dates

    """
    # List of Month Values that constitute the Quarterly Value Parameters
    quarterly_list = [3, 6, 9, 12]
    # Empty List to hold Weekly Expires
    weekly_expiry_date_list: list[timedelta | date | None | Any] = []
    # Empty List to hold Monthly Expires
    monthly_expiry_date_list: list[timedelta | date | None | Any] = []
    # Empty List to hold Quarterly Expires
    quarterly_expiry_date_list: list[timedelta | date | None | Any] = []
    # Initializing the Weekly_Expiry Variable with None Value
    weekly_expiry = None
    first_event = True

    offset = (today.weekday() - 4) % 7
    friday = today - timedelta(days=offset)
    friday = friday + timedelta(days=7)

    if friday < today:
        weekly_expiry = friday + timedelta(days=7)

    elif friday > today:
        weekly_expiry = friday

    current_month = weekly_expiry.month
    print("Initial Current Month is ", current_month)

    while len(quarterly_expiry_date_list) <= quarterly_count:
        # print("Entering While Loop")
        if weekly_expiry.month == current_month:
            weekly_expiry_date_list.append(weekly_expiry)
            weekly_expiry = weekly_expiry + timedelta(days=7)

        else:
            monthly = weekly_expiry_date_list[-1]
            if not duplicates:
                weekly_expiry_date_list.pop(-1)

            monthly_expiry_date_list.append(monthly)
            current_month = weekly_expiry.month
            weekly_expiry = monthly + timedelta(days=7)

            if monthly_expiry_date_list[-1].month in quarterly_list:
                # This If Statements to ensure Every First Quartely Expiry is Marked into Monthly Expiry
                if first_event is True:
                    first_event = False
                else:
                    quarterly_expiry_date_list.append(monthly)
                # Duplicate Detection is not Required as
                # Eventually Duplicates will be elininated due to the First Event Pass
                """
                if not duplicates:
                    monthly_expiry_date_list.pop(-1)
                # print(f"Quarterly Expiry Date : {str(quarterly_expiry_date_list[-1])}")
                # print(f"Quarterly Expiry Month is: {str(quarterly_expiry_date_list[-1].month)}")
                """
            # print(f"Monthly Expiry Date : {str(monthly_expiry_date_list[-1])}")

    print(f"Original Length of Weekly Expiry List Array : {len(weekly_expiry_date_list)}, "
          f"But Length of returned Array : {len(weekly_expiry_date_list[:weekly_count])} ")
    print(f"Original Length of Monthly Expiry List Array : {len(monthly_expiry_date_list)}, "
          f"But Length of returned Array : {len(monthly_expiry_date_list[:monthly_count])} ")
    print(f"Original Length of Weekly Expiry List Array : {len(quarterly_expiry_date_list)}, "
          f"But Length of returned Array : {len(quarterly_expiry_date_list[:quarterly_count])} ")

    return (weekly_expiry_date_list[:weekly_count],
            monthly_expiry_date_list[:monthly_count],
            quarterly_expiry_date_list[:quarterly_count])



if __name__ == "__main__":
    (weekly_expiry_dates, monthly_expiry_dates, quarterly_expiry_dates) = generate_expiry_dates(weekly_count = 5,
                                                                                                monthly_count = 5,
                                                                                                quarterly_count = 5,
                                                                                                today=date.today(),
                                                                                                duplicates=False)
    i = 0
    for i in range(len(weekly_expiry_dates)):
        print(f"Weekly {i} Expiry Dates : {weekly_expiry_dates[i]}")
        i += 1

    j = 0
    for j in range(len(monthly_expiry_dates)):
        print(f"Monthly {j} Expiry Dates : {monthly_expiry_dates[j]}")
        j += 1

    k = 0
    for k in range(len(quarterly_expiry_dates)):
        print(f"Quarterly {k} Expiry Dates : {quarterly_expiry_dates[k]}")
        k += 1
