from src.common_functions import generate_expiry_dates
from datetime import datetime


def test_generate_expiry_dates():
    assert True
    sample_date = datetime(day=15, month=5, year=2024)
    weekly_expiry_dates, monthly_expiry_dates, quarterly_expiry_dates = generate_expiry_dates(weekly_count=4,
                                                                                              monthly_count=4,
                                                                                              quarterly_count=4,
                                                                                              today=sample_date.date())
    assert len(weekly_expiry_dates) == 4
    assert len(monthly_expiry_dates) == 4
    assert len(quarterly_expiry_dates) == 4
