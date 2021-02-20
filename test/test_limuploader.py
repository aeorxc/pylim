import pandas as pd
from datetime import date
from datetime import timedelta

import pytest

from pylim import lim
from pylim import limuploader


def test_upload_series():
    # Arrange.
    spot_price1 = 1.21
    spot_price2 = 4.89
    today = date.today()
    # don't work with weekends so make weekday
    if today.weekday() == 5 or today.weekday() == 6:
        today = today + timedelta(days=-2)

    data = {
        'TopRelation:Test:SPOTPRICE1;TopColumn:Price:Close': spot_price1,
        'TopRelation:Test:SPOTPRICE2': spot_price2,
    }
    upload_df = pd.DataFrame(data, index=[today])
    dfmeta = {'description': 'desc'}
    # Act.
    limuploader.upload_series(upload_df, dfmeta)
    download_df = lim.series(['SPOTPRICE1', 'SPOTPRICE2'])
    # Assert
    assert download_df.loc[f"{today:%Y-%m-%d}"]['SPOTPRICE1'] == pytest.approx(spot_price1)
    assert download_df.loc[f"{today:%Y-%m-%d}"]['SPOTPRICE2'] == pytest.approx(spot_price2)
