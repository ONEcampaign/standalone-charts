import pandas as pd
from oda_data import OECDClient, set_data_path, provider_groupings

from scripts import config
from scripts.oda_charts.common import LAST_UPDATE

set_data_path(config.Paths.oda_cache)

BASE_YEAR = 2023



def get_total_aid_to_africa(providers: int | list[int]) -> pd.DataFrame:
    """
    Get the total aid to Africa from the OECD database.
    """
    # Create an OECDClient instance with the specified parameters

    client = OECDClient(providers=providers, base_year=BASE_YEAR, recipients=10001)
    return client.get_indicators(indicators="ONE.10.206_106").assign(
        last_update=LAST_UPDATE
    )


def get_bilateral_aid_to_africa(providers: int | list[int]):
    client = OECDClient(providers=providers, base_year=BASE_YEAR, recipients=10001)
    return client.get_indicators(indicators="DAC2A.10.206").assign(
        last_update=LAST_UPDATE
    )


if __name__ == "__main__":
    # G7 total
    g7 = get_total_aid_to_africa(20003)
    g7.to_csv(config.Paths.output / "g7_aid_to_africa.csv", index=False)

    # DAC Countries total
    dac = get_total_aid_to_africa(20001)
    dac.to_csv(config.Paths.output / "dac_aid_to_africa.csv", index=False)

    dac_countries = list(provider_groupings()["dac_countries"])
    dac_c = get_total_aid_to_africa(dac_countries)
    dac_c.to_csv(config.Paths.output / "dac_countries_aid_to_africa.csv", index=False)


    # All official bilat
    total = get_bilateral_aid_to_africa(20005)
    total.to_csv(config.Paths.output / "official_aid_to_africa.csv", index=False)
