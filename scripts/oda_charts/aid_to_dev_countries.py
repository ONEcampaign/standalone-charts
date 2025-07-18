import pandas as pd
from oda_data import OECDClient, set_data_path, provider_groupings, recipient_groupings
from scripts import config
from scripts.oda_charts.common import LAST_UPDATE

set_data_path(config.Paths.oda_cache)

BASE_YEAR = 2023



def get_total_aid_to_dev_countries(providers: int | list[int]) -> pd.DataFrame:
    """
    Get the total aid to Africa from the OECD database.
    """
    # Create an OECDClient instance with the specified parameters

    client = OECDClient(providers=providers, base_year=BASE_YEAR, recipients=10100)
    return client.get_indicators(indicators="ONE.10.206_106").assign(
        last_update=LAST_UPDATE
    )

if __name__ == "__main__":
    dac_countries = list(provider_groupings()["dac_countries"])
    dac_c = get_total_aid_to_dev_countries(dac_countries)
    dac_c.to_csv(config.Paths.output / "dac_countries_aid_to_dev_countries.csv", index=False)

