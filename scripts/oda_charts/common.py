import pandas as pd
from oda_data import OECDClient, set_data_path, provider_groupings

from scripts import config

set_data_path(config.Paths.oda_cache)

LAST_UPDATE = pd.Timestamp.now().strftime("%Y-%m-%d")


def get_gni(providers: int | list[int], base_year: int = 2023) -> pd.DataFrame:
    """
    Get the total aid to Africa from the OECD database.
    """
    # Create an OECDClient instance with the specified parameters

    client = OECDClient(providers=providers, base_year=base_year)
    return client.get_indicators(indicators="DAC1.40.1").assign(last_update=LAST_UPDATE)


if __name__ == "__main__":
    # DAC countries GNI
    dac_countries = list(provider_groupings()["dac_countries"])
    dac_gni = get_gni(providers=dac_countries, base_year=2023)
    dac_gni.to_csv(config.Paths.output / "dac_gni_2023_constant.csv", index=False)
