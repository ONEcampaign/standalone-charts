from pathlib import Path


class Paths:
    """Class to store the paths to the data and output folders."""

    project = Path(__file__).resolve().parent.parent
    output = project / "output"
    scripts = project / "scripts"
    oda_charts = scripts / "oda_charts"
    oda_cache = oda_charts / ".cache"
