import os
import pandas as pd
from . import BaseTask
from meteodatalab import ogd_api

from earthkit.data import config
config.set("cache-policy", "temporary")


def timedelta_to_iso8601(td: pd.Timedelta) -> str:
    """Convert pandas Timedelta to ISO 8601 duration format (e.g., 'P1DT2H30M')."""
    total_seconds = int(td.total_seconds())
    
    days = total_seconds // 86400
    remainder = total_seconds % 86400
    hours = remainder // 3600
    remainder %= 3600
    minutes = remainder // 60
    seconds = remainder % 60
    
    iso = f"P{days}D"  # Always include days, even if 0
    iso += "T"
    if hours or minutes or seconds:
        if hours:
            iso += f"{hours}H"
        if minutes:
            iso += f"{minutes}M"
        if seconds:
            iso += f"{seconds}S"
    else:
        iso += "0H"  # Default to 0H if no time component
    
    return iso

class Retrieve_ICON_CH1_EPS(BaseTask):
    MEMORY = pd.Timedelta('1d')
    LEADTIMES = [timedelta_to_iso8601(td) for td in pd.timedelta_range(start='0h', end='48h', freq='1h')]

    def __init__(self, variable, reference_time="latest", *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.variable = variable
        self.reference_time = reference_time

    def populate(self):
        print("Populating ICON CH1 EPS data...")

        req = ogd_api.Request(
            collection="ogd-forecasting-icon-ch2",
            variable=self.variable,
            ref_time="latest",
            perturbed=False,
            lead_time=self.LEADTIMES,
        )

        return req

    def retrieve(self, req):
        print("Downloading ICON CH1 EPS data...")

        import requests
        from pathlib import Path
        
        # Build URLs manually from request parameters
        # OGD API pattern: https://ogd.data.admin.ch/{collection}/{variable}/{ref_time}/{horizon}
        base_url = "https://ogd.data.admin.ch"
        
        # Get reference time (resolve "latest" if needed)
        if req.reference_datetime == "latest":
            # Query the API for the latest available reference time
            collection_url = f"{base_url}/{req.collection}"
            response = requests.get(collection_url)
            response.raise_for_status()
            # Parse response to get latest ref_time - this depends on API format
            # For now, we'll try to proceed with "latest" or implement proper parsing
            ref_time = "latest"
        else:
            ref_time = req.reference_datetime
        
        # Build URLs for each horizon
        urls = []
        for horizon in req.horizon:
            # Convert timedelta to hours
            hours = int(horizon.total_seconds() / 3600)
            # Build URL - adjust pattern based on actual OGD API structure
            url = f"{base_url}/{req.collection}/{req.variable}/{ref_time}/+{hours:03d}H"
            urls.append(url)
        
        print(f"Generated {len(urls)} URLs")
        
        # Download files manually
        download_dir = Path("data/icon_ch1_eps")
        download_dir.mkdir(parents=True, exist_ok=True)
        
        files = []
        for idx, url in enumerate(urls[:3]):  # Test with first 3 files
            filename = download_dir / f"{req.variable}_{idx:03d}.grib2"
            print(f"Downloading: {url}")
            
            try:
                response = requests.get(url, stream=True, timeout=30)
                response.raise_for_status()
                
                with open(filename, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                files.append(filename)
                print(f"Saved to: {filename}")
            except Exception as e:
                print(f"Error downloading {url}: {e}")
        
        return files

    def process(self):
        print("Processing ICON CH1 EPS data...")

    def delete(self):
        print("Deleting temporary ICON CH1 EPS data...")