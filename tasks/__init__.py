import pandas as pd

class BaseTask:
    REPORT_FAILURE = True
    REPORT_SUCCESS = True
    REPORT_SUCCESS_CYCLE = '00:00:00

    def __init__(self, *args, **kwargs):
        pass

    def report_failure(self, ex):
        print(f"Task failed: {ex}")

    def report_success(self, ex):
        print(f"Task failed: {ex}")

    def upload_to_azure(self, *args, **kwargs):
        print("Uploading to Azure...")

    def cleanup(self):
        print("Cleaning up...")

class Retrieve_ICON_CH1_EPS(BaseTask):
    MEMORY = pd.Timedelta('2d')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def populate(self):
        print("Populating ICON CH1 EPS data...")

    def retrieve(self):
        print("Downloading ICON CH1 EPS data...")

    def process(self):
        print("Processing ICON CH1 EPS data...")

    def delete(self):
        print("Deleting temporary ICON CH1 EPS data...")


class Retrieve_ICON_CH2_EPS(BaseTask):
    pass

class Retrieve_ERA5_LAND(BaseTask):
    pass

class Retrieve_IMERG(BaseTask):
    pass

class Retrieve_GFS(BaseTask):
    pass

class Retrieve_ICON(BaseTask):
    pass

class Retrieve_Wallonie(BaseTask):
    pass