'''
Tasks read their storage tiers and credentials from the environment (see
BaseTask's LOCAL_STORAGE_FOLDER / TRANSFER_FOLDER / CLOUD_STORAGE_FOLDER).
main.py loads .env before importing; pytest does not, so without this any test
that instantiates a task fails in populate() on a None folder.
'''

from dotenv import load_dotenv

load_dotenv()
