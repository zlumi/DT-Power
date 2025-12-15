import os

# Data Configuration
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
DEFAULT_DATA_FILE = "Continuous_Orders-NL-20210626-20210628T042947000Z.csv"
FILEPATH = os.path.join(DATA_DIR, DEFAULT_DATA_FILE)

# App Configuration
PAGE_LAYOUT = "wide"
