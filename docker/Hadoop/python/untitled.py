import os

import pandas as pd

from dotenv import dotenv_values
from sqlalchemy import create_engine, inspect

CONFIG = dotenv_values('.env')
if not CONFIG:
    CONFIG = os.environ

connection_uri = "postgresql+psycopg2://{}:{}@{}:{}".format(
    CONFIG["POSTGRES_USER"],
    CONFIG["POSTGRES_PASSWORD"],
    CONFIG['POSTGRES_HOST'],
    CONFIG["POSTGRES_PORT"],
)

engine = create_engine(connection_uri, pool_pre_ping=True)
engine.connect()

dataset = "https://gist.githubusercontent.com/mmphego/5b6fc4d6dc3c8fba4fce9d994a2fe16b/raw/ab5df0e76812e13df5b31e466a5fb787fac0599a/wine_quality.csv"

df = pd.read_csv(dataset)

df.head()