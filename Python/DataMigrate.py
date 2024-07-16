import sys
import numpy as np
from MongoDB import MongoDBManager
import pandas as pd
from datetime import datetime, date
import calendar


def insert_legend_data(database_n, tblname):
    legend_data = pd.read_feather("../../peskas.kenya.data.pipeline/inst/data/legacy_dat_processed.feather")

    legend_data['_id'] = legend_data['landing_id']
    print(len(legend_data))
    print(legend_data)

    legend_data['landing_date'] = pd.to_datetime(legend_data['landing_date'])

    # Converting pandas datetime to Unix timestamp
    legend_data['landing_date'] = legend_data['landing_date'].apply(lambda x: x.timestamp())

    legend_data_json = legend_data.to_dict(orient='records')
    print(legend_data_json[:100])

    db_helper = MongoDBManager(database_n, tblname)  # MongoDBHandler(db_connector())
    print(len(legend_data_json))
    db_helper.insert_multiple_documents(legend_data_json)


if __name__ == "__main__":
    database_name = 'kenya'
    table_name = 'legend_data'
    insert_legend_data(database_name, table_name)


