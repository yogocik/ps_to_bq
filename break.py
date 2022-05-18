import datetime
import json
from multiprocessing import dummy
from google.cloud import bigquery
import pytz
import typing

dummy_data = {
  "before": {
    "id": 2,
    "name": "Gunandar",
    "created_at": 1652252473773807,
    "updated_at": 1652252473773807
  },
  "after": {
    "id": 2,
    "name": "Ginyu",
    "created_at": 1652252473773807,
    "updated_at": 1652341039620215
  },
  "source": {
    "version": "1.9.2.Final",
    "connector": "postgresql",
    "name": "cdc_test",
    "ts_ms": 1652341039620,
    "snapshot": "false",
    "db": "data_cdc_testdb",
    "sequence": "[\"9790049520\",\"9790051144\"]",
    "schema": "public",
    "table": "testing",
    "txId": 2611786,
    "lsn": 9790051144,
    "xmin": ""
  },
  "op": "u",
  "ts_ms": 1652341039944,
  "transaction": ""
}

dummy_data_cc = {
  "before": None,
  "after": {
    "id": 2,
    "name": "Gunandar",
    "created_at": 165225247377498,
    "updated_at": 1652341039620498
  },
  "source": {
    "version": "1.9.2.Final",
    "connector": "postgresql",
    "name": "cdc_test",
    "ts_ms": 1652341039498,
    "snapshot": "false",
    "db": "data_cdc_testdb",
    "sequence": "[\"9790049520\",\"9790051144\"]",
    "schema": "public",
    "table": "testing",
    "txId": 2611786,
    "lsn": 9790051144,
    "xmin": ""
  },
  "op": "c",
  "ts_ms": 1652341039520,
  "transaction": ""
}

dummy_data_2 = {
  "before": {
    "id": 1,
    "name": "Damian",
    "code": "XAM",
    "created_at": 1652257309034061,
    "updated_at": 1652257309034061
  },
  "after": None,
  "source": {
    "version": "1.9.2.Final",
    "connector": "postgresql",
    "name": "cdc_test",
    "ts_ms": 1652262089153,
    "snapshot": "false",
    "db": "data_cdc_testdb",
    "sequence": "[\"9658244328\",\"9658244528\"]",
    "schema": "public",
    "table": "random",
    "txId": 2588573,
    "lsn": 9658244528,
    "xmin": ""
  },
  "op": "d",
  "ts_ms": 1652262089531,
  "transaction": None
}

payload_data = {
    "payload": dummy_data_2
}

# COPY FROM HERE INTO CLOUD FUNCTION

client = bigquery.Client()

PROJECT_ID = "trial-project-star-capital"                   # BigQuery Project ID
DATASET_ID = "CDC"                                          # BigQuery Dataset ID
table_prefix = "cdc_raw"                                    # BigQuery table's prefix for raw dataset
table_list = [("testing", "testing"), ("random", "random")] # [(source_table_name, bq_table_name)] source-dataset table pairs for routing
table_key_index = {"testing": ["id"],
                   "random": ["id", "code"]}                # List of key index in certain sources include primary or composite key
data_change_attr = ["after","before"]                       # Message body attribute where data change log was written
time_key_list = ["ts_ms","create", "update"]                # List of substring of key attribute for time data in source payload
tz = pytz.timezone('Asia/Jakarta')                          # Predefined timezone for time conversion

def routing_table(router: typing.List[typing.Tuple[str,str]] = table_list, 
                  prefix: str =table_prefix) -> dict:
    """Route source data into specific big query table

    Args:
        router (typing.List[typing.Tuple[str,str]], optional): list of tuple contains of source-bq_table pairs
        prefix (str, optional): Big Query table prefix

    Returns:
        dict: modified data in dict format (source payload)
    """
    route = {elem[0]: "_".join([prefix, elem[1]]) for elem in router}
    return route

def shallow_flatten(data:dict, attr_name:str, is_null:bool=False, 
                    sep:str="_") -> dict:
    """ Flat-nested (only one-level deep, not recursive way) of dictionary data (source payload)

    Args:
        data (dict): source payload
        attr_name (str): target attribute name to be flatten_
        is_null (bool, optional): give null value to all key of related attribute data if true.
        sep (str, optional): prefix for attribute key-value data.

    Returns:
        dict: modified data in dict format (source payload)
    """
    if data[attr_name] is None:
        return data
    else:
        for key, val in data[attr_name].items():
            data[attr_name+sep+key] = None if is_null else val
    return data


def extract_key_index(data:dict, table_name_attr:str="source_table" ,
                      attr_search_list: typing.List[str]=data_change_attr) -> dict:
    """Extract key index from log data in source payload

    Args:
        data (dict): source/dataset payload
        table_name_attr (str, optional): name of table attribute in source payload 
        attr_search_list (typing.List[str], optional): search list of log data attribute

    Returns:
        dict: modified data in dict format (source payload)
    """
    for ch_attr in attr_search_list:
        if data[ch_attr] is None:
            continue
        else:
            for key in table_key_index[data[table_name_attr]]:
                data[key] = data[ch_attr][key] 
    return data


def convert_epoch_to_datetime(data:dict,prefix:str="dt", sep: str="_", 
                              tz: object = tz,
                              time_key_list: typing.List[str]=time_key_list) -> dict:
    """Convert epoch timestamp into datetime with timezone.

    Args:
        data (dict): source payload
        prefix (str, optional): prefix for converted attribute data (create new one)
        sep (str, optional): separator for prefix and related time data key
        tz (object, optional): timezone for conversion
        time_key_list (typing.List[str], optional): List of substring of time data attribute in source payload

    Returns:
        dict: modified data in dict format (source payload)
    """
    placeholder = {}
    for key, val in data.items():
        if any(sub in key for sub in time_key_list) and (
            isinstance(val,int) or isinstance(val,float)):
            placeholder[sep.join([prefix, key])] = datetime.datetime.fromtimestamp(
                val/1000, tz).strftime('%Y-%m-%d %H:%M:%S') 
    return {**placeholder, **data}


def process_data(data: dict) -> dict:
    """ Processing source data like breakdown, conversion, and so on.

    Args:
        data (dict): source payload

    Returns:
        dict: modified source payload
    """
    final_data = data["payload"].copy() if "payload" in data.keys() else data.copy()
    final_data = shallow_flatten(final_data, "source")
    final_data = extract_key_index(final_data)
    final_data["before"] = json.dumps(final_data["before"]) if isinstance(
        final_data["before"],dict) else None 
    final_data["after"] = json.dumps(final_data["after"]) if isinstance(
        final_data["after"],dict) else None
    xmin = final_data["source_xmin"] 
    final_data["source_xmin"] = None if not(isinstance(xmin, int)) or not(
        isinstance(xmin, float)) else float(xmin)
    final_data["source_snapshot"] = False if final_data.get(
        "source_snapshot").lower() == "false" else True
    final_data = convert_epoch_to_datetime(final_data)
    del final_data["source"]
    del final_data["transaction"]
    return final_data

def send_into_bq(data:dict, route: dict=routing_table()):
    """Send processed source data into specific big query table 

    Args:
        data (dict): modified source payload
        route (dict, optional): big query table routing
    """
    rows_to_insert = [data]
    if data['source_table'] not in route:
        print("Source is not registered in the table router")
    else:
        errors = client.insert_rows_json(
            f"{PROJECT_ID}.{DATASET_ID}.{route[data['source_table']]}", rows_to_insert
        )
        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))


def main():
    """Main execution for data processing and loading 
    """
    # Add json loads here
    mydata = process_data(dummy_data)
    print(json.dumps(mydata, indent=4, default=str))
    send_into_bq(mydata)
   
if __name__ == "__main__":
    main()


