import datetime
import json
from multiprocessing import dummy
from google.cloud import bigquery
import pytz

PROJECT_ID = "trial-project-star-capital"
DATASET_ID = "CDC"
TABLE_ID = "cdc-test-raw"

client = bigquery.Client()

dummy_data = {
  "before": {
    "id": 39,
    "name": "Guri",
    "created_at": 1652252473773807,
    "updated_at": 1652252473773807
  },
  "after": {
    "id": 39,
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

dummy_data_2 = {
  "before": {
    "id": 1,
    "name": "Damian",
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
    "table": "testings",
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

table_prefix = "raw_cdc"

def routing_table(prefix: str =table_prefix) -> dict:
    table_list = [("testing", "testing"), ("random", "random")]
    route = {elem[0]: "_".join([prefix, elem[1]]) for elem in table_list}
    return route

def shallow_flatten(data:dict, attr_name:str, is_null:bool=False) -> dict:
    if data[attr_name] is None:
        return data
    else:
        for key, val in data[attr_name].items():
            data[attr_name+"."+key] = None if is_null else val
    return data

def epoch_to_datetime(data:dict) -> dict:
    time_key_list = ["ts_ms","create", "update"]
    tz = pytz.timezone('Asia/Jakarta')
    data = {key: datetime.datetime.fromtimestamp(val/1000, tz).strftime('%Y-%m-%d %H:%M:%S') if 
        any(sub in key for sub in time_key_list) and (isinstance(val,int) or isinstance(val,float)) 
        else val for key, val in data.items()}
    return data


def process_data(data: dict) -> dict:
    final_data = data["payload"].copy() if "payload" in data.keys() else data.copy()
    # before_null = False
    # after_null = False
    final_data["transaction"] = str(final_data.get("transaction")) if final_data.get("transaction") is not None else None
    # if final_data["before"] is None and final_data["after"] is not None:
    #     final_data["before"] = final_data["after"]
    #     before_null=True
    # elif final_data["after"] is None and final_data["before"] is not None:
    #     final_data["after"] = final_data["before"]
    #     after_null=True
    # final_data = shallow_flatten(final_data, "after", after_null)
    # final_data = shallow_flatten(final_data, "before", before_null)
    final_data = shallow_flatten(final_data, "source")
    xmin = final_data["source.xmin"] 
    final_data["source.xmin"] = None if not(isinstance(xmin, int)) or not(isinstance(xmin, float)) else float(xmin)
    # final_data["source.ts_ms"] = datetime.datetime.fromtimestamp(final_data.get("source.ts_ms")/1000, tz).strftime('%Y-%m-%d %H:%M:%S')
    # final_data["cdc_ts"] = datetime.datetime.fromtimestamp(final_data.get("ts_ms")/1000, tz).strftime('%Y-%m-%d %H:%M:%S')
    final_data["source.snapshot"] = False if final_data.get("source.snapshot").lower() == "false" else True
    final_data = epoch_to_datetime(final_data)
    del final_data["source"]
    return final_data

def send_into_bq(data:dict, route: dict=routing_table()):
    rows_to_insert = [data]
    if data['source.table'] not in route:
        print("Source is not registered in the table router")
    else:
        bq_table = route[data['source.table']]
        print(f"{PROJECT_ID}.{DATASET_ID}.{bq_table}")
    # errors = client.insert_rows_json(
    #     f"{PROJECT_ID}.{DATASET_ID}.{route[data['source.table']]}", rows_to_insert
    # )
    # if errors == []:
    #     print("New rows have been added.")
    # else:
    #     print("Encountered errors while inserting rows: {}".format(errors))


def main():
    mydata = process_data(dummy_data_2)
    # Add json loads here
    print(json.dumps(mydata, indent=4, default=str))
    send_into_bq(mydata)
   
if __name__ == "__main__":
    main()


