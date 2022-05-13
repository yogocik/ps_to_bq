import datetime
import json
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
    "table": "testing",
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

def process_data(data: dict) -> dict:
    final_data = data["payload"].copy() if "payload" in data.keys() else data.copy()
    tz = pytz.timezone('Asia/Jakarta')
    final_data["transaction"] = str(final_data.get("transaction")) if final_data.get("transaction") is not None else None
    final_data["before"] = json.dumps(final_data["before"]) if isinstance(final_data["before"],dict) else None 
    final_data["after"] = json.dumps(final_data["after"]) if isinstance(final_data["after"],dict) else None
    xmin = final_data.get("source").get("xmin") if "xmin" in final_data.get("source").keys() else None
    final_data["source"]["xmin"] = None if not(isinstance(xmin, int)) or not(isinstance(xmin, float)) else float(xmin)
    final_data["source_ts"] = datetime.datetime.fromtimestamp(final_data.get("source").get("ts_ms")/1000, tz).strftime('%Y-%m-%d %H:%M:%S')
    final_data["cdc_ts"] = datetime.datetime.fromtimestamp(final_data.get("ts_ms")/1000, tz).strftime('%Y-%m-%d %H:%M:%S')
    final_data["snapshot"] = False if final_data.get("source").get("snapshot").lower() == "false" else True
    final_data["db_name"] = final_data.get("source").get("db")
    final_data["schema"] = final_data.get("source").get("schema")
    final_data["table"] = final_data.get("source").get("table")
    final_data["rep_slot"] = final_data.get("source").get("name")
    return final_data

def send_into_bq(data:dict):
    rows_to_insert = [data]
    errors = client.insert_rows_json(
        f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", rows_to_insert
    )
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))


def main():
    mydata = process_data(payload_data)
    print(json.dumps(mydata, indent=4, default=str))
    send_into_bq(mydata)
   
if __name__ == "__main__":
    main()


