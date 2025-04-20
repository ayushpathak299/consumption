import requests
import json
import time
from datetime import datetime, timedelta
from google.cloud import bigquery
import os


class NrOrgData:
    newrelic_key = os.getenv("NEWRELIC_API_KEY")
    if not newrelic_key:
        print("❌ ERROR: New Relic API Key is missing!")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "auth.json"

    # Define the date explicitly for January 1st, 2024

    yesterday = datetime.now() - timedelta(days=1)
    starttime = yesterday.strftime("%Y-%m-%d 00:00:00")
    # starttime = "2025-03-16 00:00:00"

    url = "https://api.newrelic.com/graphql"
    i, j, k = 0, 15, 0
    metricdate = datetime.strptime(starttime, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")

    total_gigabytes = 0  # Initialize total sum of GigabytesIngested

    while k < 96:  # 96 iterations to cover a full 24-hour period in 15-minute intervals
        st = (datetime.strptime(starttime, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S")
        et = (datetime.strptime(starttime, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=j)).strftime("%Y-%m-%d %H:%M:%S")

        # Query for fetching data from NewRelic for the specified time range
        masterquery = "{actor{account(id: 67421){nrql(query: \" SELECT * from NrConsumption since 'starttime' UNTIL 'endtime' limit max \") {results}}}}"
        runquery = masterquery.replace("starttime", st).replace("endtime", et)
        print(f"metricdate: {metricdate} and query={runquery}")

        headers = {"Accept": "application/json", "API-Key": newrelic_key}
        client = bigquery.Client()
        table_id = "pmodatabase-398513.newreliccrondata.projectconsumption_final_cleaned"

        response = requests.post(url, data=runquery, headers=headers)
        response_data = response.json()
        orgdata = []
        try:
            if response_data and isinstance(response_data, dict):
                if "data" in response_data and response_data["data"]:
                    if "actor" in response_data["data"] and response_data["data"]["actor"]:
                        if "account" in response_data["data"]["actor"] and response_data["data"]["actor"]["account"]:
                            if "nrql" in response_data["data"]["actor"]["account"] and \
                                    response_data["data"]["actor"]["account"]["nrql"]:
                                if "results" in response_data["data"]["actor"]["account"]["nrql"] and \
                                        response_data["data"]["actor"]["account"]["nrql"]["results"]:
                                    orgdata = response_data["data"]["actor"]["account"]["nrql"]["results"]
        except Exception as ex:
            print(f"❌ Error while processing response: {ex}")

        rows_to_insert = []
        updatetime = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

        for data in orgdata:
            gigabytes = data.get("GigabytesIngested", -1)

            if gigabytes != -1:  # Exclude -1 values when summing
                total_gigabytes += gigabytes


            jsonobj = {
                "GigabytesIngested": gigabytes,
                "BytesIngested": data.get("BytesIngested", -1),
                "consumingAccountId": data.get("consumingAccountId", -1),
                "consumingAccountName": data.get("consumingAccountName", ""),
                "consumption": data.get("consumption", -1),
                "customerId": data.get("customerId", ""),
                "dimensions": data.get("dimensions", ""),
                "ignoredConsumption": data.get("ignoredConsumption", -1),
                "masterAccountId": data.get("masterAccountId", -1),
                "masterAccountName": data.get("masterAccountName", ""),
                "metric": data.get("metric", ""),
                "month": data.get("month", ""),
                "monthTimestamp": data.get("monthTimestamp", -1),
                "organizationGroupId": data.get("organizationGroupId", ""),
                "organizationId": data.get("organizationId", ""),
                "productLine": data.get("productLine", ""),
                "timestamp": data.get("timestamp", -1),
                "type": data.get("type", ""),
                "usageMetric": data.get("usageMetric", ""),
                "version": data.get("version", ""),
                "metricdate": metricdate,
                "updatetime": updatetime,
            }
            rows_to_insert.append(jsonobj)

        # Insert into BigQuery
        if rows_to_insert:
            errors = client.insert_rows_json(table_id, rows_to_insert)
            if not errors:
                print(f"✅ Data for {st} - {et} inserted successfully into projectconsumption_final table")
            else:
                print(f"❌ Errors while inserting rows: {errors}")

        k += 1
        i += 15
        j += 15

    # Print the final total sum of GigabytesIngested
    print(f"📊 Total Gigabytes Ingested (excluding -1): {total_gigabytes}")

