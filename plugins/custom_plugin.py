from airflow.plugins_manager import AirflowPlugin
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
#for custom hooks
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from pathlib import Path

import requests
import csv

class CovidDailyDataSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self,api_public_url, *args, **kwargs):
        self.api_public_url=api_public_url
        super().__init__(*args, **kwargs)

    def poke(self,context):
        date= context['ti'].xcom_pull(key='date',task_ids='get_execution_date')
        daily_summary_available=self._check_daily_data(
            api_public_url=self.api_public_url, 
            date=date
        )
        print(f"Checking available data of date: {date}")
        return daily_summary_available

    def _check_daily_data(self, api_public_url: str, date: str):
        """Check if daily data available.

        Args:
            api_public_url (str): api url
            date: str

        Returns:
            bool
        """
        daily_summary_req = requests.post(
            url=f"{api_public_url}/{date}"          
        )
        if daily_summary_req.status_code == 404:
            return False
        if daily_summary_req.ok:
            return True

        raise requests.HTTPError(f"Failed to check {date}'s data: {daily_summary_req.status_code}")



class APIToPostgreSQLHook(BaseHook):
    def __init__(self):
        print("##custom hook started##")

    def copy_table(self, postgres_conn_id, api_public_url, date, rows):

        print("### fetching records from API URL ###")
        data_set=self._download_daily_data(api_public_url, date)

        filtered_data_set = self.filterTheDict(data_set, lambda key_value : key_value[0] in rows )

        print("### inserting records into PostgreSQL table ###")
        postgresserver = PostgresHook(postgres_conn_id)
        
        values=[list(obj.values()) for obj in filtered_data_set]
        postgresserver.insert_rows(table="daily_covid_data",
                                replace_index="ID",
                                rows=values, 
                                target_fields=["ID","COUNTRY_REGION", "CONFIRMED", "DEATHS", "RECOVERED", "UPDATED_DATE"], 
                                replace=True
                                )

        return True

    def _download_daily_data(self, api_public_url: str, date: str):
        """Download daily data.

        Args:
            api_public_url (str): api url
            date: str

        Returns:
            DailyData: Json Object
        """
        daily_summary_req = requests.post(
            url=f"{api_public_url}/{date}"          
        )

        if daily_summary_req.ok:
            return daily_summary_req.json()
            
        raise requests.HTTPError(f"Failed to download {date}'s data: {daily_summary_req.status_code}")

    def filterTheDict(self,jsonList, callback):
        filtered_json_list=[]
        for dictObj in jsonList:
            newDict = dict()
            for (key, value) in dictObj.items():
                if callback((key, value)):
                    newDict[key] = value
                if len(newDict) == 6:
                    print(newDict.keys())
                    print(newDict)
                    OrderedDict={"id": newDict["provinceState"]+newDict["countryRegion"] + newDict["confirmed"] + newDict["deaths"] + newDict["recovered"],
                                 'countryRegion':newDict["countryRegion"],
                                 "confirmed": 0 if newDict["confirmed"]=='' else int(newDict["confirmed"]),
                                 "deaths": 0 if newDict["deaths"]=='' else int(newDict["deaths"]) ,
                                 "recovered": 0 if newDict["recovered"]=='' else int(newDict["recovered"]) ,
                                 'lastUpdate':newDict["lastUpdate"], }
                    filtered_json_list.append(OrderedDict)
        return filtered_json_list

class PostgreSQLToCsv(BaseHook):
    def __init__(self):
        print("##custom hook started##")

    def copy_table(self, postgres_conn_id, path, date):
        

        print("### fetching records from PostgreSQL table ###")
        postgresserver = PostgresHook(postgres_conn_id)
        sql_query = "SELECT COUNTRY_REGION as Country, UPDATED_DATE as Date, SUM(CONFIRMED) AS Total_confirmed_cases, SUM(DEATHS) AS Deaths, SUM(RECOVERED) AS Recovered FROM daily_covid_data WHERE UPDATED_DATE = DATE '%s' GROUP BY COUNTRY_REGION, UPDATED_DATE ORDER BY Total_confirmed_cases DESC;" % date
        data = postgresserver.get_records(sql_query)
        Path(path).mkdir(exist_ok=True, parents=True)
        contruct_full_path=path + "/cases_per_country" + date + ".csv"
        with open(contruct_full_path,'w') as out:
            csv_out=csv.writer(out)
            csv_out.writerow(['Country','Date','Total_confirmed_cases','Deaths','Recovered'])
            csv_out.writerows(list(data))

        return True

    

    

class CovidCustomPlugin(AirflowPlugin):
    name = "covid_plugin"
    operators = []
    sensors = [CovidDailyDataSensor]
    hooks = [APIToPostgreSQLHook,PostgreSQLToCsv]