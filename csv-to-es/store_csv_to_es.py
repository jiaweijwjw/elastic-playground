"""
Script which connects to elasticsearch instance and creates a new index from a csv file. Each row in the csv file would be a document in the index.
"""
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import os
from typing import Union

# Create your own .env file on the same level as this script with the following variables:
# ES_USERNAME="your elastic username"
# ES_PASSWORD="your elastic password"

load_dotenv()  # take environment variables from .env file.
ES_USERNAME: str = os.getenv("ES_USERNAME")
ES_PASSWORD: str = os.getenv("ES_PASSWORD")

HOST: str = ""
PORT: int = 9200  # Default port for elasticsearch

CSV_FILE_PATH: str = "data.csv"
INDEX_NAME: str = "my_index"

# For WSL2 connection to elasticsearch instance running locally on Windows via bin/elasticsearch.bat
import subprocess

HOST = subprocess.check_output("hostname").decode("utf-8").strip() + ".local"
# Replace HOST with localhost if running directly on Windows.


class Pipeline:
    def __init__(self, csv_file_path: str, index_name: str):
        self.__es = self.__connect_elasticsearch()
        self.csv_file_path = csv_file_path
        self.index_name = index_name

    def run(self) -> None:
        print("")
        # self.convert()

    # def convert(self):
    #     import csv

    #     with open(self.csv_file_path, "r") as csv_file:
    #         csv_reader = csv.DictReader(csv_file)
    #         for row in csv_reader:
    #             print(row)
    #             self.insert_into_es(row)

    # def insert_into_es(self, row: dict):
    #     my_es.index(index=self.index_name, body=row)

    def __connect_elasticsearch(self) -> Union[Elasticsearch, None]:
        """
        Returns an elasticsearch instance if manages to connect to it, else returns None.
        """
        es = Elasticsearch(
            [
                {
                    "host": HOST,
                    "port": PORT,
                    "scheme": "http",  # Have to disable SSL in es config, it is set to true by default in newer es versions
                }
            ],
            basic_auth=(ES_USERNAME, ES_PASSWORD),
        )
        if es.ping():
            print("Connected to ElasticSearch instance.")
            return es
        else:
            print("Unable to connect to ElasticSearch instance.")
            return None


if __name__ == "__main__":
    pipeline = Pipeline(CSV_FILE_PATH, INDEX_NAME)
    pipeline.run()
