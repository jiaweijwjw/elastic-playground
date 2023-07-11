"""
Script which connects to elasticsearch instance and creates a new index from a csv file. Each row in the csv file would be a document in the index.
"""
from elasticsearch import Elasticsearch

host: str = ""
port: int = 9200

elastic_username: str = ""
elastic_password: str = ""

# For WSL2 connection to elasticsearch instance running locally on Windows via bin/elasticsearch.bat
import subprocess

host = subprocess.check_output("hostname").decode("utf-8").strip() + ".local"


def connect_elasticsearch():
    _es = None
    _es = Elasticsearch(
        [
            {
                "host": host,
                "port": port,
                "scheme": "http",  # Have to disable SSL in es config, it is set to true by default in newer es versions
            }
        ],
        basic_auth=("elastic", "d1OG7lxxPoNUw4EAfX_3"),
    )
    if _es.ping():
        print("Yay Connect")
    else:
        print("Awww it could not connect!")
    return _es


my_es = connect_elasticsearch()


# class Convertor:
#     def __init__(self, csv_file_path: str, index_name: str):
#         self.csv_file_path = csv_file_path
#         self.index_name = index_name

#     def convert(self):
#         import csv

#         with open(self.csv_file_path, "r") as csv_file:
#             csv_reader = csv.DictReader(csv_file)
#             for row in csv_reader:
#                 print(row)
#                 self.insert_into_es(row)

#     def insert_into_es(self, row: dict):
#         my_es.index(index=self.index_name, body=row)
