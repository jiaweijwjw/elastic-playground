"""
Script which connects to elasticsearch instance and creates a new index from a csv file. Each row in the csv file would be a document in the index.
Start your elasticsearch instance before running this script.
https://github.com/elastic/elasticsearch-py/blob/main/examples/bulk-ingest/bulk-ingest.py
"""
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv
import os
from typing import Union, Generator
import csv
import tqdm

# Create your own .env file on the same level as this script with the following variables:
# ES_USERNAME="your elastic username"
# ES_PASSWORD="your elastic password"

# Init global variables. Consider using a config file in future.
load_dotenv()  # take environment variables from .env file.
ES_USERNAME: str = os.getenv("ES_USERNAME")
ES_PASSWORD: str = os.getenv("ES_PASSWORD")

HOST: str = ""
PORT: int = 9200  # Default port for elasticsearch

CSV_FILE_PATH: str = "sample_data.csv"
INDEX_NAME: str = "sample_index"

# For WSL2 connection to elasticsearch instance running locally on Windows via bin/elasticsearch.bat
import subprocess

HOST = subprocess.check_output("hostname").decode("utf-8").strip() + ".local"
# Replace HOST with localhost if running directly on Windows.


class Pipeline:
    def __init__(self, csv_file_path: str, index_name: str) -> None:
        self.__es = None
        self.__csv_file_path = csv_file_path
        self.__index_name = index_name

    def run(self) -> None:
        """
        Runs the pipeline.
        The stages are as follows:
        Connect to elasticsearch instance.
        Index the documents to elasticsearch via streaming_bulk.
        """
        print("Connecting to ElasticSearch instance...")
        self.__es = self.__connect_elasticsearch()

        print("Indexing documents...")
        self.__index_csv_to_elasticsearch()

        # TODO: Add more stages to the pipeline.

    def __count_num_of_docs(self) -> int:
        """
        Counts the number of rows (es docs) in the csv file.

        Returns:
            int: Number of rows in the csv file / The number of documents to be indexed.
        """
        try:
            with open(self.__csv_file_path, "r") as file:
                return sum(1 for _ in file) - 1
        except FileNotFoundError:
            print(f"File {self.__csv_file_path} not found.")

    def __connect_elasticsearch(self) -> Union[Elasticsearch, None]:
        """
        Connect to elasticsearch instance.

        Returns:
            Union[Elasticsearch, None]: Returns an elasticsearch instance if manages to connect to it, else returns None.
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

    def __csv_data_action_generator(self) -> Generator:
        """
        Generator which yields the csv data as a dict with the formatting as column_name: value.

        Yields:
            Generator: Each row in the csv file as a dict (es document).
        """
        try:
            with open(self.__csv_file_path, "r") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    doc = {"_index": self.__index_name, "_source": row}
                    yield doc
        except FileNotFoundError:
            print(f"File {self.__csv_file_path} not found.")

    def __index_csv_to_elasticsearch(self) -> None:
        """
        Indexes the csv file to elasticsearch via streaming_bulk.
        Shows the indexing progress via tqdm.
        """
        num_of_docs = self.__count_num_of_docs()
        progress = tqdm.tqdm(unit="docs", total=num_of_docs)
        successes = 0
        for ok, action in helpers.streaming_bulk(
            client=self.__es,
            index=self.__index_name,
            actions=self.__csv_data_action_generator(),
            max_retries=2,
        ):
            progress.update(1)
            if ok:
                successes += 1
            else:
                print(action)
        progress.close()
        print(f"Successfully indexed {successes}/{num_of_docs} documents.")


if __name__ == "__main__":
    pipeline = Pipeline(CSV_FILE_PATH, INDEX_NAME)
    pipeline.run()
