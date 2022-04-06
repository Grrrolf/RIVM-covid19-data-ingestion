import os
import gzip
import datetime
import requests

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.data_format import DataFormat
from azure.kusto.data.exceptions import KustoClientError, KustoServiceError

from azure.kusto.ingest import (
    QueuedIngestClient,
    IngestionProperties,
    FileDescriptor,
    BaseIngestClient,
)


def generate_connection_string(cluster_url: str) -> KustoConnectionStringBuilder:
    return KustoConnectionStringBuilder.with_aad_application_key_authentication(
            cluster_url, os.environ.get("APP_ID"), os.environ.get("APP_KEY"), os.environ.get("APP_TENANT")
    )


def get_file(url: str, output_filename: str):

    print(f"{datetime.datetime.now()} - Download {output_filename}")
    temp_time = datetime.datetime.now()
    result = requests.get(url, allow_redirects=True)

    print(f"{datetime.datetime.now()} - Downloaded in {datetime.datetime.now() - temp_time}")
    extension = result.headers.get("content-type").split('/')[1]
    content_size = result.headers.get("Content-Length")
    source_file = f"{output_filename}.{extension}.gz"

    print(f"{datetime.datetime.now()} - write {output_filename}.{extension}.gz")
    temp_time = datetime.datetime.now()
    with gzip.open(source_file, 'wb') as file:
        file.write(result.content)
    print(f"{datetime.datetime.now()} - File written in {datetime.datetime.now() - temp_time}")

    return source_file, content_size


def ingest_from_file(ingest_client: BaseIngestClient, file_path: str, database: str,  table: str,
                     data_format: DataFormat, mapping_name: str = None,  file_size: int = 0):

    ingestion_properties = IngestionProperties(
            database=database,
            table=table,
            ingestion_mapping_reference=mapping_name,
            data_format=data_format,
            # ignoreFirstRecord=true,
            #  Setting the ingestion batching policy takes up to 5 minutes to take effect.
            #  We therefore set Flush-Immediately for the sake of the sample.  Comment out
            #  the line below after running the sample the first few times.
            flush_immediately=True,
            additional_properties={'ignoreFirstRecord': 'true'},
    )

    print(f'{datetime.datetime.now()} - Ingest data -> {table}')
    # For optimal ingestion batching and performance, specify the uncompressed data size in the file
    # descriptor instead of the default below of 0. Otherwise, the service will determine the file size, requiring
    # an additional s2s call, and may not be accurate for compressed files.
    file_descriptor = FileDescriptor(file_path, size=file_size)
    temp_time = datetime.datetime.now()
    ingest_client.ingest_from_file(file_descriptor, ingestion_properties=ingestion_properties)
    print(f"{datetime.datetime.now()} - Data ingested in {datetime.datetime.now() - temp_time}")


def truncate_table(kusto_client: KustoClient, database: str, table_name: str):
    query = f".drop extents <| .show table {table_name} extents | project ExtentId"
    print(f"{datetime.datetime.now()} - {query}")
    # time.sleep(20)
    try:
        kusto_client.execute_mgmt(database, query)
        print(f"{datetime.datetime.now()} - Truntated {table_name}")
        return
    except KustoClientError as ex:
        print(f"Client error while trying to execute query' on database '{database}'")
        print(ex)
    except KustoServiceError as ex:
        print(f"Server error while trying to execute query on database '{database}'")
        print(ex)
    except Exception as ex:
        print(f"Unknown error while trying to execute query on database '{database}'")
        print(ex)
    return


def process_data(ingestion_client: BaseIngestClient,
                 kusto_client: KustoClient,
                 database: str):

    reports = {
               "covid19_infectieradar_symptomen_per_dag": {
                   "URL": "https://data.rivm.nl/covid-19/COVID-19_Infectieradar_symptomen_per_dag.csv",
                   "dataformat": "SCSV"},
               "covid19_aantallen_gemeente_cumulatief": {
                   "URL": "https://data.rivm.nl/covid-19/COVID-19_aantallen_gemeente_cumulatief.csv",
                   "dataformat": "SCSV"},
               "covid19_aantallen_gemeente_per_dag": {
                   "URL": "https://data.rivm.nl/covid-19/COVID-19_aantallen_gemeente_per_dag.csv",
                   "dataformat": "SCSV"},
               "covid19_aantallen_settings_per_dag": {
                   "URL": "https://data.rivm.nl/covid-19/COVID-19_aantallen_settings_per_dag.csv",
                   "dataformat": "SCSV"},
               "covid19_gedrag": {
                   "URL": "https://data.rivm.nl/covid-19/COVID-19_gedrag.csv",
                   "dataformat": "SCSV"},
               "covid19_gehandicaptenzorg": {
                   "URL": "https://data.rivm.nl/covid-19/COVID-19_gehandicaptenzorg.csv",
                   "dataformat": "SCSV"},
               "covid19_ic_opnames": {
                   "URL": "https://data.rivm.nl/covid-19/COVID-19_ic_opnames.csv",
                   "dataformat": "SCSV"},
               "covid19_prevalentie": {
                   "URL": "https://data.rivm.nl/covid-19/COVID-19_prevalentie.json",
                   "dataformat": "MULTIJSON"},
               "covid19_reproductiegetal": {
                   "URL": "https://data.rivm.nl/covid-19/COVID-19_reproductiegetal.json",
                   "dataformat": "MULTIJSON"},
               "covid19_rioolwaterdata": {
                   "URL": "https://data.rivm.nl/covid-19/COVID-19_rioolwaterdata.csv",
                   "dataformat": "SCSV"},
               "covid19_thuiswonend_70plus": {
                   "URL": "https://data.rivm.nl/covid-19/COVID-19_thuiswonend_70plus.csv",
                   "dataformat": "SCSV"},
               "covid19_uitgevoerde_testen": {
                   "URL":  "https://data.rivm.nl/covid-19/COVID-19_uitgevoerde_testen.csv",
                   "dataformat": "SCSV"},
               "covid19_vaccinatiegraad_per_gemeente_per_week_leeftijd": {
                   "URL": "https://data.rivm.nl/covid-19/COVID-19_vaccinatiegraad_per_gemeente_per_week_leeftijd.csv",
                   "dataformat": "SCSV"},
               "covid19_vaccinatiegraad_per_wijk_per_week": {
                   "URL": "https://data.rivm.nl/covid-19/COVID-19_vaccinatiegraad_per_wijk_per_week.csv",
                   "dataformat": "SCSV"},
               "covid19_varianten": {
                   "URL": "https://data.rivm.nl/covid-19/COVID-19_varianten.csv",
                   "dataformat": "SCSV"},
               "covid19_verpleeghuizen": {
                   "URL": "https://data.rivm.nl/covid-19/COVID-19_verpleeghuizen.csv",
                   "dataformat": "SCSV"},
               "covid19_ziekenhuis_ic_opnames_per_leeftijdsgroep": {
                   "URL": "https://data.rivm.nl/covid-19/COVID-19_ziekenhuis_ic_opnames_per_leeftijdsgroep.csv",
                   "dataformat": "SCSV"},
               "covid19_ziekenhuisopnames": {
                   "URL": "https://data.rivm.nl/covid-19/COVID-19_ziekenhuisopnames.csv",
                   "dataformat": "SCSV"},
               "covid19_casus_landelijk": {
                   "URL": "https://data.rivm.nl/covid-19/COVID-19_casus_landelijk.csv",
                   "dataformat": "SCSV"}
    }

    # location = 'Output'

    for report, data in reports.items():
        print(25 * "=-=-")
        mapping = f"{report}_mapping"
        table_name = report
        url = data["URL"]
        dataformat = DataFormat[data["dataformat"]]

        ##############################################
        # Download the source data, gzip and save the file
        # and return the file name + file size
        source = get_file(url=url, output_filename=report)

        #############################################
        # truncate the source table
        print(f"{datetime.datetime.now()} - Truncate {table_name}")
        truncate_table(kusto_client=kusto_client, table_name=table_name, database=database)
        print(f"{datetime.datetime.now()} - Truncated -> {table_name}")
        #############################################
        # Ingest data into Kusto
        ingest_from_file(ingest_client=ingestion_client,
                         file_path=source[0],
                         file_size=int(source[1]),
                         table=table_name,
                         mapping_name=mapping,
                         data_format=dataformat,
                         database=database,)


def start():
    # destination database information
    kusto_url = "https://kvc37f426102d414c8388c.northeurope.kusto.windows.net"
    ingest_url = "https://ingest-kvc37f426102d414c8388c.northeurope.kusto.windows.net"
    database_name = "covid19"

    #####################################
    # Create the connection strings
    kusto_connection_string = generate_connection_string(kusto_url)
    ingest_connection_string = generate_connection_string( ingest_url)
    #####################################
    # create the clients once and reuse them.
    kusto_client = KustoClient(kusto_connection_string)
    ingest_client_destination = QueuedIngestClient(ingest_connection_string)

    process_data(ingestion_client=ingest_client_destination,
                 database=database_name,
                 kusto_client=kusto_client)
    print(f'{datetime.datetime.now()} - Completed')


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    start()
    print("   -------")
    print(f'   Processing time: {datetime.datetime.now() - start_time}')
