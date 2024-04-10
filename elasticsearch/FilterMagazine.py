import os
import mysql.connector
from elasticsearch7 import Elasticsearch, helpers
from dotenv import load_dotenv
import time

from alias import add_alias

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

DB_HOST = os.environ.get("DB_HOST")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DATABASE = os.environ.get("DB_DATABASE")

limit_count = None
new_index = "primary_filtermagazines"
old_index = "secondary_filtermagazines"


def query():
    initial_db = None
    initial_cursor = None

    try:
        initial_db = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DATABASE
        )

        initial_cursor = initial_db.cursor(prepared=True)

        sql_query = """
            SELECT
                tt.id, tt.testerURL, tt.testerName,
                COUNT(t.TESTID) anz
            FROM
                tester tt
            LEFT JOIN
                tests t ON(tt.id = t.testerID)
            GROUP BY
                tt.id
            HAVING
                anz > 0
            ORDER BY
                tt.testerName ASC
        """

        if limit_count is not None:
            sql_query += " LIMIT %s"
            initial_cursor.execute(sql_query, (limit_count,))
        else:
            initial_cursor.execute(sql_query)

        return initial_cursor.fetchall()

    except mysql.connector.Error as e:
        print("Failed to query table in MySQL: {}".format(e))
        return []

    finally:
        if initial_db and initial_db.is_connected():
            initial_cursor.close()
            initial_db.close()


def connect_elasticsearch():
    _es = None
    _es = Elasticsearch(['search.testbericht.de'], scheme="https", port=443, timeout=5000.0, bulk_size=10000)
    if _es.ping():
        print('Connect to Elasticsearch')
    else:
        print('it could not connect!')
    return _es


def create_index(es_object, index_name):
    created = False

    settings = {
        "settings": {
            "number_of_shards": 4,
            "number_of_replicas": 1
        },
        "mappings": {
            "properties": {
                "dynamic": "strict",
                "id": {
                    "type": "long",
                },
                "testerURL": {
                    "type": "text",
                    "fielddata": {
                        "loading": "eager"
                    }
                },
                "testerName": {
                    "type": "text",
                },
                "anz": {
                    "type": "long",
                },
            }
        }
    }

    try:
        if not es_object.indices.exists(index_name):
            es_object.indices.create(index=index_name, ignore=400, body=settings)
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


def get_index_name(es_object, index_name):
    exist = True

    try:
        if not es_object.indices.exists(index_name):
            exist = False
    except Exception as ex:
        print(str(ex))
    finally:
        return exist


def main():
    print("=========== Start filtermagazines ===========")

    start_time = time.time()

    es_object = connect_elasticsearch()

    if get_index_name(es_object=es_object, index_name=new_index):
        ind_name = old_index
        remove_name = new_index
    else:
        ind_name = new_index
        remove_name = old_index
    create_index(es_object=es_object, index_name=ind_name)

    records = query()
    query_time = time.time()
    delta_query = query_time - start_time
    print("--- Query: {:10.1f} seconds ---".format(delta_query))

    filtermagazines = []
    for record in records:
        row = {
            "id": int(record[0]),
            "testerURL": record[1] if record[1] else "",
            "testerName": record[2] if record[2] else "",
            "anz": int(record[3]) if record[3] else 0,
        }

        filtermagazines.append(row)

    json_time = time.time()
    delta_json = json_time - query_time
    print("--- Json: {:10.1f} seconds ---".format(delta_json))

    helpers.bulk(es_object, filtermagazines, index=ind_name)
    elastic_time = time.time()
    delta_elastic = elastic_time - json_time
    print("--- Elastic: {:10.1f} seconds ---".format(delta_elastic))
    print("--- Total: {:10.1f} seconds ---".format(delta_query + delta_json + delta_elastic))
    print("Imported Records:", len(filtermagazines))

    add_alias(option='filtermagazines', add_index=ind_name)
    if get_index_name(es_object, remove_name):
        es_object.indices.delete(index=remove_name, ignore=[400, 404])
    alias_time = time.time()
    delta_alias = alias_time - elastic_time
    print("--- Alias: {:10.1f} seconds ---".format(delta_alias))

    print("============ End filtermagazines ===========")


if __name__ == '__main__':
    main()
