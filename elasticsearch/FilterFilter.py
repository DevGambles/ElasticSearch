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
new_index = "primary_filters"
old_index = "secondary_filters"


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
                fn.id, fn.title, fn.anzeige, fn.url, fn.filterURL, fn.anzeigeKategorie fnAnzeigeKategorie, fn.noIndex noIndexFN, fn.anzahl,
                fk.name, fk.anzeigeKategorie fkAnzeigeKategorie, fk.noIndex noIndexFK,
                k.kategorieURL, k.kategorieName
            FROM
                filter_namen fn
            LEFT JOIN
                filter_kategorien fk ON(fk.id = fn.fkid)
            LEFT JOIN
                kategorien k ON(k.id = fk.kategorieID)
            WHERE
                showKategorie = 1
            ORDER BY
                fn.anzahl DESC
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

                "title": {
                    "type": "text",
                    "fielddata": {
                        "loading": "eager"
                    }
                },
                "anzeige": {
                    "type": "text",
                },
                "url": {
                    "type": "text",
                },
                "filterURL": {
                    "type": "text",
                },
                "fnAnzeigeKategorie": {
                    "type": "short",
                },
                "noIndexFN": {
                    "type": "short",
                },
                "anzahl": {
                    "type": "short",
                },
                "name": {
                    "type": "text",
                },
                "fkAnzeigeKategorie": {
                    "type": "short",
                },
                "noIndexFK": {
                    "type": "short",
                },
                "kategorieURL": {
                    "type": "text",
                },
                "kategorieName": {
                    "type": "text",
                }
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
    print("=========== Start Filters ===========")

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

    filters = []
    for record in records:
        row = {
            "id": int(record[0]),
            "Title": record[1] if record[1] else "",
            "anzeige": record[2] if record[2] else "",
            "url": record[3] if record[3] else "",
            "filterURL": record[4] if record[4] else "",
            "fnAnzeigeKategorie": int(record[5]) if record[5] else 0,
            "noIndexFN": int(record[6]) if record[6] else 0,
            "anzahl": int(record[7]) if record[7] else 0,
            "name": record[8] if record[8] else "",
            "fkAnzeigeKategorie": int(record[9]) if record[9] else 0,
            "noIndexFK": int(record[10]) if record[10] else 0,
            "kategorieURL": record[11] if record[11] else "",
            "kategorieName": record[12] if record[12] else "",
        }

        filters.append(row)

    json_time = time.time()
    delta_json = json_time - query_time
    print("--- Json: {:10.1f} seconds ---".format(delta_json))

    helpers.bulk(es_object, filters, index=ind_name)
    elastic_time = time.time()
    delta_elastic = elastic_time - json_time
    print("--- Elastic: {:10.1f} seconds ---".format(delta_elastic))
    print("--- Total: {:10.1f} seconds ---".format(delta_query + delta_json + delta_elastic))
    print("Imported Records:", len(filters))

    add_alias(option='filters', add_index=ind_name)
    if get_index_name(es_object, remove_name):
        es_object.indices.delete(index=remove_name, ignore=[400, 404])
    alias_time = time.time()
    delta_alias = alias_time - elastic_time
    print("--- Alias: {:10.1f} seconds ---".format(delta_alias))

    print("============ End Filters ===========")


if __name__ == '__main__':
    main()
