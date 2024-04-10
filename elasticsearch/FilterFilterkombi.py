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
new_index = "primary_filterkombis"
old_index = "secondary_filterkombis"


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
                fko.id, fko.title, fko.anzahl, fko.noIndex,
                GROUP_CONCAT(fn.url ORDER BY
                    IF(fk.sort <> 0, fk.sort, 999) ASC,
                    fk.name ASC,
                    IF(fn.sort <> 0, fn.sort, 999) ASC,
                    fn.anzeige ASC
                SEPARATOR '-') url,
                k.kategorieURL
            FROM
                filter_namen fn
            LEFT JOIN
                filter_kombinieren_eintraege fke ON(fn.id = fke.fnid)
            LEFT JOIN
                filter_kombinieren fko ON(fko.id = fke.fkoid)
            LEFT JOIN
                kategorien k ON(k.id = fko.kategorieID)
            LEFT JOIN
                filter_kategorien fk ON(fk.id = fn.fkid)
            WHERE
                fko.aktiv = 1
            GROUP BY
                fko.id
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
                "anzahl": {
                    "type": "long",
                },
                "noIndex": {
                    "type": "short",
                },
                "url": {
                    "type": "text",
                },
                "kategorieURL": {
                    "type": "text",
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
    print("=========== Start filterkombis ===========")

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

    filterkombis = []
    for record in records:
        row = {
            "id": int(record[0]),
            "title": record[1] if record[1] else "",
            "anzahl": int(record[2]) if record[2] else 0,
            "noIndex": int(record[3]) if record[3] else 0,
            "url": record[4] if record[4] else "",
            "kategorieURL": record[5] if record[5] else ""
        }

        filterkombis.append(row)

    json_time = time.time()
    delta_json = json_time - query_time
    print("--- Json: {:10.1f} seconds ---".format(delta_json))

    helpers.bulk(es_object, filterkombis, index=ind_name)
    elastic_time = time.time()
    delta_elastic = elastic_time - json_time
    print("--- Elastic: {:10.1f} seconds ---".format(delta_elastic))
    print("--- Total: {:10.1f} seconds ---".format(delta_query + delta_json + delta_elastic))
    print("Imported Records:", len(filterkombis))

    add_alias(option='filterkombis', add_index=ind_name)
    if get_index_name(es_object, remove_name):
        es_object.indices.delete(index=remove_name, ignore=[400, 404])
    alias_time = time.time()
    delta_alias = alias_time - elastic_time
    print("--- Alias: {:10.1f} seconds ---".format(delta_alias))

    print("============ End filterkombis ===========")


if __name__ == '__main__':
    main()
