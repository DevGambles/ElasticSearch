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

TYPESENSE_HOST = os.environ.get("TYPESENSE_HOST")
TYPESENSE_KEY = os.environ.get("TYPESENSE_KEY")

limit_count = None
new_index = "primary_categories"
old_index = "secondary_categories"


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

        # sql_query = """
        #     SELECT
        #         CONCAT(h.HERSTELLERNAME, ' ', k.kategorieName) title, CONCAT('/', k.kategorieURL, '/', h.URLSTRUKTUR,
        #         '/') url, k.kategorieBild img
        #     FROM
        #         filter_hersteller fh
        #     LEFT JOIN
        #         hersteller h ON(h.id = fh.herstellerID)
        #     LEFT JOIN
        #         kategorien k ON(fh.kategorieID = k.id)
        #     ORDER BY
        #         fh.anzahl DESC,
        #         h.HERSTELLERNAME ASC,
        #         k.kategorieName ASC
        # """

        sql_query = """
            SELECT
                CONCAT(h.name, ' ', k.week_id) title, CONCAT('/', k.id, '/', h.password,
                '/') url, fh.name img
            FROM
                agents fh
            LEFT JOIN
                zones h ON(h.id = fh.zone_id)
            LEFT JOIN
                sales k ON(fh.id = k.agent_id)
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
    _es = Elasticsearch(['search.testbericht.de'], scheme="https", port=443, timeout=500, bulk_size=3000)
    if _es.ping():
        print('Connect to Elasticsearch')
    else:
        print('it could not connect!')
    return _es


def create_index(es_object, index_name):
    created = False

    try:
        if not es_object.indices.exists(index_name):
            es_object.indices.create(index=index_name, ignore=400)
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
    print("=========== Start Categories ===========")

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

    categories = []
    index = 0
    for record in records:
        index = index + 1
        print(index)
        print(record[0])
        print(record[1])
        print(record[2])
        print(record[0].decode())
        print(record[1].decode())
        print(record[2].decode())
        row = {
            "title": record[0].decode() if record[0] else "",
            "url": record[1].decode() if record[1] else "",
            "img": record[2].decode() if record[2] else "",
        }

        categories.append(row)
    print(categories)
    json_time = time.time()
    delta_json = json_time - query_time
    print("--- Json: {:10.1f} seconds ---".format(delta_json))

    helpers.bulk(es_object, categories, index=ind_name)
    elastic_time = time.time()
    delta_elastic = elastic_time - json_time
    print("--- Elastic: {:10.1f} seconds ---".format(delta_elastic))
    print("--- Total: {:10.1f} seconds ---".format(delta_query + delta_json + delta_elastic))
    print("Imported Records:", len(categories))

    add_alias(option='categories', add_index=ind_name)
    if get_index_name(es_object, remove_name):
        es_object.indices.delete(index=remove_name, ignore=[400, 404])
    alias_time = time.time()
    delta_alias = alias_time - elastic_time
    print("--- Alias: {:10.1f} seconds ---".format(delta_alias))

    print("============ End Categories ===========")


if __name__ == '__main__':
    main()
