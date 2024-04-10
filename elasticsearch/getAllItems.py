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
new_index = "primary_items"
old_index = "secondary_items"


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
    p.id, p.PNAME, p.PURL, p.DATENBLATT, p.DATENBLATTDETAILS, p.IMG, p.MAN, p.EAN, p.CONTENT, p.TESTCONTENT, p.SCORE, p.TESTS, p.TESTPRO, p.TESTCONTRA, p.TESTSIEGER,
    p.PREISSIEGER, p.MEINUNGENSCORE, p.MEINUNGEN, p.meinungenPunkte, p.AMAZONMEINUNGEN, p.serienZusatz, TRIM(LEADING '{' from (TRIM(TRAILING '}' FROM 'p.energieEffizienzKlasse' ))) AS 'energieEffizienzKlasse', p.noindex noIndex, p.noIndex2,
    pa.anzAngebote, pa.preis, pa.sortPos,
    k.noIndex noIndexKategorie, k.kategorieURL,
    pb.pfad bildPfad, pb.breite, pb.hoehe,
    GROUP_CONCAT(DISTINCT CONCAT(mas.star, '-', mas.anzahl) SEPARATOR '|') starsAmazon,
    GROUP_CONCAT(DISTINCT CONCAT(mos.star, '-', mos.anzahl) SEPARATOR '|') starsOtto,
    GROUP_CONCAT(DISTINCT CONCAT(m.id, '-', m.meinungstern) SEPARATOR '|') starsTBDE
FROM
    pname2pid_mapping p
LEFT JOIN
    pname2pid_angebote pa ON(pa.produktID = p.id)
LEFT JOIN
    kategorien k ON(k.id = p.kategorieID)
LEFT JOIN
    hersteller h ON(h.id = p.herstellerID)
LEFT JOIN
    tbmeinungen m ON (m.produktID = p.id AND m.meinungstatus = 0)
LEFT JOIN
    meinungen_amazon_stars mas ON (mas.produktID = p.id)
LEFT JOIN
    meinungen_otto_stars mos ON (mos.produktID = p.id)
LEFT JOIN
    produktbilder pb ON(pb.produktID = p.ID AND pb.pos = 1 AND pb.groesse = 'L')
WHERE
    p.gesperrt = 0 AND
    h.gesperrt = 0
GROUP BY
    p.id
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
    _es = Elasticsearch(['search.testbericht.de'], scheme="https", port=443, timeout=500000.0, bulk_size=10000)
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
                "PNAME": {
                    "type": "text",
                    "fielddata": {
                        "loading": "eager"
                    }
                },
                "PURL": {
                    "type": "text",
                },
                "DATENBLATT": {
                    "type": "text",
                },
                "DATENBLATTDETAILS": {
                    "type": "text",
                },
                "IMG": {
                    "type": "text",
                },
                "MAN": {
                    "type": "text",
                },
                "EAN": {
                    "type": "text",
                },
                "CONTENT": {
                    "type": "text",
                },
                "TESTCONTENT": {
                    "type": "text",
                },
                "SCORE": {
                    "type": "long",
                },
                "TESTS": {
                    "type": "long",
                },
                "TESTPRO": {
                    "type": "text",
                },
                "TESTCONTRA": {
                    "type": "text",
                },
                "TESTSIEGER": {
                    "type": "long",
                },
                "PREISSIEGER": {
                    "type": "long",
                },
                "MEINUNGENSCORE": {
                    "type": "text",
                },
                "MEINUNGEN": {
                    "type": "long",
                },
                "meinungenPunkte": {
                    "type": "double",
                },
                "AMAZONMEINUNGEN": {
                    "type": "long",
                },
                "serienZusatz": {
                    "type": "text",
                },
                "energieEffizienzKlasse": {
                    "type": "text",
                },
                "noIndex": {
                    "type": "short",
                },
                "noIndex2": {
                    "type": "short",
                },
                "anzAngebote": {
                    "type": "long",
                },
                "preis": {
                    "type": "long",
                },
                "sortPos": {
                    "type": "long",
                },
                "noIndexKategorie": {
                    "type": "short",
                },
                "kategorieURL": {
                    "type": "text",
                },
                "bildPfad": {
                    "type": "text",
                },
                "breite": {
                    "type": "long",
                },
                "hoehe": {
                    "type": "long",
                },
                "starsAmazon": {
                    "type": "text",
                },
                "starsOtto": {
                    "type": "text",
                },
                "starsTBDE": {
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
    print("=========== Start items ===========")

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

    items = []
    for record in records:
        row = {
            "id": int(record[0]),
            "PNAME": record[1] if record[1] else "",
            "PURL": record[2] if record[2] else "",
            "DATENBLATT": record[3] if record[3] else "",
            "DATENBLATTDETAILS": record[4] if record[4] else "",
            "IMG": record[5] if record[5] else "",
            "MAN": record[6] if record[6] else "",
            "EAN": record[7] if record[7] else "",
            "CONTENT": record[8] if record[8] else "",
            "TESTCONTENT": record[9] if record[9] else "",
            "SCORE": int(record[10]) if record[10] else 0,
            "TESTS": int(record[11]) if record[11] else 0,
            "TESTPRO": record[12] if record[12] else "",
            "TESTCONTRA": record[13] if record[13] else "",
            "TESTSIEGER": int(record[14]) if record[14] else 0,
            "PREISSIEGER": int(record[15]) if record[15] else 0,
            "MEINUNGENSCORE": record[16] if record[16] else "",
            "MEINUNGEN": int(record[17]) if record[17] else 0,
            "meinungenPunkte": float(record[18]) if record[18] else 0,
            "AMAZONMEINUNGEN": int(record[19]) if record[19] else 0,
            "serienZusatz": record[20] if record[20] else "",
            "energieEffizienzKlasse": record[21] if record[21] else "",
            "noIndex": int(record[22]) if record[22] else 0,
            "noIndex2": int(record[23]) if record[23] else 0,
            "anzAngebote": int(record[24]) if record[24] else 0,
            "preis": int(record[25]) if record[25] else 0,
            "sortPos": int(record[26]) if record[26] else 0,
            "noIndexKategorie": int(record[27]) if record[27] else 0,
            "kategorieURL": record[28] if record[28] else "",
            "bildPfad": record[29] if record[29] else "",
            "breite": int(record[30]) if record[30] else 0,
            "hoehe": int(record[31]) if record[31] else 0,
            "starsAmazon": record[32] if record[32] else "",
            "starsOtto": record[33] if record[33] else "",
            "starsTBDE": record[34] if record[34] else "",
        }

        items.append(row)

    json_time = time.time()
    delta_json = json_time - query_time
    print("--- Json: {:10.1f} seconds ---".format(delta_json))

    helpers.bulk(es_object, items, index=ind_name)
    elastic_time = time.time()
    delta_elastic = elastic_time - json_time
    print("--- Elastic: {:10.1f} seconds ---".format(delta_elastic))
    print("--- Total: {:10.1f} seconds ---".format(delta_query + delta_json + delta_elastic))
    print("Imported Records:", len(items))

    add_alias(option='items', add_index=ind_name)
    if get_index_name(es_object, remove_name):
        es_object.indices.delete(index=remove_name, ignore=[400, 404])
    alias_time = time.time()
    delta_alias = alias_time - elastic_time
    print("--- Alias: {:10.1f} seconds ---".format(delta_alias))

    print("============ End items ===========")


if __name__ == '__main__':
    main()
