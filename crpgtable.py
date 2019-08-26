import sys, os
import yaml
import json
import io
from contextlib import contextmanager
from collections import OrderedDict

save_stderr = sys.stderr
sys.stderr = io.StringIO()
import psycopg2
sys.stderr = save_stderr

from app.utils.confenv import setVariables
setVariables('environment/dev.env.yml')

TEMPLATE_DB_URL = "postgresql://{DB_USER}:{DB_PASSWORD}@/{DB_NAME}?host={CLOUD_SQL_DIR}/{GCP_PROJECT}:{GCP_ZONE}:{GCLOUD_SQL_INSTANCE}"

SQL = {
        "create table" : "CREATE TABLE {table} ();",
        "drop table"   : "DROP TABLE {table};",
        "add column"   : "ALTER TABLE {table} ADD COLUMN {column} {datatype} NOT NULL",
        "copy"         : "COPY {table} FROM STDIN NULL '' DELIMITER ',' CSV;",
        "table-exists" : "SELECT TABLENAME FROM PG_TABLES where schemaname = 'public' and tablename = '{table}';"
      }

SCHEMA = 'configs/schemas.json'

################################################################################################################

def main(args):
    table_name = args[0] or "webhook"
    if table_name not in ['condition', 'webhook']:
        sys.exit(0)

    schema = [table for table in read_schema(SCHEMA) if table_name in table][0][table_name]
    file_name = "data/csv/%s.csv" % table_name
    full_path = '%s/%s' % (os.getcwd(), file_name)
    print("filename: %s exists? %s" % (full_path, os.path.exists(full_path)))

    with dbConnection() as dbConn:
        cursor = dbConn.cursor()
        cursor.execute(SQL['table-exists'].format(table=table_name))
        result = cursor.fetchall()
        dropit = len(result) != 0
        with table_generator(cursor, table_name, drop=dropit):
            for column_name, data_type in schema.items():
                cursor.execute(SQL['add column'].format(table=table_name, column=column_name, datatype=data_type))

            #cursor.execute(SQL['copy'].format(table=table_name, path=full_path))
            csvf = open(full_path, 'r', encoding='utf-8')
            copy_command = SQL['copy'].format(table=table_name)
            cursor.copy_expert(copy_command, csvf)
            csvf.close()
        dbConn.commit()

################################################################################################################

def dbConnection():
    gcp_project = os.getenv('GCP_PROJECT')
    gcp_zone    = os.getenv('GCP_ZONE')
    db_instance = os.getenv('GCLOUD_SQL_INSTANCE')
    db_name     = os.getenv('DB_NAME')
    db_user     = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    cloud_sql_dir = os.getenv('CLOUD_SQL_DIR')
    database_uri = TEMPLATE_DB_URL.format(DB_USER=db_user, DB_PASSWORD=db_password, DB_NAME=db_name, 
                                          CLOUD_SQL_DIR=cloud_sql_dir, GCP_PROJECT=gcp_project, GCP_ZONE=gcp_zone,
                                          GCLOUD_SQL_INSTANCE=db_instance)
    print("Database URI: %s" % database_uri)
    # connect to the PostgreSQL server

    try:
        dbconn = psycopg2.connect(database_uri)
        return dbconn
    except:
        print("ERROR: Unable to get a Postgres DB Connection to %s / %s" % (db_instance, db_name))
        return None
    
################################################################################################################

@contextmanager
def table_generator(cursor, table_name, drop=False):
    cursor.execute(SQL["create table"].format(table=table_name))
    print("created table: %s" %table_name)
    try:
        yield
    finally:
        if drop:
            cursor.execute(SQL["drop table"].format(table=table_name))
            print("dropped table: %s" %table_name)

def read_config(config_file):
    with open(config_file, 'r') as file:
        config = yaml.load(file)
    return config

def read_schema(schema_file):
    with open(schema_file) as json_data:
        return json.load(json_data, object_pairs_hook=OrderedDict)['schema']

################################################################################################################
################################################################################################################

if __name__ == '__main__':
    main(sys.argv[1:])
