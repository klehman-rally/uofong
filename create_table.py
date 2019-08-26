import sys
import io
import yaml
# hide wheel warning about importing psycopg2-binary and PostgresSQL 2.8
save_stderr = sys.stderr
sys.stderr = io.StringIO()
import psycopg2
sys.stderr = save_stderr


installation_table = \
"""
CREATE TABLE installation (
             install_id   INTEGER PRIMARY KEY,
             sub_id       INTEGER,
             api_key      BYTEA,
             created_date TIMESTAMPTZ,
             enabled_date TIMESTAMPTZ,
             last_update  TIMESTAMPTZ,
             last_used    TIMESTAMPTZ,
             hit_count    INTEGER,
             enabled      BOOLEAN
);
"""

def main(args):
    config_file_name = args[0]  # config
    target           = args[1]  # primordial
    #config = read_config(config_file_name)[target]
    config_blob = read_config(config_file_name)
    config = config_blob[target]

    dbconn = None
    try:
        database_uri = config['TEMPLATE_DB_URL'].format(db_user=config['DB_USER'], db_password=config['DB_PASSWORD'],
                                                        db_name=config['DB_NAME'], gcp_project=config['GCP_PROJECT'],
                                                        gcp_zone=config['GCP_ZONE'],
                                                        gcloud_sql_instance=config['GCLOUD_SQL_INSTANCE'])
        print("Database URI: %s" % database_uri)
        # connect to the PostgreSQL server
        dbconn = psycopg2.connect(database_uri)
        cur = dbconn.cursor()
        # create the table
        cur.execute(installation_table)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        dbconn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if dbconn is not None:
            dbconn.close()


def read_config(config_file):
    with open(config_file, 'r') as file:
        config = yaml.load(file)
    return config

#####################################################################################
#####################################################################################

if __name__ == '__main__':
    main(sys.argv[1:])