import sys, os
import io

save_stderr = sys.stderr
sys.stderr = io.StringIO()
import psycopg2
sys.stderr = save_stderr

from app.utils.confenv import setVariables
setVariables('environment/dev.env.yml')


#TEMPLATE_DB_URL = "postgresql://{DB_USER}:{DB_PASSWORD}@/{DB_NAME}?host=/cloudsql/{GCP_PROJECT}:{GCP_ZONE}:{GCLOUD_SQL_INSTANCE}"
TEMPLATE_DB_URL = "postgresql://{DB_USER}:{DB_PASSWORD}@/{DB_NAME}?host={CLOUD_SQL_DIR}/{GCP_PROJECT}:{GCP_ZONE}:{GCLOUD_SQL_INSTANCE}"

#####################################################################################################

def main(args):
    dbconn = dbConnection()
    try:
        cur = dbconn.cursor()
        # query
        cur.execute("SELECT * from JANKY")
        one = cur.fetchone()
        print(one)

        insertion =  """INSERT INTO installation(install_id,sub_id,api_key,created_date) VALUES (%(install_id)s, %(sub_id)s, %(api_key)s, %(created_date)s)"""
        #cur.executemany(insertion, data)

        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        dbconn.commit()
    except Exception as error:
        print(error)
    finally:
        if dbconn is not None:
            dbconn.close()

#####################################################################################################

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
    dbconn = psycopg2.connect(database_uri)
    return dbconn

    #try:
    #    dbconn = psycopg2.connect(database_uri)
    #    return dbconn
    #except:
    #    print("ERROR: Unable to get a Postgres DB Connection to %s / %s" % (db_instance, db_name))
    #    return None
    
#####################################################################################################
#####################################################################################################

if __name__ == '__main__':
    main(sys.argv[1:])

