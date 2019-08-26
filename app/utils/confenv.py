import os
import yaml

def read_config(config_file):
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

def setVariables(config_file):
    config = read_config(config_file)

    #os.environ['CRYPTOKEY_ID']          = config['CRYPTOKEY_ID']
    os.environ['GCP_PROJECT']           = config['LOCAL_GCP_PROJECT']
    os.environ['GOOGLE_CLOUD_PROJECT']  = config['LOCAL_GCP_PROJECT']
    os.environ['GCP_ZONE']              = config['GCP_ZONE']

    os.environ['DB_NAME']               = config['DB_NAME']
    os.environ['DB_PASSWORD']           = config['DB_PASSWORD']
    os.environ['DB_USER']               = config['DB_USER']
    os.environ['GCLOUD_SQL_INSTANCE']   = config['GCLOUD_SQL_INSTANCE']
    os.environ['CLOUD_SQL_DIR']         = config['CLOUD_SQL_DIR']
    #os.environ['TEMPLATE_DATABASE_URL'] = config['TEMPLATE_DB_URL'] #restore

    #os.environ['INGESTED_TOPIC']            = config['INGESTED_TOPIC']
    #os.environ['INGESTED_SUBSCRIPTION']     = config['INGESTED_SUBSCRIPTION']
    #os.environ['PANNED_PULLREQUEST_TOPIC']  = config['PANNED_PULLREQUEST_TOPIC']
    #os.environ['PANNED_SUBSCRIPTION']       = config['PANNED_SUBSCRIPTION']
    #os.environ['POSTED_TOPIC']              = config['POSTED_TOPIC']
    #os.environ['POSTED_SUBSCRIPTION']       = config['POSTED_SUBSCRIPTION']

