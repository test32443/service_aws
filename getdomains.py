import sys
import logging
import rds_config
import pymysql
#rds settings
rds_host  = rds_config.db_endpoint
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name


logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):

    try:
        conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=5, cursorclass=pymysql.cursors.DictCursor)
    except:
        logger.error("ERROR: Unexpected error: Could not connect to MySql instance")
        sys.exit()

    try:
        with conn.cursor() as cur:
            cur.execute("select DomainID, count from Domains order by count desc limit 3")
            rows = cur.fetchall()
            cur.close()
            conn.close()
            return rows
    except:
        logger.error("ERROR: Unexpected error: Could not fetch query results")
        sys.exit()
