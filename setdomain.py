import sys
import logging
import rds_config
import pymysql
from urlparse import urlparse

#rds settings
rds_host  = rds_config.db_endpoint
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name


logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=5, cursorclass=pymysql.cursors.DictCursor)
except:
    logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
    sys.exit()

def handler(event, context):

    try:
        parsedUrl = urlparse((event['url'])).hostname
        if parsedUrl is not None:
            with conn.cursor() as cur:
                cur.execute("insert into Domains (domainid,count) values (%s,1) on duplicate key update count = count + 1", (parsedUrl))
                conn.commit()
                cur.close()

                return "Updated counter for domain %s" % parsedUrl
        else:
            return "No domain name found"
    except:
        logger.error("ERROR: Unexpected error: Could not fetch query results")
        sys.exit()
