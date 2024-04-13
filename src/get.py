import os
import logging
import mysql.connector
import pandas as pd
from config import LIMIT
from env import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB


import time

logger = logging.getLogger(__name__)


def get_page_df():
    """
    https://www.mediawiki.org/wiki/Manual:Page_table
    +--------------------+---------------------+------+-----+---------+----------------+
    | Field              | Type                | Null | Key | Default | Extra          |
    +--------------------+---------------------+------+-----+---------+----------------+
    | page_id            | int(10) unsigned    | NO   | PRI | NULL    | auto_increment |
    | page_namespace     | int(11)             | NO   | MUL | NULL    |                |
    | page_title         | varbinary(255)      | NO   |     | NULL    |                |
    | page_is_redirect   | tinyint(3) unsigned | NO   | MUL | 0       |                |
    | page_is_new        | tinyint(3) unsigned | NO   |     | 0       |                |
    | page_random        | double unsigned     | NO   | MUL | NULL    |                |
    | page_touched       | binary(14)          | NO   |     | NULL    |                |
    | page_links_updated | varbinary(14)       | YES  |     | NULL    |                |
    | page_latest        | int(10) unsigned    | NO   |     | NULL    |                |
    | page_len           | int(10) unsigned    | NO   | MUL | NULL    |                |
    | page_content_model | varbinary(32)       | YES  |     | NULL    |                |
    | page_lang          | varbinary(35)       | YES  |     | NULL    |                |
    +--------------------+---------------------+------+-----+---------+----------------+
    """
    if os.getenv("MYSQL_HOST") is None:
        df = pd.read_csv('data/page-1000.csv')
        return df

    try:
        start_time = time.perf_counter()
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
        df = pd.read_sql_query(
            f"SELECT * FROM page ORDER BY page_title LIMIT {LIMIT}", conn)
        logger.debug(f"Query `page` table ({
                     (time.perf_counter() - start_time):.2f}s)")
        processing_start_time = time.perf_counter()
        df['page_title'] = df['page_title'].str.decode('utf-8')
        logger.debug(f"Preprocess page_df ({
                     (time.perf_counter() - processing_start_time):.2f}s)")
        # page_df[['page_id', 'page_namespace', 'page_title', 'page_is_redirect', 'page_is_new', 'page_random', 'page_touched',
        #          'page_links_updated', 'page_latest', 'page_len', 'page_content_model', 'page_lang']].to_csv('data/top_1000_pages.csv', index=False)
        return df
    except Exception as e:
        logger.error(str(e))
    finally:
        conn.close()


def get_pagelinks_df():
    """
    https://www.mediawiki.org/wiki/Manual:Pagelinks_table

    +-------------------+---------------------+------+-----+---------+-------+
    | Field             | Type                | Null | Key | Default | Extra |
    +-------------------+---------------------+------+-----+---------+-------+
    | pl_from           | int(10) unsigned    | NO   | PRI | 0       |       |
    | pl_namespace      | int(11)             | NO   | PRI | 0       |       |
    | pl_title          | varbinary(255)      | NO   | PRI |         |       |
    | pl_from_namespace | int(11)             | NO   | MUL | 0       |       |
    | pl_target_id      | bigint(20) unsigned | YES  | MUL | NULL    |       |
    +-------------------+---------------------+------+-----+---------+-------+
    """
    if os.getenv("MYSQL_HOST") is None:
        df = pd.read_csv('data/pagelinks-1000.csv')
        return df

    try:
        start_time = time.perf_counter()
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
        df = pd.read_sql_query(
            f"SELECT * FROM pagelinks ORDER BY pl_title LIMIT {LIMIT}", conn)
        logger.debug(f"Query `pagelinks` table ({
                     (time.perf_counter() - start_time):.2f}s)")
        processing_start_time = time.perf_counter()
        df['pl_title'] = df['pl_title'].str.decode('utf-8')
        logger.debug(f"Preprocess pagelinks_df ({
                     (time.perf_counter() - processing_start_time):.2f}s)")
        # pagelinks_df[['pl_from', 'pl_namespace', 'pl_title', 'pl_from_namespace', 'pl_target_id']].to_csv('data/top_1000_pagelinks.csv', index=False)
        return df
    except Exception as e:
        print(str(e))
    finally:
        conn.close()
