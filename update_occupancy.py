# -*- coding=UTF-8 -*-
import psycopg2
import time
import sys
import yaml
# update_occupancy"db--report_inner"

PG_SERVER_HOST = "192.168.1.99"
PG_SERVER_PORT = 5432
PG_SERVER_USER = "zczx_write"
PG_SERVER_PASS = "zczx112211"

def connect_pg(db):
    server_host = PG_SERVER_HOST
    server_port = PG_SERVER_PORT
    server_db = db
    server_user = PG_SERVER_USER
    server_pass = PG_SERVER_PASS
    try:
        db_connect = psycopg2.connect(host=server_host, port=server_port, user=server_user, password=server_pass, database=server_db)
    except Exception, e:
        print e
    return db_connect

def db_execute(server, sql):
    dbconn = server.cursor()
    try:
        dbconn.execute(sql)
        server.commit()
    except Exception, e:
        print(e)
        server.rollback()
    dbconn.close()

def up_brand_sql(pcid, cid, ls):
    conf_files = open(sys.path[0] + "/sub_brand_sql.yaml")
    conf = yaml.load(conf_files)
    conf_files.close()
    sql = conf['up_brand_data']
    brand_sql = ''
    for item in ls:
        brand_sql += sql % (pcid, item, item, pcid, pcid, cid)
    return brand_sql

def up_cat_sql(pcid, cid, ls):
    conf_files = open(sys.path[0] + "/sub_brand_sql.yaml")
    conf = yaml.load(conf_files)
    conf_files.close()
    sql = conf['up_cat_data']
    cat_sql = ''
    for item in ls:
        cat_sql += sql % (pcid, item, item, pcid, pcid, cid)
    return cat_sql

def calculate_data(pcid, cid):
    conf_files = open(sys.path[0] + "/sub_brand_sql.yaml")
    conf = yaml.load(conf_files)
    conf_files.close()
    sql = conf['calculate_data']
    return sql % (pcid, cid, pcid, cid)

if __name__ == "__main__":
    server = connect_pg("report_inner")
    pcid = '9'
    files = open(sys.path[0] + '/pcid9.txt', 'r')
    ls = ['biz30day', 'total_sold_price']
    # date = '201701'
    for rows in files.readlines():
        cid = rows.strip('\n')
        print str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())) + ' start ' + cid
        db_execute(server, up_brand_sql(pcid, cid, ls))
        db_execute(server, up_cat_sql(pcid, cid, ls))
        db_execute(server, calculate_data(pcid, cid))
        print str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())) + ' done ' + cid

    server.close()
    files.close()
