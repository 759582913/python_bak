# -*- coding=UTF-8 -*-
import psycopg2
import sys
import time

PG_SERVER_HOST = "192.168.1.99"
PG_SERVER_PORT = 5432
PG_SERVER_DB = "db_zc"
PG_SERVER_USER = "zczx_write"
PG_SERVER_PASS = "zczx112211"

def connect_pg(db):
    server_host = PG_SERVER_HOST
    server_port = PG_SERVER_PORT
    server_db = db
    server_user = PG_SERVER_USER
    server_pass = PG_SERVER_PASS
    try:
        db_connect = psycopg2.connect(host=server_host, port=server_port, user=server_user, password=server_pass, database=server_db )
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

if __name__ == "__main__":
    server = connect_pg("report_inner")
    pcid = "5"
    files = open(sys.path[0] + "/pcid5.txt", "r")

    for row in files.readlines():
        cid = row.strip('\n')
        print str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())) + ': start ' + cid
        report_table = 'del_report_pcid{}.report_pcid{}cid{}'.format(pcid, pcid, cid)

        get_brand_trend_sql = "insert into brand_analysis.brand_sales_pcid{} select pcid,cast(cid as numeric) as cid,\
cidname,brand,datamonth,sum(biz30day) as biz30day,sum(total_sold_price) as total_sold_price from {} where \
datamonth <> '201412' group by pcid,cid,cidname,brand,datamonth order by pcid,cid,brand,datamonth".format(pcid, report_table)
        db_execute(server, get_brand_trend_sql)
        get_cat_trend_sql = "insert into brand_analysis.category_sales_pcid{} select pcid,cast(cid as numeric) as cid,\
cidname,datamonth,sum(biz30day) as biz30day,sum(total_sold_price) as total_sold_price from {} where datamonth <> '201412'\
group by pcid,cid,cidname,datamonth order by pcid,cid,datamonth".format(pcid, report_table)
        db_execute(server, get_cat_trend_sql)
        print str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())) + ': done ' + cid

    server.close()
    files.close()
