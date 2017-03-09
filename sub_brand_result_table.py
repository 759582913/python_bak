# -*-coding:utf-8-*-
# sub_brand_report_table&top_brand_occupancy"db--report_inner"
import psycopg2
import sys
import time
import yaml

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
        db_connect = psycopg2.connect(host=server_host, port=server_port, user=server_user, password=server_pass,
                                      database=server_db)
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

def db_get(server, sql):
    dbconn = server.cursor()
    result = []
    try:
        dbconn.execute(sql)
        result = dbconn.fetchall()
        server.commit()
    except Exception, e:
        print(e)
        server.rollback()
    dbconn.close()
    return result

def get_submarket_dict(server, pcid, cid):
    sql = "select distinct searchword,sm_id from submarket.dw_submarket_smid_pcid{} where cid = '{}'".format(pcid, cid)
    dbconn = server.cursor()
    submarket_dict = dict()
    try:
        dbconn.execute(sql)
        result = dbconn.fetchall()
        for sub in result:
            submarket = sub[0]
            sm_id = sub[1]
            submarket_dict[sm_id] = submarket
        server.commit()
    except Exception, e:
        print (e)
    dbconn.close()
    return submarket_dict

def brand_submarket_data(pcid, cid, sub_dict):
    conf_files_path = sys.path[0] + "/sub_brand_sql.yaml"
    conf_files = open(conf_files_path)
    conf = yaml.load(conf_files)
    conf_files.close()

    sql = conf['brand_submarket_data']
    brand_table = "brand_analysis.brand_submarkert_pcid{}".format(pcid)
    report_table = "del_report_pcid%s.report_pcid%scid%s" % (pcid, pcid, cid)
    sub_brand_sql = ''
    for key1, value1 in sub_dict.items():
        sm_id = key1
        submarket = value1
        sub_brand_sql += sql % (brand_table, submarket, sm_id, report_table, submarket)
    return sub_brand_sql

def cat_submarket_data(pcid, cid, sub_dict):
    conf_files_path = sys.path[0] + "/sub_brand_sql.yaml"
    conf_files = open(conf_files_path)
    conf = yaml.load(conf_files)
    conf_files.close()

    cat_sql = conf['cat_submarket_data']
    brand_table = "brand_analysis.brand_submarkert_pcid{}".format(pcid)
    report_table = "del_report_pcid%s.report_pcid%scid%s" % (pcid, pcid, cid)
    sub_cat_sql = ''
    for key1, value1 in sub_dict.items():
        sm_id = key1
        submarket = value1
        sub_cat_sql += cat_sql % (brand_table, cid, submarket, sm_id, report_table, submarket)
    return sub_cat_sql

def top_brand_submarket(pcid, cid, sub_dict):
    conf_files_path = sys.path[0] + "/sub_brand_sql.yaml"
    conf_files = open(conf_files_path)
    conf = yaml.load(conf_files)
    conf_files.close()
    sub_table = 'brand_analysis.brand_submarkert_pcid{}'.format(pcid)
    top_brand_sql = conf['top_brand_occupancy']
    top_sql = []
    others = u"'其他','empty','other/其他','其他/other','其它'"
    for key1, values in sub_dict.items():
        sm_id = key1
        for datamonth in ['201611', '201612', '201701']:
            top_sql.append(top_brand_sql % (sub_table, sub_table, cid, sm_id, cid, sm_id, datamonth, sub_table, cid, sm_id, datamonth, others, cid,
sub_table, sub_table, cid, sm_id, cid, sm_id, datamonth, sub_table, cid, sm_id, datamonth, others, cid))
    return top_sql

def db_write(server, data_ls, pcid):
    dbconn = server.cursor()
    sql = "insert into submarket.top_brand_occupancy_pcid{} values (%s,%s,%s,%s,%s,%s,%s,%s)".format(pcid)
    try:
        dbconn.executemany(sql, data_ls)
        server.commit()
    except Exception, e:
        print(e)
        server.rollback()
    dbconn.close()

if __name__ == "__main__":
    server1 = connect_pg("basic")
    server2 = connect_pg("report_inner")
    pcid_list = ['2', '4', '5', '6', '8', '9']
    for pcid in pcid_list:
        files = open(sys.path[0] + "/pcid{}.txt".format(pcid), "r")
        for row in files.readlines():
            cid = row.strip('\n')
            print str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())) + ': start ' + cid
            market_segment_dict = get_submarket_dict(server1, pcid, cid)
            # db_execute(server2, brand_submarket_data(pcid, cid, market_segment_dict))
            # db_execute(server2, cat_submarket_data(pcid, cid, market_segment_dict))
            # top_brand
            data_list = []
            top_biz30day = top_brand_submarket(pcid, cid, market_segment_dict)
            for biz_sql in top_biz30day:
                # insert into table
                # db_execute(server2, biz_sql)
                for items in db_get(server2, biz_sql):
                    cidname = items[1]
                    submarket = items[2]
                    sm_id = items[3]
                    datamonth = items[4]
                    top_brand_biz30day_occupancy = items[5]
                    top_brand_total_sold_price_occupancy = items[6]
                    data_list.append((pcid, cid, cidname, submarket, sm_id, datamonth, top_brand_biz30day_occupancy, top_brand_total_sold_price_occupancy))
            db_write(server2, data_list, pcid)
            print str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())) + ': done ' + cid
        files.close()
    server1.close()
    server2.close()

