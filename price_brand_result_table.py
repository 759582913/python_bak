# -*- coding=UTF-8 -*-
import psycopg2
import time
import sys
import yaml
# price_duan_brand_table"db--report_inner"

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

def db_execute_get(server, sql):
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

def get_smid_dict(server, pcid, cid):
    dbconn = server.cursor()
    sql = "select distinct price_sub,sm_id from submarket.mj_price_duan_smid_pcid{} where cid = '{}';".format(pcid, cid)
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
        print(e)
        server.rollback()
    dbconn.close()
    return submarket_dict

def get_price_duan(server, pcid, cid):
    dbconn = server.cursor()
    report_table = "del_report_pcid{}.report_pcid{}cid{}".format(pcid, pcid, cid)
    sql = "select distinct price from {} where datamonth = '201612' order by price".format(report_table)
    price_duan_result = []
    try:
        dbconn.execute(sql)
        server.commit()
        price_truple = dbconn.fetchall()
        price_result = []

        for i in price_truple:
            price = i[0]
            price_result.append(price)
        length = len(price_result)
        min_price = str(min(price_result))
        low = str(price_result[int(round(length * 0.45, 0))])
        middle = str(price_result[int(round(length*0.85, 0))])
        middle_high = str(price_result[int(round(length * 0.97, 0))])
        high = str(max(price_result))
        price_duan_result.append((min_price, low, middle, middle_high, high))
    except Exception, e:
        print (e)
        server.rollback()
    dbconn.close()
    return price_duan_result

def brand_price_duan_sql(pcid, cid, price_duan_list, price_duan_dict, p1='p1', p2='p2', p3='p3', p4='p4'):
    conf_files = open(sys.path[0] + "/sub_brand_sql.yaml")
    conf = yaml.load(conf_files)
    conf_files.close()
    sql = conf['brand_price_duan_data']
    brand_table = "brand_analysis.brand_submarkert_pcid{}".format(pcid)
    report_table = "del_report_pcid%s.report_pcid%scid%s" % (pcid, pcid, cid)
    price_duan_sql_dict = dict()
    for price_tag in price_duan_list:
        min_price = price_tag[0]
        low = price_tag[1]
        middle = price_tag[2]
        middle_high = price_tag[3]
        high = price_tag[4]
        if p1 in price_duan_dict.keys():
            price_low = 'and price >=' + min_price + ' and price <' + low
            price_id1 = p1
            # price_low_duan = u'低端'
            price_low_duan = price_duan_dict[price_id1]
            sql_low = sql % (brand_table, price_low_duan, price_id1, report_table, price_low)
            price_duan_sql_dict[price_id1] = sql_low
        else:
            pass
        if p2 in price_duan_dict.keys():
            price_middle = 'and price >=' + low + ' and price <' + middle
            price_id2 = p2
            # price_middle_duan = u'中端'
            price_middle_duan = price_duan_dict[price_id2]
            sql_middle = sql % (brand_table, price_middle_duan, price_id2, report_table, price_middle)
            price_duan_sql_dict[price_id2] = sql_middle
        else:
            pass
        if p3 in price_duan_dict.keys():
            price_middle_high = 'and price >=' + middle + ' and price <' + middle_high
            price_id3 = p3
            # price_middle_high_duan = u'中高端'
            price_middle_high_duan = price_duan_dict[price_id3]
            sql_middle_high = sql % (brand_table, price_middle_high_duan, price_id3, report_table, price_middle_high)
            price_duan_sql_dict[price_id3] = sql_middle_high
        else:
            pass
        if p4 in price_duan_dict.keys():
            price_high = 'and price >=' + middle_high + ' and price <' + high
            price_id4 = p4
            # price_high_duan = u'高端'
            price_high_duan = price_duan_dict[price_id4]
            sql_high = sql % (brand_table, price_high_duan, price_id4, report_table, price_high)
            price_duan_sql_dict[price_id4] = sql_high
        else:
            pass
    return price_duan_sql_dict

def cat_price_duan_sql(pcid, cid, price_duan_list, price_duan_dict, p1='p1', p2='p2', p3='p3', p4='p4'):
    conf_files = open(sys.path[0] + "/sub_brand_sql.yaml")
    conf = yaml.load(conf_files)
    conf_files.close()
    sql = conf['cat_price_duan_data']
    brand_table = "brand_analysis.brand_submarkert_pcid{}".format(pcid)
    report_table = "del_report_pcid%s.report_pcid%scid%s" % (pcid, pcid, cid)
    cat_price_duan_sql_dict = dict()
    for price_tag in price_duan_list:
        min_price = price_tag[0]
        low = price_tag[1]
        middle = price_tag[2]
        middle_high = price_tag[3]
        high = price_tag[4]
        if p1 in price_duan_dict.keys():
            price_low = 'and price >=' + min_price + ' and price <' + low
            price_id1 = p1
            # price_low_duan = u'低端'
            price_low_duan = price_duan_dict[price_id1]
            cat_sql_low = sql % (brand_table, cid, price_low_duan, price_id1, report_table, price_low)
            cat_price_duan_sql_dict[price_id1] = cat_sql_low
        else:
            pass
        if p2 in price_duan_dict.keys():
            price_middle = 'and price >=' + low + ' and price <' + middle
            price_id2 = p2
            # price_middle_duan = u'中端'
            price_middle_duan = price_duan_dict[price_id2]
            cat_sql_middle = sql % (brand_table, cid, price_middle_duan, price_id2, report_table, price_middle)
            cat_price_duan_sql_dict[price_id2] = cat_sql_middle
        else:
            pass
        if p3 in price_duan_dict.keys():
            price_middle_high = 'and price >=' + middle + ' and price <' + middle_high
            price_id3 = p3
            # price_middle_high_duan = u'中高端'
            price_middle_high_duan = price_duan_dict[price_id3]
            cat_sql_middle_high = sql % (brand_table, cid, price_middle_high_duan, price_id3, report_table, price_middle_high)
            cat_price_duan_sql_dict[price_id3] = cat_sql_middle_high
        else:
            pass
        if p4 in price_duan_dict.keys():
            price_high = 'and price >=' + middle_high + ' and price <' + high
            price_id4 = p4
            # price_high_duan = u'高端'
            price_high_duan = price_duan_dict[price_id4]
            cat_sql_high = sql % (brand_table, cid, price_high_duan, price_id4, report_table, price_high)
            cat_price_duan_sql_dict[price_id4] = cat_sql_high
        else:
            pass
    return cat_price_duan_sql_dict

if __name__ == "__main__":
    server1 = connect_pg("basic")
    server2 = connect_pg("report_inner")
    pcid = '5'
    files = open(sys.path[0] + '/pcid5_d.txt', 'r')

    for row in files.readlines():
        cid = row.strip('\n')
        print str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())) + ' start ' + cid + ' brand'
        price_dict = get_smid_dict(server1, pcid, cid)
        price_list = get_price_duan(server2, pcid, cid)
        for key1, value1 in brand_price_duan_sql(pcid, cid, price_list, price_dict, p1='p1', p2='p2', p3='p3', p4='p4').items():
            db_execute(server2, value1)
        print str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())) + ' done ' + cid + ' brand'
        print str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())) + ' start ' + cid + ' cat'
        for key2, value2 in cat_price_duan_sql(pcid, cid, price_list, price_dict, p1='p1', p2='p2', p3='p3',p4='p4').items():
            db_execute(server2, value2)
        print str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())) + ' done ' + cid + ' cat'
    server1.close()
    server2.close()
    files.close()
