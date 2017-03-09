# -*- coding=UTF-8 -*-

import pandas as pd
import psycopg2
import sys
import time
from sqlalchemy import create_engine
import yaml
import multiprocessing 

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
    return result

def get_date_list(server, pcid, cid, brand, sm_id):
    sql = "select distinct datamonth from brand_analysis.brand_submarkert_pcid{} where cid = '{}' and brand = '{}' \
and sm_id = '{}' order by datamonth".format(pcid, cid, brand, sm_id)
    dbconn = server.cursor()
    result = []
    try:
        dbconn.execute(sql)
        result = dbconn.fetchall()
        server.commit()
    except Exception, e:
        print (e)
    dbconn.close()
    return result

def get_brand_list(server, pcid, cid):
    sql = "select distinct brand from brand_analysis.brand_submarkert_pcid{} where cid = '{}' and \
brand not in ('其他','empty','other/其他','other','其他/other','其它','','{}') and brand is not null and brand not like '%\%%' and brand not like '%\"%';".format(pcid, cid, cid)
    dbconn = server.cursor()
    result = []
    try:
        dbconn.execute(sql)
        result = dbconn.fetchall()
        server.commit()
    except Exception, e:
        print (e)
    dbconn.close()
    return result

def brand_submarket_sql(pcid, cid, brand, sm_id):
    conf_files = open(sys.path[0] + "/submarket.yaml")
    conf = yaml.load(conf_files)
    conf_files.close()
    sql = conf['brand_submarket_sql']

    return sql % (pcid, cid, brand, sm_id, pcid, cid)

def calculate_occupancy_sub(df, date_ls, sale_list):
    for month in date_ls:
        for item in sale_list:
            if df.loc['sub_{}'.format(item), "{}".format(month)] == 0 or df.loc['sub_{}'.format(item), "{}".format(month)] is None:
                df.loc['{}_occupancy_sub'.format(item), "{}".format(month)] = None
            else:
                df.loc['{}_occupancy_sub'.format(item), "{}".format(month)] =round((df.loc['{}'.format(item), "{}".format(month)] / df.loc['sub_{}'.format(item), "{}".format(month)])*100, 2)
    return df

def calculate_rate_ring(df, date_ls, sale_list, occupany_ls):
    for i in range(0, len(date_ls)):
        if i+1 < len(date_ls):
            last_month = date_ls[i]
            month = date_ls[i + 1]
            for item in sale_list:
                sd = df.loc['{}'.format(item), "{}".format(last_month)]
                if sd == 0 or sd is None or df.loc['{}'.format(item), '{}'.format(month)] is None:
                    df.loc['rate_ring_{}'.format(item), '{}'.format(month)] = None
                else:
                    df.loc['rate_ring_{}'.format(item), '{}'.format(month)] = round((df.loc['{}'.format(item), '{}'.format(month)] / df.loc['{}'.format(item), '{}'.format(last_month)]-1)*100, 2)
            for item_oc in occupany_ls:
                sd = df.loc['{}'.format(item_oc), "{}".format(last_month)]
                if sd == 0 or df.loc['{}'.format(item_oc), '{}'.format(month)] is None or sd is None:
                    df.loc['rate_ring_{}'.format(item_oc), '{}'.format(month)] = None
                else:
                    df.loc['rate_ring_{}'.format(item_oc), '{}'.format(month)] = round(df.loc['{}'.format(item_oc), '{}'.format(
                        month)]-df.loc['{}'.format(item_oc), '{}'.format(last_month)], 2)
        else:
            pass
    return df

def calculate_rate_year(df, date_ls, sale_list):
    if len(date_ls) > 11:
        for i in range(12, len(date_ls)):
            if i + 1 < len(date_ls)+1:
                last_year_month = date_ls[i - 12]
                month = date_ls[i]
                for item in sale_list:
                    sd = df.loc['{}'.format(item), '{}'.format(last_year_month)]
                    if sd == 0 or sd is None:
                        df.loc['rate_year_{}'.format(item), '{}'.format(month)] = None
                    else:
                        df.loc['rate_year_{}'.format(item), '{}'.format(month)] = round((df.loc['{}'.format(item), '{}'.format(month)] / df.loc[\
                                '{}'.format(item), '{}'.format(last_year_month)]-1)*100, 2)
            else:
                pass
    else:
        pass
    return df

def calculate_season(df, date_ls, sale_list):
    for i in range(2, len(date_ls)):
        if i + 1 < len(date_ls)+1:
            last_season_month = date_ls[i - 2]
            month = date_ls[i]
            for item in sale_list:
                df.loc['season_{}'.format(item), '{}'.format(month)] = df.loc['{}'.format(item), '{}'.format(last_season_month):'{}'.format(month)].sum()
                df.loc['season_brand_{}'.format(item), '{}'.format(month)] = df.loc['brand_{}'.format(item), '{}'.format(last_season_month):'{}'.format(month)].sum()
                if df.loc['season_brand_{}'.format(item), '{}'.format(month)] == 0 or df.loc['season_brand_{}'.format(item), '{}'.format(month)] is None:
                    df.loc['season_{}_occupancy_brand'.format(item), '{}'.format(month)] = None
                else:
                    df.loc['season_{}_occupancy_brand'.format(item), '{}'.format(month)] = round((df.loc['season_{}'.format(item), '{}'.format(month)] /df.loc['season_brand_{}'.format(item), '{}'.format(month)]) * 100, 2)
        else:
            pass
    return df

def calculate_season_raise(df, date_ls, sale_list):
    if len(date_ls) > 4:
        for i in range(5, len(date_ls)):
            if i + 1 < len(date_ls)+1:
                last_season = date_ls[i - 3]
                month = date_ls[i]
                for item in sale_list:
                    if df.loc['season_{}'.format(item), '{}'.format(last_season)] == 0 or df.loc['season_{}'.format(item), '{}'.format(last_season)] is None:
                        df.loc['season_{}_raise'.format(item), '{}'.format(month)] = None
                    else:
                        df.loc['season_{}_raise'.format(item), '{}'.format(month)] = round((df.loc['season_{}'.format(item), '{}'.format(month)] / df.loc[\
                            'season_{}'.format(item), '{}'.format(last_season)]-1)*100, 2)

                    if df.loc['season_brand_{}'.format(item), '{}'.format(month)] is None or df.loc['season_brand_{}'.format(item), '{}'.format(last_season)] is None or df.loc['season_brand_{}'.format(item), '{}'.format(month)] == 0 or df.loc['season_brand_{}'.format(item), '{}'.format(last_season)] == 0:
                        df.loc['season_{}_occupancy_brand_raise'.format(item), '{}'.format(month)] = None
                    else:
                        df.loc['season_{}_occupancy_brand_raise'.format(item), '{}'.format(month)] = round((df.loc['season_{}'.format(item), '{}'.format(month)]/df.loc['season_brand_{}'.format(item), '{}'.format(month)]-df.loc['season_{}'.format(item), '{}'.format(last_season)]/df.loc['season_brand_{}'.format(item), '{}'.format(last_season)])*100, 2)
            else:
                pass
    else:
        pass
    return df

def get_id(server, pcid, cid, brand):
    dbconn = server.cursor()
    brand_id_sql = "select distinct sm_id,submarket from brand_analysis.brand_submarkert_pcid{} where cid = '{}' and \
brand = '{}';".format(pcid, cid, brand)
    brand_id_dict = dict()
    try:
        dbconn.execute(brand_id_sql)
        sm_ls = dbconn.fetchall()
        sm_dict = dict()
        for sm in sm_ls:
            sm_id = sm[0]
            submarket = sm[1]
            sm_dict[sm_id] = submarket
            brand_id_dict[brand] = sm_dict
            server.commit()
    except Exception, e:
        print (e)
    dbconn.close()
    return brand_id_dict

def calculate_data_sql(pcid, cid, brand):
    conf_files = open(sys.path[0] + "/submarket.yaml")
    conf = yaml.load(conf_files)
    conf_files.close()
    sql = conf['calculate_data']

    return sql % (pcid, cid, brand, pcid, cid, brand)

def update_sql(pcid, cid):
    conf_files = open(sys.path[0] + "/submarket.yaml")
    conf = yaml.load(conf_files)
    conf_files.close()
    sql = conf['update_brand_sub_sql']

    return sql % (pcid, cid)

def assist_id_dict(pcid, cid, brand, submarket, sm_id, date_ls):
    assist_id_dict = dict()
    i = 0
    pcid_list = []
    cid_list = []
    brand_list = []
    sub_list = []
    sm_id_list = []
    while i < len(date_ls):
        pcid_list.append(pcid)
        cid_list.append(cid)
        brand_list.append(brand)
        sub_list.append(submarket)
        sm_id_list.append(sm_id)
        i += 1
    assist_id_dict["pcid"] = pcid_list
    assist_id_dict["cid"] = cid_list
    assist_id_dict["brand"] = brand_list
    assist_id_dict["submarket"] = sub_list
    assist_id_dict["sm_id"] = sm_id_list
    
    return assist_id_dict

def start_job(cid,pcid):
    server1 = connect_pg('report_inner')
    sale_occupancy_list = ['biz30day_occupancy_brand', 'total_sold_price_occupancy_brand', 'biz30day_occupancy_sub', 'total_sold_price_occupancy_sub']
    # 初始化数据库连接
    engine = create_engine('postgresql://zczx_write:zczx112211@192.168.1.99:5432/report_inner')
    sale_list = ['biz30day', 'total_sold_price']
    print str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())) + ': start ' + cid
    for brands in get_brand_list(server1, pcid, cid):
        brand = brands[0].replace("'", "''")
        brands_dict = get_id(server1, pcid, cid, brand)
        for key1, value1 in brands_dict[brand].items():
            sm_id = key1
            submarket = value1
            date_list = []
            for date in get_date_list(server1, pcid, cid, brand, sm_id):
                datamonth = date[0]
                date_list.append(datamonth)
            brand_sub_sql = str(brand_submarket_sql(pcid, cid, brand, sm_id))
            print str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())) + ': start ' + brand + ' - ' + sm_id
            try:
                brand_sub_df = pd.read_sql(brand_sub_sql, engine, index_col=['datamonth'])
                if brand_sub_df.head() is not None:
                    assist_fileds_dict = {'rate_ring_biz30day':0,'rate_ring_total_sold_price':0,'rate_year_biz30day':0,'rate_year_total_sold_price':0,'biz30day_occupancy_sub':0,'total_sold_price_occupancy_sub':0,'rate_ring_biz30day_occupancy_brand':0,'rate_ring_total_sold_price_occupancy_brand':0,'rate_ring_biz30day_occupancy_sub':0,'rate_ring_total_sold_price_occupancy_sub':0,'season_biz30day':0,'season_total_sold_price':0,'season_biz30day_raise':0,'season_total_sold_price_raise':0,'season_brand_biz30day':0,'season_brand_total_sold_price':0,'season_biz30day_occupancy_brand':0,'season_total_sold_price_occupancy_brand':0,'season_biz30day_occupancy_brand_raise':0,'season_total_sold_price_occupancy_brand_raise':0,}
                    assist_df = pd.DataFrame(assist_fileds_dict, index=date_list)
                    df = brand_sub_df.join(assist_df, how='left')
                    brand_sub_df_t = df.T
                    sub_occupancy_df = calculate_occupancy_sub(brand_sub_df_t, date_list, sale_list)
                    rate_ring_df = calculate_rate_ring(sub_occupancy_df, date_list, sale_list, sale_occupancy_list)
                    rate_year_df = calculate_rate_year(rate_ring_df, date_list, sale_list)
                    season_df = calculate_season(rate_year_df, date_list, sale_list)
                    season_raise = calculate_season_raise(season_df, date_list, sale_list)

                    last_df = season_raise.T
                    assist_id_df = pd.DataFrame(assist_id_dict(pcid, cid, brand, submarket, sm_id, date_list), index=date_list)
                    final_df = last_df.join(assist_id_df, how='left')
                    final_df.to_sql('brand_submarkert_pcid{}'.format(pcid), engine, schema='submarket', if_exists='append', chunksize=100000)
                else:
                    pass
            finally:
                pass
    print str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())) + ': done ' + cid
    server1.close()

if __name__ == "__main__":
    server = connect_pg('basic')
    server1 = connect_pg('report_inner')
    pool_size = 4
    task_list = []
    for pcid in ['2']:
        # 获取cid列表
        pool = multiprocessing.Pool(processes=pool_size)
        cid_sql = "select distinct cid from submarket.dw_submarket_smid_pcid{} order by cid".format(pcid)
        for row in db_get(server, cid_sql):
            cid = row[0]
            pool.apply_async(start_job, (cid, pcid))
            db_execute(server1, update_sql(pcid, cid))
        pool.close()
        pool.join()
    server.close()

