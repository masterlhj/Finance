#!/usr/bin/python3
# -*- coding: utf-8 -*-

import math
from sqlalchemy import exc

import re
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
import mysql.connector
from sqlalchemy import create_engine

# pwd=input('Enter Password for server:')
pwd = 'passwd'

'''
get_date_str(s) - 문자열 s 에서 "YYYY/MM" 문자열 추출
'''
def get_date_str(s):
    date_str = ''
    r = re.search("\d{4}/\d{2}", s)
    if r:
        date_str = r.group()
        date_str = date_str.replace('/', '-')

    return date_str

'''
* code: 종목코드
* fin_type = '0': 재무제표 종류 (0: 주재무제표, 1: GAAP개별, 2: GAAP연결, 3: IFRS별도, 4:IFRS연결)
* freq_type = 'Y': 기간 (Y:년, Q:분기)
'''
def get_finstate_naver(code, fin_type='0', freq_type='Y'):
    url_tmpl = 'http://companyinfo.stock.naver.com/v1/company/ajax/cF1001.aspx?' \
                   'cmp_cd=%s&fin_typ=%s&freq_typ=%s'

    url = url_tmpl % (code, fin_type, freq_type)
    #print(url)

    dfs = pd.read_html(url, encoding="utf-8")
    df = dfs[0]
    if df.iloc[0,0].find('해당 데이터가 존재하지 않습니다') >= 0:
        return None
    
    cols = list(df.columns)
    new_cols = []
    new_cols.append(cols[0][0])
    new_cols += [c[1] for c in cols[:-1]]
    df.columns = new_cols
    df.rename(columns={'주요재무정보':'date'}, inplace=True)
    df.set_index('date', inplace=True)
    df.columns = [get_date_str(x) for x in df.columns]

    dft = df.T
    dft.index = pd.to_datetime(dft.index)

    # remove if index is NaT
    dft = dft[pd.notnull(dft.index)]
    return dft

if __name__ == "__main__":
    # stock_finstate  table creation query
    create_table_sql = '''
        create table stock_finstate (
            `매출액` real,
            `영업이익` real,
            `세전계속사업이익` real,
            `당기순이익` real,
            `당기순이익(지배)` real,
            `당기순이익(비지배)` real,
            `자산총계` real,
            `부채총계` real,
            `자본총계` real,
            `자본총계(지배)` real, 
            `자본총계(비지배)` real, 
            `자본금` real, 
            `영업활동현금흐름` real, 
            `투자활동현금흐름` real, 
            `재무활동현금흐름` real, 
            `CAPEX` real, 
            `FCF` real, 
            `이자발생부채` real, 
            `영업이익률` real, 
            `순이익률` real, 
            `ROE(%)` real, 
            `ROA(%)` real, 
            `부채비율` real, 
            `자본유보율` real, 
            `유보율` real, -- GAPP only
            `EPS(원)` real, 
            `PER(배)` real, 
            `BPS(원)` real, 
            `PBR(배)` real, 
            `현금DPS(원)` real, 
            `현금배당수익률` real, 
            `현금배당성향(%)` real, 
            `현금배당성향` real, -- GAPP only
            `발행주식수(보통주)` real, 
            `code` varchar(20) not null, 
            `name` varchar(50) not null, 
            `fin_type` char(1)  not null, 
            `freq_type` char(1)  not null,
            `date` datetime not null
        );
    '''
    
    # table creation query
    create_index_sql = '''
        alter table `stock_finstate` add unique `unique_index` (`code`, `name`, `fin_type`, `freq_type`, `date`);
    '''

    # data insert query
    insert_sql = "insert into stock_finstate (%s) values (%s)"

    # connection string
    cnx_str = 'mysql+mysqlconnector://admin:'+pwd+'@localhost/findb'
    engine = create_engine(cnx_str, echo=False)
    
    # create table and index if not exists
    check_sql = "SELECT * FROM information_schema.tables WHERE table_name = 'stock_finstate'"
    df_check = pd.read_sql(check_sql, engine)
    if len(df_check) < 1:
        engine.execute(create_table_sql)
        engine.execute(create_index_sql)

    msg_text = {'3': 'IFRS개별', '4':'IFRS연결', 'Q':'분기', 'Y':'년' }

    # 전종목 (code, name) 읽기
    df_master = pd.read_sql("SELECT code, name FROM stock_master", engine)
    stock_counts = len(df_master)
    
    for ix_m, row_m in df_master.iterrows(): # 모든 종목에 대해
        for fin_type, freq_type in [('3', 'Q'), ('3', 'Y'), ('4', 'Q'), ('4', 'Y')]: # 개별/연결, 분기/년 조합
            # check if data exists
            print(ix_m, '/', stock_counts, row_m['code'], row_m['name'], msg_text[fin_type], msg_text[freq_type])

            check_sql_tmpl = "SELECT * FROM stock_finstate WHERE code='%s' and fin_type='%s'  and freq_type='%s' "
            check_sql = check_sql_tmpl % (row_m['code'], fin_type, freq_type)
            df_check = pd.read_sql(check_sql, engine)
            if len(df_check) >= 1:
                print('skip, data exists')
                continue

            df_fs = get_finstate_naver(row_m['code'], fin_type=fin_type, freq_type=freq_type)
            if df_fs is None: # skip if '해당 데이터가 존재하지 않습니다'
                continue
            df_fs['code'] = row_m['code']
            df_fs['name'] = row_m['name']
            df_fs['fin_type'] = fin_type
            df_fs['freq_type'] = freq_type

            for ix, row in df_fs.iterrows():
                cols = ', '.join("`{0}`".format(w) for w in row.keys())
                cols += ', ' + '`date`'
                vals = ', '.join("{0}".format('NULL' if math.isnan(w) else w) for w in row.values[:-4])
                vals += ', ' + ', '.join("'{0}'".format(w) for w in row.values[-4:]) 
                vals += ", '{0}'".format(ix)
                sql = insert_sql % (cols, vals)
                #print(sql)
                try:
                    engine.execute(sql)
                except exc.IntegrityError as err:
                    print('IntegrityError - 중복')