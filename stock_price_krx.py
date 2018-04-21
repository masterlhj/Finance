#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import requests
from io import StringIO
from datetime import datetime, timedelta
import mysql.connector
from sqlalchemy import create_engine, exc

# pwd=input('Enter Password for server:')
pwd = 'passwd'

columns_map = { '년/월/일': 'date', '종가': 'close', '대비':'change', '거래량(주)':'volume', '거래대금(원)':'trading_value',
            '시가':'open', '고가': 'high','저가':'low', '시가총액(백만)':'mar_cap', '상장주식수(주)':'stock_count'}

# 주식코드 ISIN 얻기 (검사코드 계산)
def code_to_isin(code):
    val_list = [40, 47, 14] + [x * y for x, y in zip([int(ch) for ch in code], [1, 2, 1, 2, 1, 2])] + [0, 0]
    check_val = sum([int(v/10) + v % 10 for v in val_list]) % 10
    check_str = '0'
    if check_val > 0:
        check_str = str(10 - check_val)
    return 'KR7' + code + '00' + check_str

def get_price_krx(code, start=datetime(1900,1,1), end=datetime(2100,1,1)):
    start_str = start.strftime("%Y%m%d")
    end_str = end.strftime("%Y%m%d")

    # STEP 01: Generate OTP
    gen_otp_url = "http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx"
    gen_otp_data = {
        'name':'fileDown',
        'filetype':'csv',
        'url':'MKD/04/0402/04020100/mkd04020100t3_02',
        'isu_cd': code_to_isin(code),
        'fromdate':start_str,
        'todate':end_str,
    }

    r = requests.post(gen_otp_url, gen_otp_data)
    code = r.text

    # STEP 02: download
    down_url = 'http://file.krx.co.kr/download.jspx'
    down_data = {
        'code': code,
    }

    csv_str = requests.post(down_url, down_data).text
    if csv_str[-1] != '"': # KRX BUG (마지막 "이 누락되는 경우 대응)
        csv_str += '"'
    df = pd.read_csv(StringIO(csv_str), thousands=',', parse_dates=['년/월/일'])
    df.rename(columns=columns_map, inplace=True)
    #df.set_index('date', inplace=True)
    return df

if __name__ == "__main__":
    # stock_price_krx 테이블 생성
    create_table_sql = '''create table if not exists `stock_price_krx` (    
        `date` datetime,
        `code` varchar(20),
        `close` int,
        `change` int,
        `open` int,
        `high` int, 
        `low` int,  
        `volume` int,
        `trading_value` bigint,
        `mar_cap` bigint,
        `stock_count` bigint,
        primary key (date, code)
    );
    '''

    insert_sql = """
        insert into `stock_price_krx` (`date`, `code`, `close`, `change`, `open`, `high`, `low`, `volume`, `trading_value`, `mar_cap`, `stock_count`) 
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    
    cnx_str = 'mysql+mysqlconnector://admin:'+pwd+'@localhost/findb'
    engine = create_engine(cnx_str, echo=False)

    # create table if not exists
    engine.execute(create_table_sql)
    
    df_master = pd.read_sql("SELECT * FROM stock_master", engine)
    for inx, row in df_master.iterrows():
        print(inx, '/', len(df_master),row['code'], row['name'], end='')
        #  start: DB에 저장된 마지막 날짜 + 1일
        df_max = pd.read_sql('select * from stock_price_krx where code="%s" order by date desc limit 1' % row['code'], engine)
        
        last_date = datetime(1900,1,1)
        if len(df_max) > 0 and df_max['date'].iloc[0] != None:
            last_date = datetime.strptime(str(df_max['date'].iloc[0]), "%Y-%m-%d %H:%M:%S")
        start = last_date + timedelta(1)

        # end: 어제
        yday = datetime.today() - timedelta(1)
        end = datetime(yday.year, yday.month, yday.day)
        
        df_price = get_price_krx(row['code'], start, end)
        df_price['code'] = row['code']

        data_list = []
        for ix, r in df_price.iterrows():
            tup =( r['date'].strftime('%Y-%m-%d %H:%M:%S'), 
                      r['code'], r['close'], r['change'], r['open'], r['high'], r['low'], r['volume'], 
                      r['trading_value'], r['mar_cap'], r['stock_count'])
            data_list.append(tup) # insert many
        if len(data_list) <= 0:
            print()
            continue
            
        try:
            engine.execute(insert_sql, data_list)
        except exc.IntegrityError as err:
            print('IntegrityError - 중복, SKIP')
        print(', %d row inserted' % (len(df_price)))