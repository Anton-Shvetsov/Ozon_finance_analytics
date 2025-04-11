import os
import requests
import json
import numpy as np
import pandas as pd
import datetime
import tkinter as tk
from tkinter import Label, messagebox, Button, Tk, filedialog, ttk
from tkcalendar import Calendar # type: ignore

client_id_dict = {'--COMPANY NAME--' : '--CLIENT-ID--'} # ENTER YOUR COMPANY NAME AND CLIENT ID
api_key_dict = {'--COMPANY NAME--' : '--API-KEY--'}     # ENTER YOUR COMPANY NAME AND API KEY


def get_response_fbo(start_date, end_date, client_id, api_key):

    headers = {
    "Host": "api-seller.ozon.ru",
    "Client-Id": f"{client_id}",
    "Api-Key": f"{api_key}",
    "Content-Type": "application/json"
    }

    data = {
    "dir": "DESC",
    "filter": {
        "since": f"{start_date}T00:00:00.000Z",
        "status": "",
        "to": f"{end_date}T23:59:59.999Z"
    },
    "limit": 1000,
    "offset": 0,
    "translit": False,
    "with": {
        "analytics_data": True,
        "financial_data": True
    }
    }

    response = requests.post(url='https://api-seller.ozon.ru/v2/posting/fbo/list',headers=headers,data=json.dumps(data))
    if response.status_code == 200:
        return response.json()['result']
    
    raise ConnectionError(f'Data not loaded: status code {response.status_code}')


def get_response_fbs(start_date, end_date, client_id, api_key):

    headers = {
    "Host": "api-seller.ozon.ru",
    "Client-Id": f"{client_id}",
    "Api-Key": f"{api_key}",
    "Content-Type": "application/json"
    }

    data = {
    "dir": "DESC",
    "filter": {
        "since": f"{start_date}T00:00:00.000Z",
        "status": "",
        "to": f"{end_date}T23:59:59.999Z"
    },
    "limit": 1000,
    "offset": 0,
    "translit": False,
    "with": {
        "analytics_data": True,
        "financial_data": True
    }
    }

    response = requests.post(url='https://api-seller.ozon.ru/v3/posting/fbs/list',headers=headers,data=json.dumps(data))
    if response.status_code == 200:
        return response.json()['result']['postings']
    
    raise ConnectionError(f'Data not loaded: status code {response.status_code}')

def get_response_stocks(client_id, api_key):
    
    headers = {
    "Host": "api-seller.ozon.ru",
    "Client-Id": f"{client_id}",
    "Api-Key": f"{api_key}",
    "Content-Type": "application/json"
    }

    data = {
    "filter": {},
    "limit": 1000
    }

    response = requests.post(url='https://api-seller.ozon.ru/v4/product/info/stocks',headers=headers,data=json.dumps(data))
    if response.status_code == 200:
        return response.json()['items']
        
    print(response.json())
        
    raise ConnectionError(f'Data not loaded: status code {response.status_code}')

def get_response_finance(start_date, end_date, page, client_id, api_key):

    headers = {
        "Host": "api-seller.ozon.ru",
        "Client-Id": f"{client_id}",
        "Api-Key": f"{api_key}",
        "Content-Type": "application/json"
    }

    data = {
    "filter": {
        "date": {
        "from": f"{start_date}T00:00:00.000Z",
        "to": f"{end_date}T23:59:59.999Z"
        },
        "operation_type": [],
        "posting_number": "",
        "transaction_type": "all"
    },
    "page": page,
    "page_size": 1000
    }

    response = requests.post(url='https://api-seller.ozon.ru/v3/finance/transaction/list',headers=headers,data=json.dumps(data))

    if response.status_code == 200:
        return response.json()['result']
    raise ConnectionError(f'Data not loaded: status code {response.status_code}')

def load_preprocess_finance(start, end, client_id, api_key):

    operations = []

    left = start
    if (end - left).days > 27:
        right = start + datetime.timedelta(days=27)
    else:
        right = end


    while (end - left).days >= 0:
        
        if right > end:
            right = end

        page = 1
        while True:
                response = get_response_finance(start_date=left.strftime('%Y-%m-%d'), end_date=right.strftime('%Y-%m-%d'),
                                                page=page, client_id=client_id, api_key=api_key)
                if response['page_count']:
                    operations += response['operations']
                    page += 1
                else:
                    break

        left = right + datetime.timedelta(days=1)
        right = left + datetime.timedelta(days=min(27,(end-left).days))         

    return pd.DataFrame(operations)

def load_preprocess_orders(start, end, client_id, api_key, get_response):

    operations = []

    left = start
    if (end - left).days > 90:
        right = start + datetime.timedelta(days=90)
    else:
        right = end

    while (end - left).days >= 0:
        
        if right > end:
            right = end
        
        response = get_response(start_date=left.strftime('%Y-%m-%d'), end_date=right.strftime('%Y-%m-%d'),
                                    client_id=client_id, api_key=api_key)
        operations += response


        left = right + datetime.timedelta(days=1)
        right = left + datetime.timedelta(days=min(90,(end-left).days))         

    return pd.DataFrame(operations)

def load_preprocess_stocks(client_id, api_key):
    
    def preprocess_stocks(stock, ftype, availability):
        if not stock:
            return None
        for s in stock:
            if s['type'] == ftype:
                return s[availability]
        return 0

    stocks = get_response_stocks(client_id, api_key)
    df_stocks = pd.DataFrame(stocks)
    df_stocks['fbs_present'] = df_stocks['stocks'].apply(lambda x: preprocess_stocks(x, 'fbs', 'present'))
    df_stocks['fbo_present'] = df_stocks['stocks'].apply(lambda x: preprocess_stocks(x, 'fbo', 'present'))
    df_stocks['fbs_reserved'] = df_stocks['stocks'].apply(lambda x: preprocess_stocks(x, 'fbs', 'reserved'))
    df_stocks['fbo_reserved'] = df_stocks['stocks'].apply(lambda x: preprocess_stocks(x, 'fbo', 'reserved'))
    df_stocks = df_stocks.drop(columns = ['stocks','product_id']).dropna().reset_index(drop=True)
    return df_stocks

def calculate_services_cost(services):
    total_service_cost = 0
    for service in services:
        total_service_cost += service['price']
    return total_service_cost

def get_services_cost(df_finance):
    if 'operation_type_name' not in df_finance.keys():
        return pd.DataFrame() 
    df_services_cost = df_finance[df_finance['operation_type_name'] == 'Доставка покупателю'].reset_index(drop=True).loc[:,['services','posting','items']]
    if df_services_cost.empty:
        return pd.DataFrame() 
    df_services_cost.loc[:, 'posting_number'] = df_services_cost['posting'].apply(lambda x: x['posting_number'])
    df_services_cost.loc[:, 'all_items_services_cost'] = df_services_cost['services'].apply(calculate_services_cost)
    df_services_cost.loc[:, 'num_items'] = df_services_cost['items'].apply(lambda x: len(x))
    df_services_cost.loc[:, 'services_cost'] = df_services_cost['all_items_services_cost'] / df_services_cost['num_items']
    df_services_cost = df_services_cost.loc[:, ['posting_number','services_cost']]
    return df_services_cost

def get_order_date_from_operation_date(operation):
    if operation['operation_type_name'] == 'Доставка покупателю':
        return datetime.datetime.strptime(operation['posting']['order_date'],'%Y-%m-%d %H:%M:%S')
    if operation['operation_type_name'] == 'Доставка и обработка возврата, отмены, невыкупа':
        return datetime.datetime.strptime(operation['posting']['order_date'],'%Y-%m-%d %H:%M:%S')
    if operation['operation_type_name'] == 'Оплата эквайринга':
        return datetime.datetime.strptime(operation['posting']['order_date'],'%Y-%m-%d %H:%M:%S')
    if operation['operation_type_name'] == 'Сервисный сбор за интеграцию с логистической платформой':
        return datetime.datetime.strptime(operation['posting']['order_date'],'%Y-%m-%d %H:%M:%S')
    if operation['operation_type_name'] == 'Приобретение отзывов на платформе':
        return datetime.datetime.strptime(operation['operation_date'],'%Y-%m-%d %H:%M:%S') - datetime.timedelta(days=14)
    if operation['operation_type_name'] == 'Трафареты':
        return datetime.datetime.strptime(operation['operation_date'],'%Y-%m-%d %H:%M:%S')
    return datetime.datetime.strptime(operation['operation_date'],'%Y-%m-%d %H:%M:%S')

def process_finance(df_finance, start_date, end_date):
    df_finance['order_date'] = df_finance.loc[:,['operation_type_name','operation_date','posting']].apply(
    lambda x: get_order_date_from_operation_date(x.to_dict()),axis=1)
    df_finance = df_finance[(df_finance['order_date'] >= start_date) & 
                            (df_finance['order_date'] <= end_date)].reset_index(drop=True)
    df_finance['order_date'] = df_finance['order_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    df_amount = df_finance.groupby('operation_type_name').agg({'amount' : ('count', 'mean', 'sum')}).round(2)
    df_total_amount = pd.DataFrame(['total', None, None, round(df_finance.amount.sum(),2)]).T
    return df_finance, df_amount, df_total_amount
    

def preprocess_fbs(df_fbs):
    if df_fbs.empty:
        return pd.DataFrame()
    df = df_fbs.loc[:,['posting_number','order_number', 'status', 'in_process_at','products', 'financial_data', 'analytics_data']]
    df = df.rename(columns={'in_process_at':'created_at'})
    df['finance_data'] = df['financial_data'].apply(lambda x: x['products'])
    df['cluster_to'] = df['financial_data'].apply(lambda x: x['cluster_to'])
    df = df.explode(['products', 'finance_data']).reset_index(drop=True)
    products_df = pd.json_normalize(df['products']).loc[:,['offer_id','name','sku','quantity']]
    financial_df = pd.json_normalize(df['finance_data']).loc[:,['price','payout','commission_amount','commission_percent','total_discount_percent','actions']]
    analytics_df = pd.json_normalize(df['analytics_data']).loc[:,['delivery_type','payment_type_group_name']]
    df = df.drop(['products', 'financial_data', 'finance_data', 'analytics_data'], axis=1).reset_index(drop=True)
    df = pd.concat([df, products_df, financial_df, analytics_df], axis=1)
    df['scheme'] = 'fbs'
    return df

def preprocess_fbo(df_fbo):
    if df_fbo.empty:
        return pd.DataFrame()
    df = df_fbo.loc[:,['posting_number','order_number', 'status', 'created_at','products', 'financial_data', 'analytics_data']]
    df['finance_data'] = df['financial_data'].apply(lambda x: x['products'])
    df['cluster_to'] = df['financial_data'].apply(lambda x: x['cluster_to'])
    df = df.explode(['products', 'finance_data']).reset_index(drop=True)
    products_df = pd.json_normalize(df['products']).loc[:,['offer_id','name','sku','quantity']]
    financial_df = pd.json_normalize(df['finance_data']).loc[:,['price','payout','commission_amount','commission_percent','total_discount_percent','actions']]
    analytics_df = pd.json_normalize(df['analytics_data']).loc[:,['delivery_type','payment_type_group_name']]
    df = df.drop(['products', 'financial_data', 'finance_data', 'analytics_data'], axis=1).reset_index(drop=True)
    df = pd.concat([df, products_df, financial_df, analytics_df], axis=1)
    df['scheme'] = 'fbo'
    return df

def get_orders(df_fbo, df_fbs, df_finance):
    df_fbo = preprocess_fbo(df_fbo)
    df_fbs = preprocess_fbs(df_fbs)
    df_services_cost = get_services_cost(df_finance)
    df_orders = pd.concat([df_fbo,df_fbs]).sort_values('created_at', ascending=False, ignore_index=True)
    df_orders = pd.merge(df_orders, df_services_cost, how='left')
    df_orders.loc[:,'amount'] = df_orders['payout'] + df_orders['services_cost']
    df_orders['client_id'] = df_orders['posting_number'].apply(lambda x: x.split('-')[0]).astype(int, copy=False)
    df_orders['created_at_date'] = df_orders.created_at.apply(lambda x: datetime.datetime.strptime(x[:19],'%Y-%m-%dT%H:%M:%S').date())
    return df_orders

def get_cluster_data(df_orders, df_deliveries):
    df_region = df_deliveries.groupby('cluster_to').agg(
        {'amount': ['count','mean', 'sum'], 'price': ['mean', 'sum']}).round(2)
    df_region_orders = df_orders.groupby(['cluster_to','status']).agg(
        {'price':['count','mean','sum']}).astype(np.float32, copy=False)
    return df_region, df_region_orders

def get_payment_type_data(df_orders):
    return df_orders.groupby('payment_type_group_name').agg(
                {'amount': ['count','mean', 'sum'], 
                 'payout': ['mean', 'sum'], 'price': ['mean', 'sum']}).round(2)

def get_delivery_type_data(df_orders):
    return df_orders.groupby('delivery_type').agg(
                {'amount': ['count','mean', 'sum'],
                 'payout': ['mean', 'sum'], 'price': ['mean', 'sum']}).round(2)

def get_products_data(df_orders):
    return df_orders.groupby(['offer_id','status']).agg(
                {'quantity':'count', 'amount': ['mean', 'sum'],
                'payout': ['mean', 'sum'], 'price': ['mean', 'sum']}).round(2)

def process_stocks(df_stocks, df_deliveries):
    df_stocks_products_mean = df_deliveries.groupby('offer_id').agg({'amount': 'mean'}).round(2)
    df_stocks = pd.merge(df_stocks, df_stocks_products_mean, on='offer_id',how='outer')
    df_stocks = df_stocks.rename(columns={'amount':'mean_payment_amount'})
    df_stocks['fbs_stock_payment'] = (df_stocks['fbs_present'] + df_stocks['fbs_reserved']) * df_stocks['mean_payment_amount']
    df_stocks['fbo_stock_payment'] = (df_stocks['fbo_present'] + df_stocks['fbo_reserved']) * df_stocks['mean_payment_amount']
    df_stocks['total_payment'] = df_stocks['fbs_stock_payment'] + df_stocks['fbo_stock_payment']
    total_stocks = df_stocks.sum(axis=0)
    total_stocks['offer_id'] = 'total'
    total_stocks['mean_payment_amount'] = None
    df_stocks.loc[len(df_stocks)] = [None] * len(total_stocks) 
    df_stocks.loc[len(df_stocks)] = total_stocks
    return df_stocks

def get_clients_data(df_orders):
    df_deliveries = df_orders[df_orders['status'] == 'delivered']
    df_orders_cancelled = df_orders[df_orders['status'] == 'cancelled']
    df_orders_in_process = df_orders[(df_orders['status'] != 'cancelled') & (df_orders['status'] != 'delivered')]
    df_clients = df_deliveries.groupby('client_id').agg({'amount': 'count'}).rename(columns={'amount':'num_client_delivered'})
    df_clients_in_process = df_orders_in_process.groupby('client_id').agg({'price': 'count'}).rename(columns={'price':'num_client_in_progress'})
    df_clients_cancelled = df_orders_cancelled.groupby('client_id').agg({'price': 'count'}).rename(columns={'price':'num_client_cancelled'})
    df_clients_order_count = pd.DataFrame(df_clients.value_counts()).rename(columns={'count':'num_clients'})
    df_unique_clients = pd.DataFrame([len(df_orders['client_id'].unique())], columns=['unique_clients'])
    df_total_clients = pd.DataFrame([len(df_orders['client_id'])], columns=['total_clients'])
    df_clients = df_clients.join(df_clients_in_process,on='client_id',how='outer')
    df_clients = df_clients.join(df_clients_cancelled,on='client_id',how='outer')
    df_clients_date = df_deliveries.groupby(['client_id','created_at_date']).agg({'amount' : 'count'})
    df_num_repeated_clients = \
        pd.DataFrame([((df_clients_date.unstack().fillna(0) > 0).astype(int, copy=False).sum(axis=1) > 1).sum()],
                                            columns=['Num repeated clients'])
    if 'client_id' in df_clients.columns:
        df_clients = df_clients.set_index(df_clients['client_id']).drop(columns='client_id')
    df_clients = df_clients.fillna(0)
    df_clients['num_client_ordered'] = df_clients['num_client_delivered'] + df_clients['num_client_in_progress']
    df_clients_order_count_ordered = pd.DataFrame(df_clients['num_client_ordered'].value_counts()).rename(columns={'count':'num_clients_ordered'})

    return df_clients, df_clients_order_count, df_unique_clients, df_total_clients, df_num_repeated_clients, df_clients_order_count_ordered
    
def get_status_data(df_orders):
    return df_orders.groupby('status').agg(
        {'status':'count','amount': ['mean', 'sum'],
        'payout': ['mean', 'sum'], 'price': ['mean', 'sum']}).round(2)

def get_stat(df_finance, df_fbo, df_fbs, df_stocks, start_date, end_date):
    dict_df = {}
    df_orders = get_orders(df_fbo, df_fbs, df_finance)
    (df_clients, df_clients_order_count, df_unique_clients, df_total_clients,
    df_num_repeated_clients, df_clients_order_count_ordered) = get_clients_data(df_orders)
    df_orders = df_orders.join(df_clients,on='client_id')
    df_deliveries = df_orders[df_orders['status'] == 'delivered']
    dict_df['df_transactions'], dict_df['df_amount'], dict_df['df_total_amount'] = process_finance(df_finance, start_date, end_date)
    dict_df['df_orders'] = df_orders
    dict_df['df_deliveries'] = df_deliveries
    dict_df['df_region'], dict_df['df_region_orders'] = get_cluster_data(df_orders,df_deliveries)
    dict_df['df_clients'] = df_clients
    dict_df['df_unique_clients'] = df_unique_clients
    dict_df['df_total_clients'] = df_total_clients
    dict_df['df_num_repeated_clients'] = df_num_repeated_clients
    dict_df['df_clients_order_count'] = df_clients_order_count
    dict_df['df_clients_order_count_ordered'] = df_clients_order_count_ordered
    dict_df['df_products_ordered'] = get_products_data(df_orders)
    dict_df['df_status'] = get_status_data(df_orders)
    dict_df['df_stocks'] = process_stocks(df_stocks, df_deliveries)
    return dict_df

def save_file(save_path, df_transactions=pd.DataFrame(), df_orders=pd.DataFrame(), df_stocks=pd.DataFrame(), df_amount=pd.DataFrame(),
        df_products=pd.DataFrame(), df_total_amount=pd.DataFrame(), df_products_total=pd.DataFrame(), df_deliveries=pd.DataFrame(),
        df_clients=pd.DataFrame(), df_unique_clients=pd.DataFrame(), df_total_clients=pd.DataFrame(),
        df_clients_order_count=pd.DataFrame(), df_clients_order_count_ordered=pd.DataFrame(), df_num_repeated_clients=pd.DataFrame(),
        df_city=pd.DataFrame(), df_region=pd.DataFrame(), df_city_orders=pd.DataFrame(), df_region_orders=pd.DataFrame(),
        df_payment_type=pd.DataFrame(), df_delivery_type=pd.DataFrame(), df_products_ordered=pd.DataFrame(), df_status=pd.DataFrame()):
    
    with pd.ExcelWriter(save_path, engine='xlsxwriter') as writer:

        if not df_status.empty:
            df_status.to_excel(writer, sheet_name='amounts', startrow=0, startcol=0)
        if not df_amount.empty:
            df_amount.to_excel(writer, sheet_name='amounts', startrow=5+len(df_status), startcol=0)
        if not df_total_amount.empty:
            df_total_amount.to_excel(writer, sheet_name='amounts', 
                    startrow=9+len(df_amount)+len(df_status), startcol=0, index=False, header = False)
        if not df_products_ordered.empty:
            df_products_ordered.to_excel(writer, sheet_name='products',startrow=0 , startcol=0)
        if not df_products.empty:
            df_products.to_excel(writer, sheet_name='products', startrow=5+len(df_products_ordered), startcol=0)
        if not df_products_total.empty:
            df_products_total.to_excel(writer, sheet_name='products', 
                startrow=9+len(df_products)+len(df_products_ordered), startcol=0, index=False, header = False)
        if not df_stocks.empty:
            df_stocks.to_excel(writer, sheet_name='stocks',startrow=0 , startcol=0, index=False)
        if not df_city_orders.empty:
            df_city_orders.to_excel(writer, sheet_name='city',startrow=0,startcol=0)
        if not df_city.empty:
            df_city.to_excel(writer, sheet_name='city',startrow=0,startcol=7)
        if not df_region_orders.empty:
            df_region_orders.to_excel(writer, sheet_name='region',startrow=0,startcol=0)
        if not df_region.empty:
            df_region.to_excel(writer, sheet_name='region',startrow=0,startcol=7)
        if not df_clients.empty:
            df_clients.to_excel(writer, sheet_name='clients', startrow=0, startcol=6)
        if not df_total_clients.empty:
            df_total_clients.to_excel(writer, sheet_name='clients', startrow=0, startcol=0, index = False)
        if not df_unique_clients.empty:
            df_unique_clients.to_excel(writer, sheet_name='clients', startrow=0, startcol=1, index = False)
        if not df_clients_order_count_ordered.empty:
            df_clients_order_count_ordered.to_excel(writer,sheet_name ='clients', startrow=0, startcol=3)
        if not df_clients_order_count.empty:
            df_clients_order_count.to_excel(writer,sheet_name ='clients', startrow=len(df_clients_order_count_ordered)+2, startcol=3)
        if not df_num_repeated_clients.empty:
            df_num_repeated_clients.to_excel(writer,sheet_name ='clients', startrow=len(df_total_clients)+2, startcol=0, index=False)
        if not df_payment_type.empty:
            df_payment_type.to_excel(writer, sheet_name='payment_type', startrow=0, startcol=0)
        if not df_delivery_type.empty:
            df_delivery_type.to_excel(writer, sheet_name='delivery_type', startrow=0, startcol=0)
        if not df_orders.empty:
            df_orders.to_excel(writer, sheet_name='orders', startrow=0, startcol=0)
        if not df_deliveries.empty:
            df_deliveries.to_excel(writer, sheet_name='deliveries',startrow=0,startcol=0)
        if not df_transactions.empty:
            df_transactions.to_excel(writer, sheet_name='transactions', startrow=0, startcol=0)

        for sheet in writer.sheets.values():
            sheet.autofit()
    
    
def process():

    global client_id_dict
    global api_key_dict
    global save_path
    company = M_company.cget('text')
    client_id = client_id_dict[company]
    api_key = api_key_dict[company]

    start_date_str = L_start_date.cget('text')
    end_date_str = L_end_date.cget('text')
    start_order_date, end_order_date, start_finance_date, end_finance_date = get_dates(start_date_str, end_date_str)
    df_finance = load_preprocess_finance(start_finance_date, end_finance_date, client_id, api_key)
    if df_finance.empty:
        raise AttributeError('No transactions over given period')
    
    df_fbo = load_preprocess_orders(start_order_date, end_order_date, client_id, api_key, get_response_fbo)
    df_fbs = load_preprocess_orders(start_order_date, end_order_date, client_id, api_key, get_response_fbs)
    df_stocks = load_preprocess_stocks(client_id, api_key)

    dict_df = get_stat(df_finance, df_fbo, df_fbs, df_stocks, start_order_date, end_order_date)
    save_file(save_path, **dict_df)
    L_result.configure(text=' Document saved ')


def check_path_valid(save_path):
    filename, ext = os.path.splitext(save_path)
    if not filename:
        raise AttributeError('Filename must be indicated')
    if ext != '.xlsx':
        raise AttributeError('File must have an .xlsx extension')
    i = 1
    while os.path.isfile(save_path):
        save_path = filename + f' ({i})' + ext
        i += 1
    return save_path
    
def get_dates(start_date,end_date):
    start_date += ' 00:00:00'
    end_date += ' 23:59:59'
    start_date = datetime.datetime.strptime(start_date,'%Y-%m-%d %H:%M:%S')
    end_date = datetime.datetime.strptime(end_date,'%Y-%m-%d %H:%M:%S')
    check_dates_valid(start_date,end_date)
    end_finance_date = end_date + datetime.timedelta(days=14)
    today = datetime.datetime.today()
    if end_finance_date > today:
        end_finance_date = today
    return start_date, end_date, start_date, end_finance_date

def check_dates_valid(start_date,end_date):
    
    if start_date >= end_date:
        raise ValueError('Start date must be less then end date')
    period = end_date - start_date
    if period.days > 365:
        raise ValueError('Max period is 365 days')
    
def save_as():
    global save_path
    company = M_company.cget('text')
    start_date_str = L_start_date.cget('text')
    end_date_str = L_end_date.cget('text')
    save_path = filedialog.asksaveasfilename(initialdir = "/",
                                          title = "Save File",
                                          defaultextension=".xlsx",
                                          initialfile=f"{company}_financial_statistics_{start_date_str}_{end_date_str}.xlsx",
                                          filetypes = (("Excel files", "*.xlsx"), ('All Files', '*.*')),
                                          confirmoverwrite=False)
    save_path = check_path_valid(save_path)
    process()

def get_start_date():
    def save_start_date():
        date_old = L_start_date.cget('text')
        date_new = cal.selection_get()
        if str(date_old) != str(date_new):
            L_start_date.configure(text=date_new)
            L_result.configure(text='')
        root.withdraw()
        root.quit()     
    root = tk.Toplevel(top)
    root.config(background = "#0058f7")
    cal = Calendar(root,font="TimesNewRoman 12", selectmode='day',
                   background='#0058f7', foreground='white',
                   selectbackground='#0058f7', selectforeground='white',
                   headersbackground='#0058f7', headersforeground='white',
                   normalbackground='white', normalforeground='#0058f7',
                   weekendbackground ='white', weekendforeground='#0058f7',
                   othermonthbackground='white', othermonthforeground='white',
                   othermonthwebackground='white', othermonthweforeground='white',
                   bordercolor='#0058f7',tooltipbackground='#0058f7')
    cal.pack(fill="both", expand=True)
    style = ttk.Style()
    style.theme_use('alt')
    style.configure('TButton', background = 'white', foreground = '#0058f7',borderwidth=0,focusthickness=0,focuscolor='none')
    ttk.Button(root, text="Select", command=save_start_date).pack()
    root.mainloop()

def get_end_date():
    def save_end_date():
        date_old = L_end_date.cget('text')
        date_new = cal.selection_get()
        if str(date_old) != str(date_new):
            L_end_date.configure(text=date_new)
            L_result.configure(text='')
        root.withdraw()
        root.quit()     
    root = tk.Toplevel(top)
    root.config(background = "#0058f7")
    cal = Calendar(root,font="TimesNewRoman 12", selectmode='day',
                   background='#0058f7', foreground='white',
                   selectbackground='#0058f7', selectforeground='white',
                   headersbackground='#0058f7', headersforeground='white',
                   normalbackground='white', normalforeground='#0058f7',
                   weekendbackground ='white', weekendforeground='#0058f7',
                   othermonthbackground='white', othermonthforeground='white',
                   othermonthwebackground='white', othermonthweforeground='white',
                   bordercolor='#0058f7',tooltipbackground='#0058f7')
    cal.pack(fill="both", expand=True)
    style = ttk.Style()
    style.theme_use('alt')
    style.configure('TButton', background = 'white', foreground = '#0058f7',borderwidth=0,focusthickness=0,focuscolor='none')
    ttk.Button(root, text="Select",command=save_end_date).pack()
    root.mainloop()
    

top = Tk()
start_date = None
end_date = None
top.title("OzonFinancialStatistics")
top.geometry("420x240")
top.config(background = "#0058f7")

Label(top, text="",background ='#0058f7').grid(row=0,column=0)

L_start_date = Label(top, text=" Select start date ",background ='#0058f7',fg = "white",font='TimesNewRoman 10')
L_start_date.grid(row=1,column=1)
B_start_date = Button(top, text=" Select start date ", command=get_start_date,
                   background ='#0058f7',foreground='white',
                   activebackground='#0058f7',activeforeground='white',
                   borderwidth=5,font='TimesNewRoman 10',width = 20)
B_start_date.grid(row=1, column=0)

Label(top, text="",background ='#0058f7',width=30).grid(row=2,column=0)

L_end_date = Label(top, text=" Select end date ",background ='#0058f7',fg = "white",font='TimesNewRoman 10')
L_end_date.grid(row=3,column=1)
B_end_date = Button(top, text=" Select end date ", command=get_end_date,
                   background ='#0058f7',foreground='white',
                   activebackground='#0058f7',activeforeground='white',
                   borderwidth=5,font='TimesNewRoman 10',width = 20)
B_end_date.grid(row=3, column=0)

Label(top, text="",background ='#0058f7',width=30).grid(row=4,column=0)

choices = list(client_id_dict.keys())
V_company = tk.StringVar(top)
V_company.set(list(client_id_dict.keys())[0])
M_company = tk.OptionMenu(top, V_company, *choices)
M_company.configure(background ='#0058f7',foreground='white', activebackground='#0058f7', 
            activeforeground='white',width=10,font='TimesNewRoman 10')
M_company.grid(row=5, column=0)

Label(top, text="",background ='#0058f7',width=30).grid(row=6,column=0)

L_result = Label(top, text="",background ='#0058f7',fg = "white",font='TimesNewRoman 10')
L_result.grid(row=5,column=1)
B_save_as = Button(top, text = "Save as", command=save_as,
                   background ='#0058f7',foreground='white',
                   activebackground='#0058f7',activeforeground='white',
                   borderwidth=5,font='TimesNewRoman 10',width = 20)

B_save_as.grid(row=7, column=1)

top.mainloop()