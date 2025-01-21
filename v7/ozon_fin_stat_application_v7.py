import os
import requests
import json
import numpy as np
import pandas as pd
import datetime
import tkinter as tk
from tkinter import Label, messagebox, Button, Tk, filedialog, ttk
from tkcalendar import Calendar # type: ignore

client_id = 'XXX' # ENTER YOUR CLIENT ID
api_key = 'XXX' # ENTER YOUR API KEY

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

def get_response_fbo(start_date, end_date, client_id, api_key):

    headers = {
    "Host": "api-seller.ozon.ru",
    "Client-Id": f"{client_id}",
    "Api-Key": f"{api_key}",
    "Content-Type": "application/json"
    }

    data = {
    "dir": "ASC",
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

def load_preprocess_finance(start, end, client_id, api_key):

    operations = []

    left = start
    if (end - left).days > 28:
        right = start + datetime.timedelta(days=28)
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
        right = left + datetime.timedelta(days=min(28,(end-left).days))         

    return pd.DataFrame(operations)

def load_preprocess_fbo(start, end, client_id, api_key):

    operations = []

    left = start
    if (end - left).days > 90:
        right = start + datetime.timedelta(days=90)
    else:
        right = end

    while (end - left).days >= 0:
        
        if right > end:
            right = end
        
        response = get_response_fbo(start_date=left.strftime('%Y-%m-%d'), end_date=right.strftime('%Y-%m-%d'),
                                    client_id=client_id, api_key=api_key)
        operations += response


        left = right + datetime.timedelta(days=1)
        right = left + datetime.timedelta(days=min(90,(end-left).days))         

    return pd.DataFrame(operations)

def get_order_date_from_operation_date(operation):
    if operation['operation_type_name'] == 'Доставка покупателю':
        return datetime.datetime.strptime(operation['posting']['order_date'],'%Y-%m-%d %H:%M:%S')
    if operation['operation_type_name'] == 'Доставка и обработка возврата, отмены, невыкупа':
        return datetime.datetime.strptime(operation['posting']['order_date'],'%Y-%m-%d %H:%M:%S')
    if operation['operation_type_name'] == 'Оплата эквайринга':
        return datetime.datetime.strptime(operation['posting']['order_date'],'%Y-%m-%d %H:%M:%S')
    if operation['operation_type_name'] == 'Приобретение отзывов на платформе':
        return datetime.datetime.strptime(operation['operation_date'],'%Y-%m-%d %H:%M:%S') - datetime.timedelta(days=14)
    if operation['operation_type_name'] == 'Трафареты':
        return datetime.datetime.strptime(operation['operation_date'],'%Y-%m-%d %H:%M:%S')
    return datetime.datetime.strptime(operation['operation_date'],'%Y-%m-%d %H:%M:%S')

def get_stat(df_finance, df_fbo, start_date, end_date):

    if not df_finance.empty:
        df_finance = df_finance.iloc[::-1].reset_index(drop=True)
        df_orders = df_finance[df_finance['operation_type_name'] == 'Доставка покупателю']
        df_orders.reset_index(inplace=True, drop=True)
        df_orders['services_cost'] = df_orders.loc[:,'services'].apply(lambda x: sum(x[i]['price'] for i in range(len(x))))
        df_orders = df_orders.join(df_orders['items'].apply(lambda x: pd.Series(x[0])))
        df_orders = df_orders.join(df_orders['posting'].apply(lambda x: pd.Series(x)))

        df_finance['order_date'] = df_finance.loc[:,['operation_type_name','operation_date','posting']].apply(
        lambda x: get_order_date_from_operation_date(x.to_dict()),axis=1)
    
        df_finance = df_finance[(df_finance['order_date'] >= start_date) & 
                                (df_finance['order_date'] <= end_date)].reset_index(drop=True)
        df_finance['order_date'] = df_finance['order_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    
        df_amount = df_finance.groupby('operation_type_name').agg({'amount' : ('count', 'mean', 'sum')}).astype(np.float16)
        df_total_amount = pd.DataFrame(('total', None, None, round(df_finance.amount.sum(),2))).T


    if not df_fbo.empty:
        df_financial_data = df_fbo['financial_data'].apply(lambda x: pd.Series(x))
        df_financial_data = df_financial_data.join(df_financial_data['products'].apply(lambda x: pd.Series(x[0])))
        df_financial_data = df_financial_data.drop(columns=['products','posting_services'])
        df_products = df_fbo['products'].apply(lambda x: pd.Series(x[0]))
        df_products = df_products.drop(columns=['price', 'currency_code'])
        df_anatytics_data = df_fbo['analytics_data'].apply(lambda x: pd.Series(x))
        df_fbo = df_fbo.join(df_financial_data)
        df_fbo = df_fbo.join(df_products)
        df_fbo = df_fbo.join(df_anatytics_data)
        df_fbo = df_fbo.drop(columns=['additional_data','financial_data', 'products', 'analytics_data','item_services',
                            'digital_codes','name','picking','client_price','currency_code','warehouse_id','sku','is_legal'])
        df_fbo['posting_number'] = df_fbo['posting_number'].astype(str)

        df_orders = pd.merge(df_fbo,df_orders,on='posting_number',how='inner')
        df_fbo = pd.merge(df_fbo,df_orders.loc[:,['posting_number','amount','accruals_for_sale']],on='posting_number',how='outer')

        df_city = df_orders.groupby('city').agg(
            {'quantity':'count', 'amount': ['mean', 'sum'], 'price': ['mean', 'sum']}).astype(np.float16)
        df_region = df_orders.groupby('region').agg(
            {'quantity':'count', 'amount': ['mean', 'sum'], 'price': ['mean', 'sum']}).astype(np.float16)
        df_payment_type = df_orders.groupby('payment_type_group_name').agg(
            {'quantity':'count', 'amount': ['mean', 'sum'],
            'payout': ['mean', 'sum'], 'price': ['mean', 'sum']}).astype(np.float16)
        df_delivery_type = df_orders.groupby('delivery_type').agg(
            {'quantity':'count', 'amount': ['mean', 'sum'], 
            'payout': ['mean', 'sum'], 'price': ['mean', 'sum']}).astype(np.float16)
        df_products_ordered = df_fbo.groupby(['offer_id','status']).agg(
            {'quantity':'count', 'amount': ['mean', 'sum'], 'accruals_for_sale' : ['mean', 'sum'],
            'payout': ['mean', 'sum'], 'price': ['mean', 'sum']}).astype(np.float16)
        df_status = df_fbo.groupby('status').agg(
            {'quantity':'count', 'amount': ['mean', 'sum'],
            'payout': ['mean', 'sum'], 'price': ['mean', 'sum']}).astype(np.float16)

        df_products = df_orders.groupby(['offer_id','status']).agg(
            {'amount': ['count', 'mean', 'sum'], 'accruals_for_sale' : ['mean', 'sum']
            }).astype(np.float16)
        products_total = ('total', None, df_orders.amount.count(), round(df_orders.amount.mean(),2), round(df_orders.amount.sum(),2),
                        round(df_orders.accruals_for_sale.mean(),2), round(df_orders.accruals_for_sale.sum(),2))
        df_products_total = pd.DataFrame(products_total).T
        df_orders['client_id'] = df_orders['posting_number'].apply(lambda x: x.split('-')[0]).astype(int)
        df_orders = df_orders.drop(columns=['posting','services','items','warehouse_id','type','operation_type_name','operation_type'])
        df_clients = df_orders.groupby('client_id').agg({'amount': 'count'})
        df_clients_order_count = pd.DataFrame(df_clients.value_counts()).rename(columns={'count':'num_clients'})
        df_unique_clients = pd.DataFrame([len(df_orders['client_id'].unique())], columns=['unique_clients'])
        df_total_clients = pd.DataFrame([len(df_orders['client_id'])], columns=['total_clients'])
        df_orders = df_orders.join(df_clients.rename(columns={'amount':'n_clients_purchases'}),on='client_id')

    if df_fbo.empty:
        return (df_finance, df_fbo, df_amount, pd.DataFrame(), df_total_amount, pd.DataFrame(), pd.DataFrame(),
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), 
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    return (df_finance, df_fbo, df_amount, df_products, df_total_amount, df_products_total, df_orders,
            df_clients, df_unique_clients, df_total_clients, df_clients_order_count, 
            df_city, df_region, df_payment_type, df_delivery_type, df_products_ordered, df_status)

def save_file(df_transactions, df_fbo, df_amount, df_products, df_total_amount, df_products_total, df_orders,
        df_clients, df_unique_clients, df_total_clients, df_clients_order_count, 
        df_city, df_region, df_payment_type, df_delivery_type, df_products_ordered, df_status, save_path):
    
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
        if not df_city.empty:
            df_city.to_excel(writer, sheet_name='region',startrow=0,startcol=0)
        if not df_region.empty:
            df_region.to_excel(writer, sheet_name='region',startrow=0,startcol=7)
        if not df_clients.empty:
            df_clients.to_excel(writer, sheet_name='clients', startrow=0, startcol=6)
        if not df_total_clients.empty:
            df_total_clients.to_excel(writer, sheet_name='clients', startrow=0, startcol=0, index = False)
        if not df_unique_clients.empty:
            df_unique_clients.to_excel(writer, sheet_name='clients', startrow=0, startcol=1, index = False)
        if not df_clients_order_count.empty:
            df_clients_order_count.to_excel(writer,sheet_name ='clients', startrow=0, startcol=3)
        if not df_payment_type.empty:
            df_payment_type.to_excel(writer, sheet_name='payment_type', startrow=0, startcol=0)
        if not df_delivery_type.empty:
            df_delivery_type.to_excel(writer, sheet_name='delivery_type', startrow=0, startcol=0)
        if not df_fbo.empty:
            df_fbo.to_excel(writer, sheet_name='orders', startrow=0, startcol=0)
        if not df_orders.empty:
            df_orders.to_excel(writer, sheet_name='deliveries',startrow=0,startcol=0)
        if not df_transactions.empty:
            df_transactions.to_excel(writer, sheet_name='transactions', startrow=0, startcol=0)

        for sheet in writer.sheets.values():
            sheet.autofit()
    
    
def process():
    try:
        global client_id
        global api_key
        global save_path
        start_date_str = L_start_date.cget('text')
        end_date_str = L_end_date.cget('text')
        start_order_date, end_order_date, start_finance_date, end_finance_date = get_dates(start_date_str, end_date_str)
        df_finance = load_preprocess_finance(start_finance_date, end_finance_date, client_id, api_key)
        df_fbo = load_preprocess_fbo(start_order_date, end_order_date, client_id, api_key)

        if df_finance.empty:
            raise AttributeError('No transactions over given period')

        (df_finance, df_fbo, df_amount, df_products, df_total_amount, df_products_total, df_orders, df_clients, 
        df_unique_clients, df_total_clients, df_clients_order_count, df_city, df_region, df_payment_type, 
        df_delivery_type, df_products_ordered, df_status) = get_stat(df_finance, df_fbo, start_order_date, end_order_date)
 
        save_file(df_finance, df_fbo, df_amount, df_products, df_total_amount, df_products_total, df_orders,
        df_clients, df_unique_clients, df_total_clients, df_clients_order_count, 
        df_city, df_region, df_payment_type, df_delivery_type, df_products_ordered, df_status, save_path)
        L_result.configure(text=' Document saved ')

    except Exception as e:
        messagebox.showinfo("Error",str(e))

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
    start_date_str = L_start_date.cget('text')
    end_date_str = L_end_date.cget('text')
    save_path = filedialog.asksaveasfilename(initialdir = "/",
                                          title = "Save File",
                                          defaultextension=".xlsx",
                                          initialfile=f"Ozon_financial_statistics_{start_date_str}_{end_date_str}.xlsx",
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
top.geometry("420x200")
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

L_result = Label(top, text="",background ='#0058f7',fg = "white",font='TimesNewRoman 10')
L_result.grid(row=5,column=0)
B_save_as = Button(top, text = "Save as", command=save_as,
                   background ='#0058f7',foreground='white',
                   activebackground='#0058f7',activeforeground='white',
                   borderwidth=5,font='TimesNewRoman 10',width = 20)

B_save_as.grid(row=5, column=1)

top.mainloop()