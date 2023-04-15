import numpy as np
import pandas as pd
import random
import os

#order detail
#(100,1000) 000 000 VND
#(4500,450000) Dollar
def change_order_detail():
    df = pd.read_csv('order_detail.csv')
    len_payment = len(df['order_id'].to_list())
    cash_type = ['Dollar'  if random.randint(1,2) == 1 else 'VND' for _ in range(len_payment)]
    df['unit'] = cash_type
    print(df.head())
    for i,row in df.iterrows():
        if row['unit'] == 'Dollar':
            row['total'] = random.randint(4500,44000)
        else:
            row['total'] = random.randint(100,1000) * 1000000
        df.iloc[[i]] =  row
    print(df.head())
    os.remove('order_detail.csv')
    df.to_csv('order_detail.csv',index=False)

#order - created column
#pick random 10000 in 100000 is 10-04-2023 - 10/04/2023 - 2023-04-10 - 2023/04/10
#transport 4 type
def change_order():
    df = pd.read_csv('order.csv')
    num_today = len(df['product_id'].to_list()) + 1
    list_today = ['10-04-2023', '10/04/2023', '2023-04-10' ,'2023/04/10']
    for i,row in df.iterrows():
        number = random.randint(1,2)
        if number == 1:
            if num_today != 0:
                row['created_at'] =  random.choice(list_today)
                df.iloc[[i]] = row
                num_today -= 1
                continue
        ele = row['created_at'].split('-')
        number = random.randint(1,4)
        str_date = ''
        # 2023-04-10
        if number == 2:
            # 10-04-2023
            str_date = ele[-1] + '-' + ele[1] + '-' + ele[0]
        if number == 3:
            # 10/04/2023
            str_date = ele[-1] + '/' + ele[1] + '/' + ele[0]
        if number == 4:
            # 2023/04/10
            str_date = ele[0] + '/' + ele[1] + '/' + ele[2]
        if number != 1:
            row['created_at'] = str_date
            df.iloc[[i]] = row
    os.remove('order.csv')
    df.to_csv('order.csv',index=False)

#null value => gom vo error schema 
#order, detail xuat hien cot null
#set 20 loi null o quantity trong order
#set 30 loi null o total | user_id trong order detail
def null_order():
    error_null_order = 20
    df = pd.read_csv('order.csv')
    for i,row in df.iterrows():
        number = random.randint(1,5)
        if number == 5:
            if error_null_order == 0:
                break
            number_2 = random.randint(1,2)
            if number_2 == 1:
                row['quantity'] = np.nan
            else:
                row['created_at'] = np.nan
            df.iloc[[i]] = row
            error_null_order -= 1
    os.remove('order.csv')
    df.to_csv('order.csv',index=False) 

def null_order_detail():
    error_null_order_detail = 30
    df = pd.read_csv('order_detail.csv')
    for i,row in df.iterrows():
        number = random.randint(1,5)
        if number == 5:
            if error_null_order_detail == 0:
                break
            number_2 = random.randint(1,2)
            if number_2 == 1:
                row['user_id'] = np.nan
            else:
                row['total'] = np.nan
            df.iloc[[i]] = row
            error_null_order_detail -= 1
    os.remove('order_detail.csv')
    df.to_csv('order_detail.csv',index=False)

if __name__ == '__main__':
    change_order_detail()
    change_order()
    null_order()
    null_order_detail()