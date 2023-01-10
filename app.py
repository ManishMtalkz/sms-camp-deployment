import pandas as pd
import numpy as np
import json
import io
import ast
import requests
from datetime import datetime
from flask import Flask, jsonify
from flask import Flask, render_template
from flask import*
import logging
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# import nltk
import re
# import string
# from nltk.corpus import stopwords
# nltk.download('punkt')
# nltk.download('stopwords')
# from nltk.tokenize import word_tokenize
#--------------------data pre-processing------------------------#
#dataset1
  
app = Flask(__name__)

def dataset():
    
    #-------------------- data preprocessing----------------------#
        
    df1 =pd.read_csv("https://mtalkzplatformstorage.blob.core.windows.net/mtalkz-files-transfer/ynX1KNPiiyFRLwS.csv")
    # df1.dtypes
    #dataset2
    df2 = pd.read_csv("https://mtalkzplatformstorage.blob.core.windows.net/mtalkz-files-transfer/Qxozys6Bpi66Cvt.csv")
    df2.dtypes
    df2.rename(columns = {'Campaign Name':'Campaign name'}, inplace = True)
    merge_dff = pd.merge(df1, df2, on ='Number',how='outer')
    merge_dff.head()
    #null value present in each column.-----
    merge_dff.isnull().sum()
    
    #-----------------Rename column-------------------
    merge_dff.rename(columns = {'Location_y':'Click Location','Location_x':'Provider Location','Campaign name_x':'Campaign name','Count':'Clicks'}, inplace = True)
    
    # ---------------------dealing with null values-------------------
    merge_dff= merge_dff.fillna(value={'Provider Location':'unknown' , 'Provider':'unknown' , 'Browser':'unknown' , 'Platform':'unknown'   ,'Status':'No','Clicks':0,'Click Location':'unknown'})
    merge_dff.drop(['Campaign name_y'], inplace = True,axis = 1)
    merge_dff['Clicks'] = merge_dff['Clicks'].astype(int)
    merge_dff['Status'] = merge_dff['Status'].replace(['Delivery','Other'], ['Delivered','Failed'])


    d = dict();

    #-----------------------calculate diffrence btwn sent-time and delivered time---------#
    # convert send time and Delivered time into datetime datatype----
    merge_dff["Send Time"] =  pd.to_datetime(merge_dff["Send Time"], infer_datetime_format=True)
    merge_dff["Delivered Time"] =  pd.to_datetime(merge_dff["Delivered Time"], infer_datetime_format=True)
    #Diffrence of send time and delivered time.-------
    merge_dff["Diffrence Time"] = merge_dff["Delivered Time"] - merge_dff["Send Time"]
    seconds = merge_dff["Diffrence Time"].astype('timedelta64[s]').astype(np.int32)
    merge_dff["Diffrence Secs"] = seconds
    # print(merge_dff.dtypes)

        
    #return delivered rate of caimpaign  
    total_sent = merge_dff['Message'].count()
    # print("total messages sent",total_sent)
    num_delivered = (merge_dff['Status'] == 'Delivered').sum()
    # print(f"number of messages delivered in APP1\t{num_delivered}")
    delivered_rate = (num_delivered / total_sent) * 100
    d['delivered_rate'] = delivered_rate

    #response rate-----------
    total_sent = int(merge_dff['Status'].count())
    sent = json.dumps( total_sent)
    clicked_msg = int((merge_dff['Clicks'] > 0).sum())
    clicked = json.dumps(clicked_msg)
    response_rate = (clicked_msg  / total_sent)*100
    d['response_rate']=response_rate
   
    

    #  frequeancy of status of the messages
    df4 =  merge_dff.groupby('Status').size().sort_values(ascending=False).reset_index()
    df4.rename(columns =  {0:'No_of_msges'},inplace = True)
    j = df4.set_index('Status')['No_of_msges'].to_json()
    sta = ast.literal_eval(j)
    sta['Total Sent'] = sent
    sta['Clicked Messages'] =  clicked
    
    df6 = merge_dff.groupby('Status').size().sort_values(ascending=False).reset_index()
    df6.rename(columns =  {0:'No_of_msges'},inplace = True)
    
    
   #----Provider and its frequency-----------
    df5 =  merge_dff.groupby('Provider').size().sort_values(ascending=False).reset_index()
    
    df5.rename(columns =  {0:'Frequency'},inplace = True)
    i = df5.set_index('Provider')['Frequency'].to_json()
    pro = ast.literal_eval(i)
    
    
    # now calculating CTR--------------click through rate-------
    ctr = (clicked_msg/num_delivered)*100
    d['CTR']= ctr
    
    #fetching the ip address column from the dataset anf find user's information------
    df8 = merge_dff[['IP Address','Clicks']]
    df9 = df8.dropna()
    ip_list = df9["IP Address"].tolist()
    top_100_ip = ip_list[1:101]
    
    def convert_ip_to_location(ip_address=[], params=[]):
        
        valid_params = ['status', 'message', 'continenet', 'continentCode', 'country',
                        'countryCode', 'region', 'regionName', 'city', 'district', 
                        'zip', 'lat', 'lon', 'timezone', 'offset', 'currency', 'isp',
                        'org', 'as', 'asname', 'reverse', 'mobile', 'proxy', 'hosting',
                        'query']

        assert isinstance(ip_address, list), 'The ip_address must be passed in a list'
        assert ip_address, 'You must pass at least one ip address to the function'
        assert isinstance(params, list), 'You must pass at least one parameter'
        for param in params:
            assert param in valid_params, f"{param} is not a valid parameter. List of valid params: {valid_params}"

        url = 'http://ip-api.com/json/'

        params = ['status', 'country', 'countryCode', 'city', 'timezone', 'mobile']
        params_string = ','.join(params)

        df10 = pd.DataFrame(columns=['ip_address'] + params)

        for ip in ip_address:
            resp = requests.get(url + ip, params={'fields': params_string})
            info = resp.json()
            if info["status"] == 'success':
                info = resp.json()
                info.update({'ip_address': ip})
                df10 = df10.append(info, ignore_index=True)
            else:
                logging.warning(f'Unsuccessful response for IP: {ip}')
        
        return df10
    
    
    df10 = convert_ip_to_location(
    ip_address = top_100_ip,
    params=['status', 'country', 'countryCode', 'city', 'timezone', 'mobile']
    )

    print(df10)
    
     #best performing location------------
     
    df11 =  df10.groupby('city').size().sort_values(ascending=False).reset_index()
    df11.rename(columns =  {0:'user_freq'},inplace = True)
    print(df11)
    l = df11.set_index('city')['user_freq'].to_json()
    loc = ast.literal_eval(l)
    
    
    return d,pro,sta,loc
    
d,pro,sta,loc = dataset()

list = []
list.append(d)
list.append(pro)
list.append(sta)
list.append(loc)



@app.route('/', methods=['GET'])
def home():
    return 'welcome to analytics part'

@app.route('/all', methods = ['GET'])
def all():
    return jsonify(list)
  
@app.route('/diff_rates', methods=['GET'])
def insights():
    return jsonify({"diffrent rates in APP1":d})

@app.route('/provider_freq',methods =['GET'])
def provider_freq():
    return jsonify({"provider in camp1 and their freq":pro})


@app.route('/status',methods =['GET'])
def status():
    return jsonify({"status of messages in APP1":sta})


@app.route('/location',methods =['GET'])
def location():
    return jsonify({"best performed location":loc})


if __name__ == '__main__':
    app.run(debug = False,host = '0.0.0.0')
  










