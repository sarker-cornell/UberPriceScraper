# -*- coding: utf-8 -*-
"""
Created on Tue Sep 24 22:34:23 2019

@author: ssarker
"""


from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import urllib.request
import random
import os
import xlsxwriter
import numpy as np
import pandas as pd
import time
import datetime
import sqlite3
conn = sqlite3.connect('uber_90.db')

c = conn.cursor()
now = datetime.datetime.now()


username = os.getenv("USERNAME")
userProfile = "C:\\Users\\" + username + "\\AppData\\Local\\Google\\Chrome\\User Data\\Default"
options = webdriver.ChromeOptions()
options.add_argument("user-data-dir={}".format(userProfile))
# add here any tag you want.
options.add_experimental_option("excludeSwitches", ["ignore-certificate-errors", "safebrowsing-disable-download-protection", "safebrowsing-disable-auto-update", "disable-client-side-phishing-detection"])
# LINK TO Chromediver goes here (Need to change)
chromedriver = "C:/Users/Home Admin/Desktop/uber_local_scrape/chromedriver.exe"
os.environ["webdriver.chrome.driver"] = chromedriver
driver = webdriver.Chrome(executable_path=chromedriver, chrome_options=options)

def find_eta(eta_val):
    now = datetime.datetime.now()
    date = now.strftime("%Y-%m-%d")
    
    eta_val = date+" "+eta_val
    eta = datetime.datetime.strptime(eta_val, "%Y-%m-%d %I:%M %p")
    
    if (eta-now).days <0:
        eta = eta+datetime.timedelta(hours=24)
    
    return (int((eta-now).total_seconds()/60))
    
def download(link):    
    try:
        driver.get(link)
    except:
        print("ERROR: Did not get Link")
        pass
  
    temp = []
 
    #print("########### Fetching Elements from Uber Webpage")

    try:
        time.sleep(6.2)
        for i in range(23):      
            for elem in driver.find_elements_by_xpath("//*[@id='booking-experience-container']/div/div[3]/div[2]/div/div[2]/div["+str(i)+"]"):
                #print(elem.text.split())
                try:
                    temp.append(elem.text)
                except:
                    pass
        print("Found Elements")            
    except:
        print("ERROR: Element Missing")
        pass
            
    #print(temp)
    return temp
                  


if __name__ == "__main__":
    
    df = pd.read_excel('original_data1.xlsx')
    url_df = pd.read_excel('uber_link_90.xlsx')

    start_time = time.time()
    interval = 10
    
        
    for i in range(0,4152):
        if (start_time + i*interval - time.time()) > 0:
            time.sleep(start_time + i*interval - time.time())
    
            print("############# Loop number:"+str(i))
            now = datetime.datetime.now()
            print(now.strftime("%Y-%m-%d,%H:%M:%S"))
            
            link_data= url_df.iloc[i%len(url_df)-1]
            print(link_data['destination']) 
            
            link=link_data['trip_link']
            data=download(link)
    
            for j in range(len(data)):
                #print(data[j])
                temp=data[j].split()
                
                #If Car is available (Eg. In 5 mins)
                if "In" in temp:
                    ind = temp.index("In")
                    
                    wait = int(temp[2+ind-1])
                    #print(wait)
                    eta_val = str(temp[4+ind-1])+" "+str(temp[5+ind-1])
                    #print(eta_val)
                    
                    try:
                        price = float(str(temp[-1])[1:])
                        #print(price)
                    except:
                        price = str(temp[-1])
                        pass
                    
                    #Adding to dataframe
                    try:
                        df = df.append({'timestamp': now,
                                        'loop': i, 
                                        'state': link_data['state'], 
                                        'city': link_data['city'],
                                        'origin': link_data['origin'], 
                                        'destination': link_data['destination'],
                                        'origin_type': link_data['origin_type'], 
                                        'destination_type': link_data['destination_type'],
                                        
                                        'car_type': temp[0], 
                                        'wait_time': wait,
                                        'eta_val': eta_val,
                                        'eta': find_eta(eta_val), 
                                        'price_usd': price,
                                        
                                        'unstructured_data': data[j].replace('\n',' ')}, ignore_index=True)
                    except:
                        print("ERROR df not working")
                        pass
                    
                    #Adding to Database
                    try:
                        rectup = (now, i, link_data['state'], link_data['city'], link_data['origin_type'], link_data['destination_type'], link_data['origin'],link_data['destination'], temp[0],wait,find_eta(eta_val),price,data[j].replace('\n',' '))
                        c.execute("""INSERT INTO uber_price VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",rectup)
                        print("DB OK")
                        conn.commit()                          
                    except:
                        rectup = (now, i, None,None,None,None,None,None,None,None,None,None,"ERROR DB not working")
                        c.execute("""INSERT INTO uber_price VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",rectup)
                        conn.commit() 
                        print("ERROR DB not working")
                        pass

                # If Car Not Available
                else:
                    try:
                        df = df.append({'timestamp': now,
                                        'loop': i,
                                        'state': link_data['state'], 
                                        'city': link_data['city'],
                                        'origin': link_data['origin'], 
                                        'destination': link_data['destination'],
                                        'origin_type': link_data['origin_type'], 
                                        'destination_type': link_data['destination_type'],
                                        
                                        'car_type': temp[0], 
                                        
                                        'unstructured_data': data[j].replace('\n',' ')}, ignore_index=True)            
                    except:
                        print("ERROR df2 not working")
                        df = df.append({'timestamp': now}, ignore_index=True)        
                        pass
                    
                    #Adding to Database
                    try:
                        rectup = (now, i,link_data['state'], link_data['city'], link_data['origin_type'], link_data['destination_type'], link_data['origin'],link_data['destination'], temp[0],None,None,None,data[j].replace('\n',' '))
                        c.execute("""INSERT INTO uber_price VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",rectup)
                        conn.commit() 
                        print("DB OK2")
                    except:
                        rectup = (now, i, None,None,None,None,None,None,None,None,None,None,"ERROR DB2 not working")
                        c.execute("""INSERT INTO uber_price VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",rectup)
                        conn.commit()      
                        print("ERROR DB2 not working")
 
                        pass
                    
        else:
            try:
                rectup = (datetime.datetime.now(), i, None,None,None,None,None,None,None,None,None,None,None)
                c.execute("""INSERT INTO uber_price VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",rectup)
                conn.commit()
                df = df.append({'timestamp': datetime.datetime.now(),'loop': i}, ignore_index=True) 
            except:
                pass 
            
                    
        if i%4150 == 0:
            try:
                # Convert the dataframe to an XlsxWriter Excel object.
                filename="dataframe/uberdata_"+str(now.strftime("%Y_%m_%d_%H_%M_%S"))+".xlsx"
                writer = pd.ExcelWriter(filename, engine='xlsxwriter')
                df.to_excel(writer, sheet_name='Sheet1')
                writer.save()
                df = pd.read_excel('original_data1.xlsx')
            except:
                pass

        
       

print("######")
print("--- %s seconds ---" % (time.time() - start_time))

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()    
