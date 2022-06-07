# -*- coding: utf-8 -*-
"""
Created on Wed Oct  2 12:33:40 2019

@author: vk24
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Oct  1 20:49:22 2019

@author: vk24
"""

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
driver.set_page_load_timeout(4.5)



def find_eta(eta_val):
    now = datetime.datetime.now()
    date = now.strftime("%Y-%m-%d")
    
    eta_val = date+" "+eta_val
    eta = datetime.datetime.strptime(eta_val, "%Y-%m-%d %I:%M %p")
    
    if (eta-now).days <0:
        eta = eta+datetime.timedelta(hours=24)
    
    return (int((eta-now).total_seconds()/60))
    
def download(link):
    
    down_start_time = time.time()
    try:
        driver.get(link)
    except:
        print("ERROR: Did not get Link")
        pass
  
    temp = []
 
    print("###### Download Time Gone")
    print("--- %s seconds ---" % (time.time() - down_start_time))
    
    try:
        sleeping_time=max((8-(time.time() - down_start_time)),0.1)
        print("Sleeping time="+str(sleeping_time))
        
        time.sleep(sleeping_time)
        for i in range(14):      
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
    interval = 200/15
    
        
    for i in range(0,2000000):
        if (start_time + i*interval - time.time()) >= 0:
            time.sleep(start_time + i*interval - time.time())
    
            print("############# Loop number:"+str(i))
            now = datetime.datetime.now()
            print(now.strftime("%Y-%m-%d,%H:%M:%S"))

            print("######")
            print("--- %s seconds ---" % (time.time() - start_time))
            
            link_data= url_df.iloc[i%len(url_df)]
            print(link_data['destination']) 
            
            link=link_data['trip_link']
            data=download(link)

            print("######")
            print("--- %s seconds ---" % (time.time() - start_time))

            for j in range(len(data)):
                #print(data[j])
                temp=data[j].split()
                
                #If Car is available (Eg. In 5 mins)
                if "In" in temp:
                    
                    try:
                        ind = temp.index("In")
                        
                        wait = int(temp[2+ind-1])
                        #print(wait)
                        eta_val = str(temp[4+ind-1])+" "+str(temp[5+ind-1])
                        #print(eta_val)
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
                        rectup = (now, i, data[j].replace('\n',' '), link_data['state'], link_data['city'], link_data['origin_type'], link_data['destination_type'], link_data['origin'],link_data['destination'], temp[0],wait,find_eta(eta_val),price,"OKAY")
                        c.execute("""INSERT INTO uber_price VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",rectup)
                        #print("DB OK")
                        conn.commit()                          
                    except:
                        rectup = (now, i, None, None,None,None,None,None,None,None,None,None,None,"ERROR DB not working")
                        c.execute("""INSERT INTO uber_price VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",rectup)
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
                        df = df.append({'timestamp': now,'loop': i}, ignore_index=True)        
                        pass
                    
                    #Adding to Database
                    try:
                        rectup = (now, i,data[j].replace('\n',' '), link_data['state'], link_data['city'], link_data['origin_type'], link_data['destination_type'], link_data['origin'],link_data['destination'], temp[0],None,None,None,"OKAY NOTAVAILABLE")
                        c.execute("""INSERT INTO uber_price VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",rectup)
                        conn.commit() 
                        print("DB OK")
                    except:
                        rectup = (now, i, data[j].replace('\n',' '), None,None,None,None,None,None,None,None,None,None,"ERROR EMPTY")
                        c.execute("""INSERT INTO uber_price VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",rectup)
                        conn.commit()      
                        print("ERROR DB2 not working")
                        pass
                    
        else:
            print("hahaha BIG ERROR ############# Loop number:"+str(i))
            rectup = (datetime.datetime.now(), i, str((start_time + i*interval - time.time())), None,None,None,None,None,None,None,None,None,None,"ERROR NO DATA LOOP JUMP")
            c.execute("""INSERT INTO uber_price VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",rectup)
            conn.commit()
            try:
                #rectup = (now, i, None, None,None,None,None,None,None,None,None,None,None,"ERROR NO DATA LOOP JUMP")

                #df = df.append({'timestamp': datetime.datetime.now()}, ignore_index=True) 
                print("ERROR NO DATA LOOP JUMP")

            except:
                print("ERROR ERROR ERROR")
                pass 
            
           
        if i%25000 == 0:
            try:
                # Convert the dataframe to an XlsxWriter Excel object.
                filename="dataframe/uberdata_"+str(now.strftime("%Y_%m_%d_%H_%M_%S"))+".xlsx"
                writer = pd.ExcelWriter(filename, engine='xlsxwriter')
                df.to_excel(writer, sheet_name='Sheet1')
                writer.save()
                df = pd.read_excel('original_data1.xlsx')
            except:
                pass
        print("###### Loop Ends")
        print("--- %s seconds ---" % (time.time() - start_time))
        
       

print("######")
print("--- %s seconds ---" % (time.time() - start_time))

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()    
