import os, json, pandas as pd

from datetime import datetime
from utils import get_scrap_data, create_engine
from sqlalchemy import MetaData, Table, Column, Integer, String, DateTime, ForeignKey, Text, select, insert


pd.options.mode.chained_assignment = None

def getBrokerlistData():
    DaftarBroker = pd.DataFrame()
    
    URL = 'https://www.idx.co.id/primary/TradingSummary/GetBrokerSummary?length=9999'
        
    data = get_scrap_data(URL)

    # Mengubah json ke dalam bentuk DataFrame
    df = pd.DataFrame(data['data'], columns=['IDFirm', 'FirmName'])

    DaftarBroker = pd.concat([DaftarBroker, df])

    # urut berdasarkan kode saham
    DaftarBroker = DaftarBroker.sort_values(by='IDFirm').reset_index(drop=True)

    return DaftarBroker

def createStockListTable():
    pass
    
def insertStockListData():
    pass    


