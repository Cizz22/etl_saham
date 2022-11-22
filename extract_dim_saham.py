import os, json, pandas as pd

from datetime import datetime
from utils import get_scrap_data, create_engine
from sqlalchemy import MetaData, Table, Column, Integer, String, DateTime, ForeignKey, Text, select, insert


pd.options.mode.chained_assignment = None

def getstocklistData():
    DaftarSaham = pd.DataFrame()
    
    URL = 'https://www.idx.co.id/primary/StockData/GetSecuritiesStock?start=0&length=9999&code=&sector=Energy&board=&language=id-id'
        
    data = get_scrap_data(URL)

    # Mengubah json ke dalam bentuk DataFrame
    df = pd.DataFrame(data['data'])

    # Mengubah format tanggal menjadi datetime
    df['ListingDate'] = [datetime.strptime(
        x[:10], '%Y-%m-%d') for x in df['ListingDate']]
    df['Sector'] = "Energy"

    DaftarSaham = pd.concat([DaftarSaham, df], ignore_index=True)

    # urut berdasarkan kode saham
    DaftarSaham = DaftarSaham.sort_values(by='Code').reset_index(drop=True)

    if os.path.exists('DataSaham\\DaftarSaham.csv'):
        olddata = pd.read_csv('DataSaham\\DaftarSaham.csv')
        tambah = pd.DataFrame(columns=DaftarSaham.columns)
        for i, data in enumerate(DaftarSaham['Code']):
            if data not in olddata['Code'].values:
                tambah = tambah.append(DaftarSaham.iloc[i])
        try:
            tambah['ListingDate'] = tambah['ListingDate'].dt.strftime(
                '%Y-%m-%d')
        except:
            pass
        olddata = olddata.append(tambah, ignore_index=True)
        olddata = olddata.sort_values(by='Code').reset_index(drop=True)
        return olddata
    else:
        return DaftarSaham

def createStockListTable():
    engine = create_engine(creds)
    metadata = MetaData(bind=engine)
    stocks = Table('saham', metadata,
                       Column('Code', Integer, primary_key=True),
                       Column('Name', Text,
                              nullable=False, unique=True),
                       Column('ListingDate', DateTime),
                       Column('Shares', Integer),
                       Column('ListingBoard', String(10)),
                       )
    stocks.drop(checkfirst=True)
    stocks.create()
    
def insertStockListData():
    pass    


getstocklistData().to_csv('DaftarSaham.csv', index=False)

