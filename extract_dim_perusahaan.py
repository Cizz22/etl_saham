import os, json, pandas as pd

from datetime import datetime
from utils import get_scrap_data, create_engine
from sqlalchemy import MetaData, Table, Column, Integer, String, DateTime, ForeignKey, Text, select, insert


pd.options.mode.chained_assignment = None

def getCompanylistData():
    DaftarSaham = pd.read_csv('DaftarSaham.csv')
    
    # DaftarSaham = [code for code in DaftarSaham['Code']]
    
    DaftarPerusahaan = pd.DataFrame()
    
    
    URL = 'https://www.idx.co.id/primary/ListedCompany/GetCompanyProfiles?KodeEmiten&language=id-id&length=1000'
    data = get_scrap_data(URL)
           
        # Mengubah json ke dalam bentuk DataFrame
    df = pd.DataFrame(data['data'], columns=['BAE','KodeEmiten','EfekEmiten_EBA','EfekEmiten_ETF','EfekEmiten_Obligasi','EfekEmiten_Saham','EfekEmiten_SPEI','Industri','SubIndustri','Email','Fax','id','JenisEmiten','KegiatanUsahaUtama','KodeDivisi','NamaEmiten','NPKP','NPWP','PapanPencatatan','Sektor','SubSektor','TanggalPencatatan','Telepon','Website'])
    
    
    for i, data in enumerate(df['KodeEmiten']):
        if data in DaftarSaham['Code'].values:
            df.iloc[i:i+1].fillna('', inplace=True)
            DaftarPerusahaan = pd.concat([DaftarPerusahaan, df.iloc[i:i+1]])
    
    DaftarPerusahaan = DaftarPerusahaan.sort_values(by='KodeEmiten').reset_index(drop=True)

    return DaftarPerusahaan
