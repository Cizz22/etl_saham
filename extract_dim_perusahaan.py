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
    df = pd.DataFrame(data['data'], columns=['BAE','DataID','Divisi','EfekEmiten_EBA','EfekEmiten_ETF','EfekEmiten_Obligasi','EfekEmiten_Saham','EfekEmiten_SPEI','Industri','SubIndustri','Email','Fax','id','JenisEmiten','KegiatanUsahaUtama','KodeDivisi','KodeEmiten','NamaEmiten','NPKP','NPWP','PapanPencatatan','Sektor','SubSektor','TanggalPencatatan','Telepon','Website','Status','Logo'])
    
    filter = df['KodeEmiten'].isin(DaftarSaham['Code'])
    
    df[filter]

    # # Mengubah format tanggal menjadi datetime
    # df['TanggalPencatatan'] = [datetime.strptime(
    # x[:10], '%Y-%m-%d') for x in df['TanggalPencatatan']]
        
    DaftarPerusahaan = pd.concat([DaftarPerusahaan, df])

    # urut berdasarkan kode DaftarPerusahaan
    # DaftarPerusahaan = DaftarPerusahaan.sort_values(by='id').reset_index(drop=True)

    return DaftarPerusahaan

getCompanylistData().to_csv('DaftarPerusahaan.csv', index=False)