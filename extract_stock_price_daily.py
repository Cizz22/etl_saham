from utils import get_scrap_data, create_engine, getdate, getcookie, cleandata,appenddata,addextra, getcookie
import tqdm, requests, json, pandas as pd
from datetime import datetime

def getStockPriceDaily():
    DaftarSaham = pd.read_csv('DaftarSaham.csv')
    awal, akhir = getdate()
    header, cookies, crumb, tradingperiod = getcookie()
    getstockdata(DaftarSaham, header, cookies, crumb, awal, akhir)
    # cleandata(awal, akhir, tradingperiod)
    # appenddata(awal, akhir)
    # addextra()
    

def getstockdata(DaftarSaham, header: dict, cookies, crumb: str, awal: str, akhir: str):
    error = []
    
    # Melakukan looping untuk mengambil semua data saham
    for kode_emiten in DaftarSaham['Code']:
        try:
            # print('scraping', kode_emiten)
            
            # Membuka website dengan crumb dan cookies yang sudah didapatkan sebelumnya
            website = requests.get('https://query1.finance.yahoo.com/v8/finance/chart/'+kode_emiten+'.JK?&range=5d&useYfid=true',                                   '&interval=1m&includePrePost=true&events=div|split|earn&lang=en-US&region=US&crumb='+crumb +
                                   '&corsDomain=finance.yahoo.com', headers=header, cookies=cookies).text
            
            # Mengubah text yang didapatkan dari request ke dalam bentuk json
            site_json = json.loads(website)
            
            # Menentukan lokasi data saham (open, low, high, close, volume)
            stockdata = site_json['chart']['result'][0]['indicators']['quote'][0]
            
            # Melakukan transformasi data dari json ke DataFrame
            stockdf = pd.DataFrame(list(zip([datetime.fromtimestamp(x).strftime("%x %X") for x in site_json['chart']['result'][0]['timestamp']],
                                            stockdata['open'], stockdata['low'], stockdata['high'], stockdata['close'],
                                            stockdata['volume'])), columns=['timestamp', 'open', 'low', 'high', 'close', 'volume'])
            
            # Mengisi data yang kosong/NaN dengan 0
            stockdf.fillna(0, inplace=True)
            
            # Mengubah tipe data kolom timestamp menjadi datetime
            stockdf['timestamp'] = pd.to_datetime(stockdf['timestamp'])
            
            # Mengubah tipe data kolom open, low, high, close, dan volume menjadi integer
            stockdf[['open', 'low', 'high', 'close', 'volume']] = stockdf[[
                'open', 'low', 'high', 'close', 'volume']].astype('int64')
            
            # Menyimpan data saham ke dalam bentuk csv
            stockdf.to_csv(f"{awal}-{akhir}-{kode_emiten}.csv", index=False)
        except:
            error.append(kode_emiten)
            continue
    print('Daftar scraping saham yang error: \n', error)
    