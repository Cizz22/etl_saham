#Dimesi
from extract_dim_perusahaan import createCompanyList, insertCompanyList
from extract_dim_saham import createStockListTable, insertStockListData
from extract_dim_sekuritas import createBrokerListTable, insertBrokerListData


#fact
from extract_stock_price_daily import createStockPriceDailyTable, insertStockPriceDaily
from extract_broker_summary import createBrokerSummary, insertBrokerSummary
from transform_fluktuasi import createFluktuasiTable, insertFluktuasiData

from utils import engine as pg
from creds import creds

def main():
    #drop all table
    print('Drop all table')
    pg(creds).connect().execute('DROP SCHEMA public CASCADE; CREATE SCHEMA public;')
    print('Extracting dimensi saham')
    createStockListTable()
    insertStockListData()
    print('done')
    print('Extracting dimensi perusahaan')
    createCompanyList()
    insertCompanyList()
    print('done')
    print('Extracting dimensi sekuritas')
    createBrokerListTable()
    insertBrokerListData()
    print('done')
    print('Extracting fact harga saham harian')
    createStockPriceDailyTable()
    insertStockPriceDaily()
    print('done')
    print('Extracting fact broker summary')
    createBrokerSummary()
    insertBrokerSummary()
    print('done')
    print('Transforming fluktuasi')
    createFluktuasiTable()
    insertFluktuasiData()
    
main()
     

