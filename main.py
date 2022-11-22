from utils import getdate

if __name__ == "__main__":
    awal, akhir = getdate()
    DaftarSaham = getstocklist()
    DaftarSaham.to_csv('IHSGstockdata\\DaftarSaham.csv',index=False)
    header, cookies, crumb, tradingperiod = getcookiecrumb()
    getstockdata(DaftarSaham, header, cookies, crumb, awal, akhir)
    cleandata(awal, akhir, tradingperiod)
    appenddata(awal, akhir)
    addextra()