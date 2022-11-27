# ETL SAHAM IDX #
This is a simple ETL project to extract data stocks from Yahoo Finance & IDX then load it into a PostgreSQL database.

## Requirements ##
Simply install the requirements using pip:
```
pip install -r requirements.txt
```

## Usage ##
To run the ETL, simply run the following command:
```
python main.py
```

## Configuration ##
Database configuration is stored in `creds` file. You can change the database configuration there.


## Data Source ##
- [IDX](https://www.idx.co.id/)
- [Yahoo Finance](https://finance.yahoo.com)

## Result ##
The result of the ETL is stored in `Result` folder. The data is stored in CSV format.

## License ##
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details