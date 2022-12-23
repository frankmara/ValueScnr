import psycopg2
import requests 

conn = psycopg2.connect(database="stocks-us", user="postgres", password="*********", host="*******", port="*****" )

def insertData(f_data):
    try:    
        sql = "INSERT INTO tickers(stock, stock_name, current_price, exchng) VALUES (%s,%s,%s,%s)"
        data = f_data
        cursor = conn.cursor()
        cursor.execute(sql, data)
        conn.commit()
        cursor.close()
    except Exception as e:
        print(e)

def main():
    try:
        url ="https://financialmodelingprep.com/api/v3/stock/list?apikey=1f78aadad1d509cf8c371c8d0ee5f88b"

        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"

        response = requests.get(url, headers={'User-Agent': ua})

        for data in response.json():
            try:
                if((data["exchange"] =="NASDAQ Global Select" or data["exchange"] =="New York Stock Exchange") and data["price"] > 10):
                    f_data = [data["symbol"], data["name"], data["price"], data["exchange"]]
                    insertData(f_data)
                    print(f_data)
            except Exception as e:
                continue
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()
    

    