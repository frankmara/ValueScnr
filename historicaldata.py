import requests
import psycopg2
import json
import time
import datetime



conn = psycopg2.connect(database="stocks-us", user = "postgres", password = "**********", host = "******", port = "****")

def insertData(f_data):
        sql = "INSERT INTO financials (stock, income_statement, balance_sheet, cash_flow, enterprise_value, key_metrics, financial_growth) VALUES (%s,%s,%s,%s,%s,%s,%s)" 
        data = f_data
        cursor = conn.cursor()
        cursor.execute(sql,data)
        conn.commit()
        cursor.close()

def main():
    ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'

    cur = conn.cursor()
    cur.execute("select stock from tickers;")
    rows = cur.fetchall()

    for row in rows:
        try:
            table = row[0]
            url = "https://financialmodelingprep.com/api/v3/income-statement/"+table+"?limit=120&apikey=1f78aadad1d509cf8c371c8d0ee5f88b"     
            response = requests.get(url, headers={'User-Agent': ua})
            income_statement = json.dumps(response.json())

            url = "https://financialmodelingprep.com/api/v3/balance-sheet-statement/"+table+"?limit=120&apikey=1f78aadad1d509cf8c371c8d0ee5f88b"  
            response = requests.get(url, headers={'User-Agent': ua})
            balance_sheet = json.dumps(response.json())

            url = "https://financialmodelingprep.com/api/v3/cash-flow-statement/"+table+"?limit=120&apikey=1f78aadad1d509cf8c371c8d0ee5f88b"
            response = requests.get(url, headers={'User-Agent': ua})
            cash_flow = json.dumps(response.json())

            url = "https://financialmodelingprep.com/api/v3/enterprise-values/"+table+"?limit=40&apikey=1f78aadad1d509cf8c371c8d0ee5f88b"
            response = requests.get(url, headers={'User-Agent': ua})
            enterprise_value = json.dumps(response.json())

            url = "https://financialmodelingprep.com/api/v3/key-metrics-ttm/"+table+"?limit=40&apikey=1f78aadad1d509cf8c371c8d0ee5f88b"     
            response = requests.get(url, headers={'User-Agent': ua})
            key_metrics = json.dumps(response.json())

            url = "https://financialmodelingprep.com/api/v3/income-statement-growth"+table+"?limit=40&apikey=1f78aadad1d509cf8c371c8d0ee5f88b"
            response = requests.get(url, headers={'User-Agent': ua})
            financial_growth = json.dumps(response.json())

            f_data = [table, income_statement, balance_sheet, cash_flow, enterprise_value, key_metrics, financial_growth] 

            insertData(f_data)
            print("Printed: ", table)
            time.sleep(1)

        except Exception as e:
            print(e)
            continue

if __name__ == "__main__":
    main()
