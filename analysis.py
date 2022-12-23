import psycopg2
from statistics import mean 
from tabulate import tabulate

def main():
    conn = psycopg2.connect(database="stocks-us", user = "postgres", password = "********", host = "******", port = "***^")
    cur = conn.cursor()
    cur.execute("SELECT * FROM financials;")
    rows = cur.fetchall()

    result = []
    for row in rows:
        checkcriterias = True
        try:
            #check MV
            stock = row[1]
            marketvalue = float(row[5][0]["marketCapitalization"])
            if marketvalue < 5000000000:
                checkcriterias = False
            
            #Check Current Ratio
            currentratio = float(row[6][0]["currentRatioTTM"])
            if currentratio < 2:
                checkcriterias = False

            #Check PB Ratio and PE Ratio
            pbratio = float(row[6][0]["pbRatioTTM"])
            peratio = float(row[6][0]["peRatioTTM"])

            if (peratio * pbratio) > 23:
                checkcriterias = False
            
            #Total Debt < 2.1 X Net Current Assets
            totaldebt = float(row[3][0]["totalDebt"])
            currentassets = float(row[3][0]["totalCurrentAssets"])

            if totaldebt > (2.1 * currentassets):
                checkcriterias = False

                #EPS > 0 for last 10Y
                counter = 1
                for e in row[2]:
                    if(float(e["eps"]) < 0):
                        checkcriterias = False
                    counter = counter + 1
                    if counter ==  10:
                        break 
                
                #EPS last year > EPS Avg last 3Y > 1.5x EPS last 10Y
                currenteps = float(row[2][0]["eps"])
                
                avg3yr = mean([
                    float(row[2][2]["eps"]),
                    float(row[2][3]["eps"]),
                    float(row[2][4]["eps"])
                ])

                avg7_9yr = mean([
                    float(row[2][6]["eps"]),
                    float(row[2][7]["eps"]),
                    float(row[2][8]["eps"])
                ])

                if not (currenteps > avg3yr and avg3yr > avg7_9yr):
                    checkcriterias = False


                      
            
            cur.execute("SELECT current_price from tickers where stock = '"+stock+"';")
            current_price = float(cur.fetchone()[0])
            bookvalue =float(row[6][0]["bookValuePerShareTTM"])
        
            

            if (checkcriterias):
                result.append([stock, current_price, marketvalue, currentratio, bookvalue, currenteps, pbratio, peratio])
                
        except Exception as e:
            continue

    print(tabulate(result, headers=['stock', 'Price', 'Market Value', 'Current Ratio', 'Book Value', 'EPS', 'P/B', 'P/E']))
    
    
    


if __name__ == "__main__":
    main()
