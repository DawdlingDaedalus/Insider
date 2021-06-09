# Scrape www.insidearbitrage.com
# Create a dataframe from that data
# Update that dataframe with closing price, 180-day moving average, and sector
# Create a CSV file called 'InisderInfo.CSV' with all this info

import pandas as pd
import pandas_datareader as dr
import yfinance as yf
from datetime import datetime,date,timedelta
import ssl
ssl._create_default_https_context = ssl._create_unverified_context # necessary for pd.read_html

startTime = datetime.now()

# creates a function to scrape www.insidearbitrage.com for insider trading data
def getsales():
    startTime = datetime.now()
    NumPages = 3
    finaldf = pd.DataFrame() # initialize a dataframe to be updated with the 6 pages
    transactiontypes = ['buying','sales']
    pagesscraped = 0
    for t in transactiontypes:
        for i in range(NumPages): # 6 pages will be scraped. 3 selling, 3 buying
            url = f"https://www.insidearbitrage.com/insider-{t}/?desk=yes&pagenum={i+1}"
            df = pd.read_html(url) #fetches the web content, returns a list of html dataframes                                
            df = df[2] # grabs the table object
            columns = df.iloc[0] # grabs the column headers
            df.columns = columns # updates the column headers
            df.drop(df.columns[0],axis=1,inplace=True) # drop the first column
            df = df[1:] # drop the first row
            if t == 'buying': # add a column for transaction type
                df['Type'] = "buy"
            else:
                df['Type'] = "sell"
            frames = [finaldf,df]
            finaldf = pd.concat(frames) # update the dataframe with recently-scraped data
            pagesscraped+=1
            print(f'{pagesscraped} Pages Scraped : Total Elapsed time = {datetime.now() - startTime}')

    finaldf.to_csv('Insider.csv') # export finaldf to our .csv file
    print(f'CSV `Insider.csv` File Created - Execution Time: {datetime.now() - startTime}')

# call the function to create Insider.csv
getsales()
df = pd.read_csv('Insider.csv',index_col = 0) # create df for updating with price, moving average, sector

# initialize a dictionary to be updated with ticker price and moving average keyed on ticker
info_dict = {}
numperiods = 180

# get most recent closing price and moving average for tickers                                                        
def getinfo(ticker,n):
    try:
        # create a dataframe using yahoo data for the ticker from 300 days ago to today
        tickerdf = dr.data.get_data_yahoo(ticker,start = date.today() - timedelta(300) , end = date.today())
        currentprice = tickerdf.iloc[-1]['Close']
        # create moving average calculation for n (180) days
        MA = pd.Series(tickerdf['Close'].rolling(n, min_periods=0).mean(), name='MA')
        currentma = MA[-1]
        print(f"data gathered for {ticker}")
        # return current data, price and moving average
        return (currentprice,currentma)
    except:
        return ('na','na')

# get the price of the ticker                                        
def getPrice(row):
    ticker = row['Symbol']
    if ticker not in info_dict.keys():
        tickerinfo = getinfo(ticker,numperiods)
        info_dict[ticker] = {}
        info_dict[ticker]["price"] = tickerinfo[0]
        info_dict[ticker]["ma"] = tickerinfo[1]
        return info_dict[ticker]["price"]
    else:
        return info_dict[ticker]["price"]

# get the moving average of the ticker                                  
def getMovingAverage(row):
    ticker = row['Symbol']
    return info_dict[ticker]["ma"]

# get the sector for each ticker, store in a dictionary
sector_dict = {}
unique_tickers = df['Symbol'].unique()

for ticker in unique_tickers:
    try:
        sector_dict[str(ticker)] = yf.Ticker(str(ticker)).info['sector']                                   
        print('updated: ', ticker)
    except:
        print('passed: ', ticker)
        pass

# define a function to return the proper sector (from sector_dict) for each row
def getSector(row):
    ticker = str(row['Symbol'])
    try:
        sector = sector_dict[ticker]
        return sector
    except:
        return 'na'



# update df with current price, moving average, and industry sector
df['Current Price'] = df.apply (lambda row: getPrice(row), axis=1)
print("Prices gathered")
df['Moving Average'] = df.apply (lambda row: getMovingAverage(row), axis=1) 
print('Moving averages gathered')
df['Sector'] = df.apply(lambda row: getSector(row), axis=1)
print('Sectors gathered')

# export as csv
df.to_csv('InsiderPrices.csv')
print(f'CSV `InsiderPrices.csv` File Created - Execution Time: {datetime.now() - startTime}')

print(f'Total Execution Time: {datetime.now() - startTime}')                                           
