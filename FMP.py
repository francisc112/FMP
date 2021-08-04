import pandas as pd
import numpy as np
import requests
from functools import reduce





class FMP:

    def __init__(self, key, ticker='None'):
        """ Initialize FMP Object
        
        Parameters:
        ----------
            key str:
                The API Key from Alpha Vantage (unique to each user)
            
            ticker str, optional:
                When initializing an FMP, a ticker must be related to that object.

        Returns:
        --------
            None: initialize FMP object

        """
        self._key = key
        self._ticker = ticker

        return self._ticker

    def _get_df(self, url):
        """ Return json from request in Pandas DataFrame

        Parameters:
        -----------
            url str: 
                The url from where to retrieve the json from


        Returns:
        --------
            pd.DataFrame:
                DataFrame containing all the information from the json request

        """

        response = requests.get(url)

        if response.status_code == 200: # Check if result was successfull

            response_df = pd.DataFrame.from_dict(response.json())

            return response_df

        else:
            print('Response for API is', response.json())
            raise ConnectionError('Couldnt connect to FMP api')

    def _get_historical_fmp(self, url):
        """ 
        Return historical data into pd.DataFrame format 

        Parameters:
        -----------
            url str:
                The url from where the http must be requested

        Returns:
        --------
            pd.DataFrame:
                The dataframe of historical data, sorted from oldest to newest date and indexed by date
        
        """

        response = requests.get(url)

        if response.status_code == 200:

            if response.json() == {}:
                print(f'{self._ticker} is empty when retrieving data')
                return None

            symbol = response.json()['symbol']

            df = pd.DataFrame.from_dict(response.json()['historical'])

            df.insert(0, 'symbol', symbol)

            df.sort_values(by='date', ascending=True, inplace=True)

            df.set_index('date', inplace=True)

            return df

        else:
            print('Response for API is', response.json())
            raise ConnectionError('Couldnt connect to FMP api')

    def get_multiple_returns(self, tickers:[str],period:str='M',compare_with_index=None):
        """
        Get in dataframe format multiple returns from the specified tickers

        Parameters:
        ---------
            tickers [str]:
                A list of the specified tickers
            period str:
                The number of periods to reduce the returns.
                Options are: [D,W,M,Q,Y]

            compare_with_index str:
                The ticker of the index to compare
                If the user specifies to compare with an index, all of the returns will be divided to that index. 
                Example:
                Normal Returns
                    [AAPL] [S&P]
                    [0.25] [0.10]

                With compare_with_index != None:
                    [AAPL] [S&P]
                    [2.5]  [1]

        Returns:
        --------
            Pandas DataFrame:
                A dataframe containing all of the specified intervals
        """

        dfs = []

        symbols = tickers

        if compare_with_index != None:
            symbols.append(compare_with_index)

        for ticker in symbols:
            df = self.historical_price_by_interval(ticker=ticker,interval='1d')
            df = df.reset_index()
            df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True)
            df['date'] = df['date'].dt.to_period(period)
            df.drop_duplicates(subset=['date'], keep='first', inplace=True)
            df['pct'] = df['adjClose'].pct_change()
            df.set_index('date', inplace=True)
            df.rename(columns={'pct': ticker}, inplace=True)
            df.dropna(inplace=True)
            dfs.append(df[[ticker]])

        df_merged = reduce(lambda left, right: pd.merge(left, right, on=['date'],
                                                        how='outer'), dfs)
        return df_merged

    def historical_price_by_interval(self,ticker=None,interval:str='1d'):
        """
        Parameters:
        -----------
        ticker str:
            The stock ticker
        
        interval str:
            The time period to return the prices of

        
        Possible Intervals: 
            4hour: 4 Hour Prices 
            1hour: 1 Hour Prices
            30min: 30 Min Prices 
            15min: 15 Min Prices
            5min: 5 Min Prices
            1min: 1 Min Prices
            1d: Daily Prices
            1w: Weekly Prices
            1m: Monthly Prices
            1q: Quarterly Prices
            1y: Yearly Prices

        Returns OHLCV Price data, along with change and change percentage of the ticker
        :return: DataFrame
        """

        url = None

        ticker = ticker if ticker != None else self._ticker # Check if user is specifying a ticker

        if interval in ['4hour','1hour','30min','15min','5min','1min']:

            url = f'https://financialmodelingprep.com/api/v3/historical-chart/{interval}/{ticker}?apikey={self._key}'

            return self._get_df(url)


        elif interval == '1d':
            url = f'https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?apikey={self._key}'

            historical_df = self._get_historical_fmp(url)
            historical_df['pct change'] = historical_df['adjClose'].pct_change()
            return historical_df

        # From now on, the user must use weekly to yearly prices, get daily prices df

        url = f'https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?apikey={self._key}'

        historical_df = self._get_historical_fmp(url)
        historical_df['pct change'] = historical_df['adjClose'].pct_change()

        historical_df['daily'] = pd.to_datetime(historical_df.index,infer_datetime_format=True)

        if interval == '1w':
            historical_df['week'] = historical_df['daily'].dt.to_period('w').apply(lambda r: r.start_time)
            return historical_df.drop_duplicates(subset=['week'],keep='first')
            
        elif interval == '1m':
            historical_df['monthly'] = historical_df['daily'].astype('datetime64[M]')
            return historical_df.drop_duplicates(subset=['monthly'],keep='first')

        elif interval == '1q':
            historical_df['quarter'] = historical_df['daily'].dt.to_period('q')
            return historical_df.drop_duplicates(subset=['quarter'],keep='first')

        elif interval == '1y':
            historical_df['year'] = historical_df['daily'].dt.year
            return historical_df.drop_duplicates(subset=['year'],keep='first')
        else:
            raise ValueError('unsupported interval for ',interval,' check your spelling')

