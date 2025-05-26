import mplfinance as mpf
import pandas as pd

class Visualization:

    COLUMNS = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']

    def visualization_ByBit_kline(
            self,
            df: pd.DataFrame
            ):
        """
        可視化したいdataframe(カラムを[期間,始値,高値,安値,終値,取引量]の順番)で渡すと、
        ローソク足で描画する関数
        """

        #ローソク足の可視化のためにdataframeの前処理
        df_copy = df.copy()
        df_copy = df_copy.drop('取引総額', axis=1)
        df_copy.columns = self.COLUMNS
        df_copy['Date'] = pd.to_datetime(df_copy['Date'])
        df_copy.set_index('Date', inplace=True)

        mpf.plot(
            df_copy,
            type='candle',   
        )