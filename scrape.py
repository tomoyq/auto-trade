import os
import pandas as pd
from pathlib import Path
import requests

class ScrapeMarketData():
    #apiのエンドポイント
    ENDPOINT = 'https://api.bybit.com/v5/market'
    COLUMNS = [
            '開始時刻',
            '始値',
            '高値',
            '安値',
            '終値',
            '取引量',
            '取引総額'
        ]

    def __init__(
            self,
            category: str = 'linear',
            symbol: str = 'BTCUSDT',
            interval: str = '1',         
        ) -> None:
        super().__init__()

        self.category = category
        self.symbol = symbol
        self.interval = interval

        #csvを保存するpath
        self.PATH = Path(
            'data',
            self.symbol + '-' + self.category,
            self.interval + 'MinutesKlines.csv'
        )

        #pathのcsvファイルがすでに存在する場合はcsvをDataFrameに変換
        #ない場合はNone
        if os.path.isfile(self.PATH):
            self.df = pd.read_csv(
                self.PATH,
                header=0,
                index_col=0
            )
        else:
            self.df = None

    def save_csv(
            self,
            df: pd.DataFrame,
    ) -> None:
        """
        インスタンス変数のdfがあれば結合し、ない場合はそのままで、
        pathの位置にcsvにして保存する関数
        """
        #self.dfがある場合は結合させる
        #dfのサイズが0より大きい場合はすでにcsvに保存されているデータがある
        if self.df.size < 0:
            self.df = pd.concat([self.df, df], axis=0)
            #indexが重複しているものは初めのデータを使用する
            self.df = self.df[~self.df.index.duplicated(keep='first')]

        #上の階層のディレクトリがない場合は作成する
        self.PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.PATH)


    def get_kline(
            self,
            start: int,
            end: int
        ) -> pd.DataFrame:
        """
        取得したい期間をtimestampで渡すとその期間のローソク足データを
        DataFrameにして返す関数
        """
        url = self.ENDPOINT + '/kline'
        params = {
            'category': self.category,
            'symbol': self.symbol,
            'interval': self.interval,
            'start': int(start),
            'end': int(end)
        }

        r = requests.get(url=url, params=params)
        #responseの中のローソク足のデータ
        data = r.json()['result']['list']

        #dataframeに変換して時間をindexに変更
        df = pd.DataFrame(
                data,
                columns=self.COLUMNS
            )
        df = df.set_index('開始時刻')
        
        return df