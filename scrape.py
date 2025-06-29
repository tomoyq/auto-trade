import datetime
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
            backtest: bool = False,       
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
        #バックテストの時とファイルがない場合はNone
        if not backtest and os.path.isfile(self.PATH):
            self.df = pd.read_csv(
                self.PATH,
                header=0,
                index_col=0,
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
        if self.df.size > 0:
            concat_df = pd.concat([self.df, df], axis=0)
            #indexが重複しているものは初めのデータを使用する
            df = concat_df[~concat_df.duplicated(keep='first', subset='開始時刻')]

        #dataframeのサイズが200以上あるときは新しいデータから200個分残して削除する
        df = df.drop(df.iloc[: -200].index).reset_index(drop=True)

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

        #dataframeに変換して時間をdatetime型に変更
        #開始時刻を昇順に並べ替え
        df = pd.DataFrame(
                data,
                columns=self.COLUMNS
            ).sort_values(by='開始時刻').reset_index(drop=True)
        
        df['開始時刻'] = [
            #ミリ秒を秒数に変換してからdatetime型
            datetime.datetime.fromtimestamp(float(timestamp) / 1000) for timestamp in df['開始時刻']]
        
        #開始時刻以外のカラムをfloat型に変換
        df[df.columns[df.columns != '開始時刻']] = df[df.columns[df.columns != '開始時刻']].astype(float)
        
        return df