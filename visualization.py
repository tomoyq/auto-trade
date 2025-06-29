import mplfinance as mpf
import pandas as pd

class Visualization:

    COLUMNS = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']

    def visualization_ByBit_kline(
            self,
            df: pd.DataFrame,
            alines: list[list[str, float]] | None = None,
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

        #alinesが渡されている場合はplotのオプションを使用する
        if alines != None:
            mpf.plot(
                df_copy,
                type='candle',
                alines=alines
            )
        else:
            mpf.plot(
                df_copy,
                type='candle',
            )

    def visualization_trend_line(
            self,
            target_kline_df: pd.DataFrame,
            analysis_df: pd.DataFrame,
        ):
        """
        可視化したいローソク足のデータとそのローソク足の中のデータからスイングハイ、
        スイングロウを抜き出したデータを渡すとローソク足とトレンドラインを同時に描画する関数
        """
        #(開始時刻の文字列, その時刻の高値もしくは安値のfloat型)の配列を作成
        points = []

        for i in range(analysis_df.index.size):
            target = analysis_df.iloc[i]

            if target['分類'] == 'スイングロウ':
                points.append((target['開始時刻'], float(target['安値'])))
            else:
                points.append((target['開始時刻'], float(target['高値'])))

        self.visualization_ByBit_kline(df=target_kline_df, alines=points)