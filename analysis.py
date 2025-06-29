from datetime import datetime
import numpy as np
import os
import pandas as pd
from pathlib import Path
import re

#ダウ理論
class Dow():

    ANALYSIS_TREND_COLUMNS = ['開始時刻', '分類', '高値', '安値', 'トレンド', '転換値', '直近目標値']
    #検証する左右の期間数
    PERIOD = 5

    def __init__(
            self,
            #トレンド予測したいローソク足のデータ
            PATH: Path,
            backtest: bool = False,
        ) -> None:
        super().__init__()

        #分析するローソク足のデータのDataframe
        self.target_klines_df = pd.read_csv(
                                    PATH,
                                    header=0,
                                    index_col=0,
                                )

        #csvを保存するpath
        #analysis/(分析する通貨名)/(ローソク足の時間).csvに保存する
        target = re.findall('data/(.*?)/(.*?).csv', str(PATH))
        self.PATH = Path(
            'analysis',
            target[0][0],
            f'{target[0][1]}.csv'
        )

        #pathのcsvファイルがすでに存在する場合はcsvをDataFrameに変換
        if not backtest and os.path.isfile(self.PATH):
            self.df = pd.read_csv(
                self.PATH,
                header=0,
                index_col=0,
            )

            #現在の相場を分析結果のデータフレームから取得
            self.trend = self.df.iloc[-1]['トレンド']

        #過去に分析していない通貨のローソク足のデータがPATHに入っているため、
        #渡されたデータをもとにスイングハイ、スイングロウを定義する
        else:
            self.df = self.analysis_dow()
            
            self.trend = np.nan
            counter = len(self.df)

            for count in range(counter):
                #現在のデータよりも前に最低３つデータが必要のため
                #ない場合はスキップする
                if count <= 2:
                    pass

                else:
                    target_df = self.df.iloc[count].copy()
                    target_data = float(target_df['高値']) if target_df['分類'] == 'スイングハイ' else float(target_df['安値'])
                    latest_data = self.df.iloc[count - 1]
                    
                    conversion_value, target_value = self.environmental_awareness(
                                                        target_data=target_data,
                                                        latest_data=latest_data,
                                                        count=count
                                                    )
                    
                    self.df.iloc[count, self.ANALYSIS_TREND_COLUMNS.index('トレンド')] = self.trend
                    self.df.iloc[count, self.ANALYSIS_TREND_COLUMNS.index('転換値')] = conversion_value
                    self.df.iloc[count, self.ANALYSIS_TREND_COLUMNS.index('直近目標値')] = target_value

            self.save_csv(df=self.df)

    def analysis_dow(
            self,
            #見比べる左右のデータ数
            period: int = PERIOD,
            ) -> pd.DataFrame:
        """
        分析するローソク足のDataframeから一つずつ値を取り出して、
        取り出した期間の前後のデータと比べてスイングハイ、スイングロウを定義して、
        分析結果をDataframeにして返す関数
        """
        total_data_count = self.target_klines_df.index.size

        #空の多次元配列を用意してスイングハイ、もしくはスイングロウと断定できたデータを追加していく
        analysis_trend_array = []

        for i in range(0, total_data_count):
            #高値か安値かの検証をする値
            predicted_value = self.target_klines_df.iloc[i]
            #左右各5期間分のデータから最大値と最小値を取得する
            #左右どちらかに5期間分のデータがない場合は空のDataframeを入れて片方の期間と比べる
            left_redicted_values = self.target_klines_df.iloc[
                i - period : i
            ] if i >= period else pd.DataFrame()

            max_left_redicted_value = np.nan if left_redicted_values.empty else left_redicted_values['高値'].max()
            min_left_redicted_value = np.nan if left_redicted_values.empty else left_redicted_values['安値'].min()
            
            right_redicted_values = self.target_klines_df.iloc[
                i + 1 : i + 1 + period
            ] if i + period <= total_data_count else pd.DataFrame()

            max_right_redicted_value = np.nan if right_redicted_values.empty else right_redicted_values['高値'].max()
            min_right_redicted_value = np.nan if right_redicted_values.empty else right_redicted_values['安値'].min()

            result_swing_high = self.swing_high(
                                    predicted_value=predicted_value,
                                    left_redicted_value=max_left_redicted_value,
                                    right_redicted_value=max_right_redicted_value
                                )
            
            #resultがNoneではない場合は配列に追加する
            if result_swing_high is not None:
                analysis_trend_array.append(result_swing_high)
            else: #Noneの時はスイングロウであるか検証する
                result_swing_low = self.swing_low(
                                    predicted_value=predicted_value,
                                    left_redicted_value=min_left_redicted_value,
                                    right_redicted_value=min_right_redicted_value
                                )
                if result_swing_low is not None:
                     analysis_trend_array.append(result_swing_low)


        analysis_trend_df = pd.DataFrame(
            data=np.array(analysis_trend_array),
            columns=self.ANALYSIS_TREND_COLUMNS
        )
        
        return self.pretreatment_df(target_df=analysis_trend_df)
    
    def swing_high(
            self,
            predicted_value: pd.DataFrame,
            left_redicted_value: float = np.nan,
            right_redicted_value: float = np.nan,
        ) -> list | None:
            """
            検証する時間のデータとその時間の前後でもっとも高い高値のデータを渡すと
            比較して検証する時間の高値よりも高い場合はNone、
            検証する時間のデータの高値が一番大きいときにはその期間のデータを
            ['開始時刻', 'スイングハイ', '高値の値', 'None', 'np.nan', 'np.nan', 'np.nan']という構造のリストにして
            返す関数
            """
            #左右のいずれかの高値が検証したい期間の高値より大きい場合は検証値はスイングハイではないと
            #判断してNoneを返す
            is_left_valid = False if np.isnan(left_redicted_value) else predicted_value[
                '高値'] < left_redicted_value
            
            is_right_valid = False if np.isnan(right_redicted_value) else predicted_value[
                '高値'] < right_redicted_value

            if is_left_valid or is_right_valid:
                return None
            else:
                swing_high = [
                                predicted_value['開始時刻'],
                                'スイングハイ',
                                predicted_value['高値'],
                                None,
                                None,
                                None,
                                None
                            ]
            return swing_high
            
    def swing_low(
            self,
            predicted_value: pd.DataFrame,
            left_redicted_value: float = np.nan,
            right_redicted_value: float = np.nan,
        ) -> list | None:
            """
            検証する時間のデータとその時間の前後でもっとも低い安値のデータを渡すと
            比較して検証する時間の安値よりも低い値があった場合はNone、
            検証する時間のデータの安値が一番小さいときにはその期間のデータを
            ['開始時刻', 'スイングロウ', 'None', '安値の値', 'np.nan', 'np.nan', 'np.nan']という構造のリストにして
            返す関数
            """
            #左右のいずれかの安値が検証したい期間の安値より小さい場合は検証値はスイングロウではないと
            #判断してNoneを返す
            is_left_valid = False if np.isnan(left_redicted_value) else predicted_value[
                '安値'] > left_redicted_value
            
            is_right_valid = False if np.isnan(right_redicted_value) else predicted_value[
                '安値'] > right_redicted_value

            if is_left_valid or is_right_valid:
                return None
            else:
                swing_low = [
                                predicted_value['開始時刻'],
                                'スイングロウ',
                                None,
                                predicted_value['安値'],
                                None,
                                None,
                                None
                            ]
            return swing_low

    def pretreatment_df(
            self,
            target_df: pd.DataFrame,
        ) -> pd.DataFrame:
        """
        分析した結果のDataframeの中に連続するスイングハイ、または、スイングロウがある場合は、
        スイングハイの場合はより高い方を、スイングロウの場合はより低い方を残してハイとロウが
        交互になるように加工してDataframeを返す関数
        """

        i = 1
        #データをすべて比較するまで実行
        while (i < target_df.index.size):
            #分類が同じ種類のものであれば値を比較する
            #違うものなら、データを残したまま次の期間のデータを比較する
            if target_df.iloc[i - 1]['分類'] == target_df.iloc[i]['分類']:
                
                match target_df.iloc[i]['分類']:
                    #分類がスイングハイならば高値を比較する
                    case 'スイングハイ':
                        #高値の低い方をdataframeから削除する
                        if target_df.iloc[i - 1]['高値'] < target_df.iloc[i]['高値']:
                            target_df = target_df.drop(target_df.index[i - 1])
                        else:
                            target_df = target_df.drop(target_df.index[i])

                    #スイングロウならば安値
                    case 'スイングロウ':
                        #安値の高い方を削除
                        if target_df.iloc[i - 1]['安値'] > target_df.iloc[i]['安値']:
                            target_df = target_df.drop(target_df.index[i - 1])
                        else:
                            target_df = target_df.drop(target_df.index[i])

                    case _:
                        print(target_df.iloc[i])
                        pass

            else: #分類が違う場合はindexを次のデータに変えて比較する
                i += 1

        #開始時刻をdatetime型に変換してローソク足データの保存されている日付だけを残して削除する
        target_df['開始時刻'] = pd.to_datetime(
            target_df['開始時刻'], format="%Y-%m-%d %H:%M:%S"
        )

        target_df = target_df[
            (self.target_klines_df.iloc[0]['開始時刻'] <= target_df['開始時刻']) &
            (target_df['開始時刻'] <= self.target_klines_df.iloc[-1]['開始時刻'])
        ]

        return target_df
    
    def save_csv(
            self,
            df: pd.DataFrame,
    ) -> None:
        """
        pathの位置にcsvにして保存する関数
        """
        #dataframeのサイズが200以上あるときは新しいデータから200個分残して削除する
        df = df.drop(df.iloc[: -200].index).reset_index(drop=True)

        #上の階層のディレクトリがない場合は作成する
        self.PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.PATH)

    def environmental_awareness(
            self,
            target_data: float,
            latest_data: pd.DataFrame,
            count: int,
        ):
        """
        現在の相場を確認する関数
        現在のローソク足データの値と直近データを渡すと今のトレンド状況に応じてトレンドが発生、
        もしくは継続または崩れたかどうかを判定して、転換値と直近目標値を返す
        """
        if isinstance(self.trend, float):
            return self.validate_trend(
                        target_data=target_data,
                        latest_data=latest_data,
                        count=count
                    )
        elif self.trend == '上昇':
            return self.validate_up_trend(
                        target_data=target_data,
                        latest_data=latest_data
                    )
        else:
            return self.validate_down_trend(
                        target_data=target_data,
                        latest_data=latest_data
                    )

    def validate_trend(self,
                       target_data: float,
                       latest_data : pd.DataFrame,
                       count: int
        ):
        """
        現在のトレンドがNoneの時に実行されて直近のローソク足がスイングハイかスイングロウかで処理を分岐させる
        スイングハイの場合は上昇トレンドか、スイングロウの場合は下降トレンドになっているかを判定して、トレンドが形成されている場合は現在のトレンド、トレンドが転換する安値、直近高値を返す関数
        """

        #スイングロウの場合は、上昇トレンドであるか検証する
        if latest_data['分類'] == 'スイングロウ':
            #直近安値と直近高値
            latest_low = float(latest_data['安値'])
            latest_second_low = float(self.df.iloc[count - 3]['安値'])
            latest_high = float(self.df.iloc[count - 2]['高値'])

            #安値が切り上がっているか判定
            is_low_price_up = latest_second_low < latest_low

            #高値が切り上がっているか判定
            is_high_price_up = latest_high < target_data

            #高値と安値がそれぞれ切り上がっている場合は上昇トレンドと捉えてトレンドに
            #'上昇'を入れてconversion_valueに直近安値を入れ,target_valueに現在のローソク足の高値を入れる
            if is_low_price_up and is_high_price_up:
                self.trend = '上昇'
                conversion_value = latest_low
                target_value = target_data

            else:
                self.trend = np.nan
                conversion_value = np.nan
                target_value = np.nan

        #スイングハイのデータの場合は、下降トレンドであるか検証する
        else:
            #直近安値と直近高値
            latest_low = float(self.df.iloc[count - 2]['安値'])
            latest_high = float(latest_data['高値'])
            latest_second_hign = float(self.df.iloc[count - 3]['高値'])

            #安値が切り下がっているか判定
            is_low_price_cut = latest_low > target_data

            #高値が切り下がっているか判定
            is_high_price_cut = latest_second_hign > latest_high

            #高値と安値がそれぞれ切り下がっている場合は下降トレンドと捉えて現在のローソク足の
            #トレンドカラムに'下降'を入れて転換値カラムに直近高値を入れ,直近目標値に現在のローソク足の
            #安値を入れる
            if is_low_price_cut and is_high_price_cut:
                self.trend = '下降'
                conversion_value = latest_high
                target_value = target_data
            
            else:
                self.trend = np.nan
                conversion_value = np.nan
                target_value = np.nan

        return conversion_value, target_value

    def validate_up_trend(
            self,
            target_data: float,
            latest_data : pd.DataFrame
        ):
        """
        target_dataの一つ前のローソク足の分類で処理を分岐させる。
        スイングハイの場合は安値を、スイングロウの場合は高値を比較してそれぞれ切り上がっている場合は上昇トレンドと判断して現在のトレンド、トレンドが転換する安値、直近高値を返す関数
        """

        #現在のローソク足の一つ前のデータから転換値を取得してtarget_dataがこの値よりも低い場合トレンド転換と判断
        conversion_value = float(latest_data['転換値'])
        current_target_value = float(latest_data['直近目標値'])

        #前回データがスイングハイの場合は安値が切り上がっていることを確認する
        if latest_data['分類'] == 'スイングハイ':
            is_low_price_up = conversion_value <= target_data

            #安値が前回安値を上回っている場合は上昇トレンドの継続と判断して、
            #トレンドカラムに'上昇'を入れて'転換値'カラムと'直近目標値'に前のデータの値を入れる
            if not is_low_price_up:
                #安値の更新に失敗した場合はトレンドの転換と判断して、トレンド,転換値,直近目標値すべてをNaNで返す
                self.trend = np.nan
                conversion_value = np.nan
                current_target_value = np.nan
            
        else:
            is_high_price_up = current_target_value <= target_data

            #直近目標値の値をtarget_data,転換値をlatest_dataの安値に更新して返す
            if is_high_price_up:
                conversion_value = latest_data['安値']
                current_target_value = target_data
            
            #切り上がっていない場合でも安値が切り上がっているため上昇トレンド継続と判断して直近目標値と転換値を前のデータのカラムから取得する
                
        return conversion_value, current_target_value
    
    def validate_down_trend(
            self,
            target_data: float,
            latest_data : pd.DataFrame
        ):
        """
        target_dataの一つ前のローソク足の分類で処理を分岐させる。
        スイングハイの場合は安値を、スイングロウの場合は高値を比較してそれぞれ切り下がっている場合は下降トレンドと判断して現在のトレンド、トレンドが転換する高値、直近安値を返す関数
        """

        conversion_value = float(latest_data['転換値'])
        current_target_value = float(latest_data['直近目標値'])

        #前回データがスイングハイの場合は安値が切り下がっていることを確認する
        if latest_data['分類'] == 'スイングハイ':

            is_low_price_down = current_target_value > target_data

            #安値が前回安値を下回っている場合は下降トレンドの継続と判断して、
            #トレンドに'下降'を入れて転換値を前回高値、直近目標値に現在のローソク足データ
            #を入れる
            if is_low_price_down:
                conversion_value = latest_data['高値']
                current_target_value = target_data
            
            #安値の更新に失敗した場合でも高値は転換値を超えていないためトレンドは継続と判断して、
            #トレンドに'下降',転換値と直近目標値を前のデータのカラムから取得する
            
        else:
            is_high_price_down = conversion_value > target_data

            #転換値を現在のローソク足データに更新して返す
            if is_high_price_down:
                conversion_value = target_data
                current_target_value = latest_data['安値']
            
            #転換値を超えた場合はトレンドの転換と判断して、トレンド,転換値,直近目標値すべてをNaNで返す
            else:
                self.trend = np.nan
                conversion_value = np.nan
                current_target_value = np.nan

        return conversion_value, current_target_value