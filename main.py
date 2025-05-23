import config
import datetime
import scrape
import time

APIKEY = config.APIKEY

def transform_timestamp(
        year: int,
        month: int,
        day: int
    ) -> float:
    #指定した日付のdateオブジェクト
    date = datetime.date(year, month, day)

    #combineメソッドでdatetimeオブジェクトに変換してtimestampを返す
    #単位がsecondなのでmsに変換
    return datetime.datetime.combine(date, datetime.time()).timestamp() * 1000

#テスト用データ
start = transform_timestamp(2025, 5, 1)
end = transform_timestamp(2025, 5, 20)

if __name__ == '__main__':
    one_BTC = scrape.ScrapeMarketData()
    one_BTC.get_kline(
        start=start,
        end=end
        )