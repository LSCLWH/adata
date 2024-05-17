from adata import stock
from sqlalchemy import create_engine


#获取2005年-2023年交易日数据
for i in range(2005, 2024):  # 从 2 开始，到 20（不包括 20）结束
    print(i)
    df = stock.info.trade_calendar(i)

    print(df)
    engine = create_engine(
        "mysql+pymysql://root:root@localhost:3306/a_historical_data?charset=utf8"
    )
    df.to_sql(name="get_trading_day", con=engine, if_exists="append", index=False)