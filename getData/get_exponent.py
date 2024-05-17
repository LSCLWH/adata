import adata
from sqlalchemy import create_engine
import pandas as pd
import archive_data
import time

#获取指数日线级别数据
#获取需要获取数据的指数代码及时间 查询所有指数代码数据 is_get = 0
query_data = "SELECT index_code,get_date FROM exponent_all_code WHERE is_get = 0;"
exp_lists = archive_data.select_data(query_data)
print(exp_lists)
for exp_list in exp_lists:
    print(exp_list)
    # 获取指数数据 stocks_trading_data
    start_date = exp_list[1].strftime("%Y-%m-%d")
    print(start_date)
    df = adata.stock.market.get_market_index(index_code=exp_list[0], start_date=start_date)
    # 判断是否dataframe对象
    is_dataframe = isinstance(df, pd.DataFrame)
    # 非空且为dataframe对象
    if df is not None and is_dataframe == True:
        engine = create_engine(
            "mysql+pymysql://root:root@localhost:3306/a_historical_data?charset=utf8"
        )
        df.to_sql(name="exponent_trading_data", con=engine, if_exists="append", index=False)
        # 执行已获取数据标记
        stock_code = exp_list[0]
        update = "UPDATE `a_historical_data`.`exponent_all_code` SET `is_get` = 1 WHERE `index_code` =  %s"
        archive_data.up_data(update, (stock_code,))
        time.sleep(2)
    else:
        # 获取失败输出信息
        print(exp_list[0] + "获取失败")

