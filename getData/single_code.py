import adata
from sqlalchemy import create_engine
import pandas as pd
import archive_data
import time


#获取个股日线级别数据
#获取需要获取数据的股票代码及上市时间 查询所有股票代码数据
query_data = "SELECT stock_code,list_date FROM all_code WHERE is_get = 0;"
datalist = slelect_data.select_data(query_data)
#遍历A股列表
for data in datalist:
    #print(data)
    print(data[0],data[1])
    #获取数据
    res_df = adata.stock.market.get_market(stock_code=data[0], k_type=1, start_date=data[1])
    #写入数据库
    # 判断是否dataframe对象
    is_dataframe = isinstance(res_df, pd.DataFrame)
    # 非空且为dataframe对象
    if res_df is not None and is_dataframe == True:
        # 调用写入数据库函数
        engine = create_engine("mysql+mysqlconnector://root:root@localhost:3306/a_historical_data?charset=utf8")
        res_df.to_sql(name="market_copy1", con=engine, if_exists="append", index=False)
        #执行已获取数据标记
        stock_code = data[0]
        update = "UPDATE all_code  SET is_get = 1  WHERE stock_code = %s"
        slelect_data.up_data(update, (stock_code,))
        time.sleep(2)
        print("暂停2秒")
    else:
        # 获取失败输出信息
        print(data[0] + "获取失败")
