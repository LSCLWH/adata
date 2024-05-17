import adata
from sqlalchemy import create_engine

# # 批量写入临时表 全量A股
# res_df = adata.stock.info.all_code()
# print(res_df)

# 获取A股所有指数信息列表
df = adata.stock.info.all_index_code()
print(df)
engine = create_engine(
    "mysql+pymysql://root:root@localhost:3306/a_historical_data?charset=utf8"
)
df.to_sql(name="all_index_code", con=engine, if_exists="replace", index=False)