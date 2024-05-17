from getData import archive_data
import time

# 查询重复数据
query_sql = """SELECT trading_day__id_5, COUNT(id) as cnt, MIN(id) as min_id
FROM 20_tactics
GROUP BY trading_day__id_5
HAVING cnt > 1"""
idDatas = archive_data.select_data(query_sql)
# 记录开始时间
start_time = time.time()
# 遍历结果，删除重复数据
for idData in idDatas:
    delete_sql = "DELETE FROM 20_tactics WHERE id = %s"
    archive_data.deleyeId(delete_sql,(idData[2],))
    print(idData[2])

# 记录结束时间
end_time = time.time()
# 计算并打印运行时间
run_time = end_time - start_time
print(f"程序运行时间为: {run_time:.6f} 秒")