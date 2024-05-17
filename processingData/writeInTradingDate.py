from getData import archive_data
import datetime

# def update():
#


# 记录开始时间
# start_time = time.time()
#获取所有日期去重 写入交易日
query = """-- 查询所有日期并去重
WITH RankedRecords AS ( SELECT id,signal_date, ROW_NUMBER() OVER ( PARTITION BY signal_date ORDER BY ( SELECT NULL )) AS rn -- 这里我们不需要真正的排序，因为只关心每组的第一行
FROM 20_tactics WHERE `trading_day_5` IS NULL ) SELECT id,signal_date FROM RankedRecords WHERE  rn = 1;"""
#获取查询结果
taclists = archive_data.select_data(query)
#遍历时间
for taclist in taclists:
    #根据时间查询该时间开始的交易日,升序
    query_date = "SELECT trade_date FROM trading_day WHERE trade_status = 1 AND trade_date > %s ORDER BY trade_date "
    date_datas = archive_data.select_datas(query_date,(taclist[1],))
    # 访问 date_datas[4] 中的 datetime.date 对象并格式化
    if isinstance(date_datas[4], tuple) and len(date_datas[4]) > 0 and isinstance(date_datas[4][0], datetime.date):
        trading_day_5 = date_datas[4][0].strftime("%Y-%m-%d")
    else:
        trading_day_5 = '1600-01-01'

    if isinstance(date_datas[9], tuple) and len(date_datas[9]) > 0 and isinstance(date_datas[9][0], datetime.date):
        trading_day_10 = date_datas[9][0].strftime("%Y-%m-%d")
    else:
        trading_day_10 = '1600-01-01'

    if isinstance(date_datas[14], tuple) and len(date_datas[14]) > 0 and isinstance(date_datas[14][0], datetime.date):
        trading_day_15 = date_datas[14][0].strftime("%Y-%m-%d")
    else:
        trading_day_15 = '1600-01-01'

    if isinstance(date_datas[19], tuple) and len(date_datas[19]) > 0 and isinstance(date_datas[19][0], datetime.date):
        trading_day_20 = date_datas[19][0].strftime("%Y-%m-%d")
    else:
        trading_day_20 = '1600-01-01'

    if isinstance(date_datas[29], tuple) and len(date_datas[29]) > 0 and isinstance(date_datas[29][0], datetime.date):
        trading_day_30 = date_datas[29][0].strftime("%Y-%m-%d")
    else:
        trading_day_30 = '1600-01-01'

    if isinstance(date_datas[44], tuple) and len(date_datas[44]) > 0 and isinstance(date_datas[44][0], datetime.date):
        trading_day_45 = date_datas[44][0].strftime("%Y-%m-%d")
    else:
        trading_day_45 = '1600-01-01'

    if isinstance(date_datas[59], tuple) and len(date_datas[59]) > 0 and isinstance(date_datas[59][0], datetime.date):
        trading_day_60 = date_datas[59][0].strftime("%Y-%m-%d")
    else:
        trading_day_60 = '1600-01-01'

    if isinstance(date_datas[89], tuple) and len(date_datas[89]) > 0 and isinstance(date_datas[89][0], datetime.date):
        trading_day_90 = date_datas[89][0].strftime("%Y-%m-%d")
    else:
        trading_day_90 = '1600-01-01'

    if isinstance(date_datas[119], tuple) and len(date_datas[119]) > 0 and isinstance(date_datas[119][0], datetime.date):
        trading_day_120 = date_datas[119][0].strftime("%Y-%m-%d")
    else:
        trading_day_120 = '1600-01-01'

    if isinstance(date_datas[159], tuple) and len(date_datas[159]) > 0 and isinstance(date_datas[159][0], datetime.date):
        trading_day_160 = date_datas[159][0].strftime("%Y-%m-%d")
    else:
        trading_day_160 = '1600-01-01'

    if isinstance(date_datas[179], tuple) and len(date_datas[179]) > 0 and isinstance(date_datas[179][0], datetime.date):
        trading_day_180 = date_datas[179][0].strftime("%Y-%m-%d")
    else:
        trading_day_180 = '1600-01-01'

    if isinstance(date_datas[229], tuple) and len(date_datas[229]) > 0 and isinstance(date_datas[229][0], datetime.date):
        trading_day_230 = date_datas[229][0].strftime("%Y-%m-%d")
    else:
        trading_day_230 = '1600-01-01'

    # if isinstance(date_datas[359], tuple) and len(date_datas[359]) > 0 and isinstance(date_datas[359][0], datetime.date):
    #     trading_day_360 = date_datas[359][0].strftime("%Y-%m-%d")
    # else:
    #     trading_day_360 = '1600-01-01'

    # if isinstance(date_datas[499], tuple) and len(date_datas[499]) > 0 and isinstance(date_datas[499][0], datetime.date):
    #     trading_day_500 = date_datas[499][0].strftime("%Y-%m-%d")
    # else:
    #     trading_day_500 = '1600-01-01'

    # if isinstance(date_datas[599], tuple) and len(date_datas[599]) > 0 and isinstance(date_datas[599][0], datetime.date):
    #     trading_day_600 = date_datas[599][0].strftime("%Y-%m-%d")
    # else:
    #     trading_day_600 = '1600-01-01'

    # if isinstance(date_datas[699], tuple) and len(date_datas[699]) > 0 and isinstance(date_datas[699][0], datetime.date):
    #     trading_day_700 = date_datas[699][0].strftime("%Y-%m-%d")
    # else:
    #     trading_day_700 = '1600-01-01'

    # if isinstance(date_datas[899], tuple) and len(date_datas[899]) > 0 and isinstance(date_datas[899][0], datetime.date):
    #     trading_day_900 = date_datas[899][0].strftime("%Y-%m-%d")
    # else:
    #     trading_day_900 = '1600-01-01'


    signal_date = taclist[1]
    #拼接sql语句
    update_date = """UPDATE 20_tactics SET 
    trading_day_5 = %s,
    trading_day_10= %s,
    trading_day_15= %s,
    trading_day_20= %s,
    trading_day_30= %s,
    trading_day_45= %s,
    trading_day_60= %s,
    trading_day_90= %s,
    trading_day_120= %s,
    trading_day_160= %s,
    trading_day_180= %s,
    trading_day_230= %s
    WHERE signal_date = %s"""
    #执行sql语句     trading_day_360= %s,trading_day_500= %s,trading_day_600= %s,trading_day_700= %s,trading_day_900= %s,
    archive_data.up_data(update_date,(trading_day_5,trading_day_10,trading_day_15,trading_day_20,trading_day_30,trading_day_45,trading_day_60,
                                      trading_day_90,trading_day_120,trading_day_160,trading_day_180,trading_day_230,signal_date))
    print(signal_date)