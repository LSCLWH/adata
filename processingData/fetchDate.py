import re
from getData import archive_data


#提取日期方法
def withdrawDate(str_date):
    # 假设这是你的文本字段
    text = str_date

    # 使用正则表达式匹配日期
    # 这个正则表达式假设日期格式为"年-月-日"，并且"年"、"月"、"日"之间有明确的分隔符
    # 注意：由于你的文本包含中文的年和月，我们需要匹配这些中文字符
    pattern = r'\d{4}年\d{1,2}月\d{1,2}日'

    # 使用re.findall找到所有匹配的日期
    dates = re.findall(pattern, text)

    # dates现在是一个列表，包含所有找到的日期字符串
    # 如果你只想要第一个日期（或者你知道文本中只有一个日期）
    first_date = dates[0] if dates else None

    # 如果你需要将字符串转换为datetime对象（可选）
    from datetime import datetime

    try:
        # 使用strptime方法将字符串转换为datetime对象
        # 注意：这里我们假设日期的格式为"YYYY年MM月DD日"，并且需要跳过中文分隔符
        date_object = datetime.strptime(first_date.replace('年', '-').replace('月', '-').replace('日', ''), '%Y-%m-%d')
    except (ValueError, TypeError):  # 如果first_date是None或者格式不正确
        date_object = None

    return date_object
    # print("提取的日期字符串:", first_date)
    # print("转换后的日期对象:", date_object)

# 记录开始时间
# start_time = time.time()

query = """WITH RankedRecords AS (
	SELECT
		id,
		jx,
		signal_date,
		ROW_NUMBER() OVER (
			PARTITION BY jx 
		ORDER BY
		( SELECT NULL )) AS rn -- 这里我们不需要真正的排序，因为只关心每组的第一行
		
	FROM
		20_tactics 
	WHERE
		signal_date IS NULL 
	) SELECT
	id,
	jx,
	signal_date 
FROM
	RankedRecords 
WHERE
	rn = 1;"""
taclists = slelect_data.select_data(query)
for taclist in taclists:
    # for_start_time = time.time()
    signal_date = withdrawDate(taclist[1])
    print(taclist[1],taclist[0])
    update = "UPDATE 20_tactics SET signal_date= %s WHERE jx = %s"
    slelect_data.up_data(update, (signal_date, taclist[1]))
    # for_end_time = time.time()
    # elapsed_time = for_end_time - for_start_time
    # print(f"代码运行了 {elapsed_time:.4f} 秒")

# # 记录结束时间
# end_time = time.time()
# # 计算并打印运行时间
# elapsed_time = end_time - start_time
# print(f"代码运行了 {elapsed_time:.4f} 秒")















