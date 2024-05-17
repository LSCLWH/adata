import mysql.connector




# 将to_sql写入的数据转写到正式表
# 配置数据库连接参数
def select_data(data_query):
    db_config = {
        "host": "127.0.0.1",
        "user": "root",
        "password": "root",
        "database": "a_historical_data",
    }

    # 建立数据库连接
    cnx = mysql.connector.connect(**db_config)
    # 创建游标对象
    cursor = cnx.cursor()
    try:
        # 执行查询
        cursor.execute(data_query)
        # 获取查询结果
        results = cursor.fetchall()
        return results
    except Exception as e:
        print('执行查询失败', e)
        cnx.rollback()  # 回滚
    finally:
        # 关闭游标和连接
        cursor.close()
        cnx.close()

def up_data(update_query,stock_code):
    db_config = {
        "host": "127.0.0.1",
        "user": "root",
        "password": "root",
        "database": "a_historical_data",
    }

    # 建立数据库连接
    cnx = mysql.connector.connect(**db_config)
    # 创建游标对象
    cursor = cnx.cursor()
    try:
        # 执行你的 SQL 语句
        # where = "stock_code = %s";
        # stock_code = '000001'
        # update_query = "UPDATE all_code  SET is_get = 1  WHERE "+where
        cursor.execute(update_query,stock_code)
        cnx.commit()
    except Exception as e:
        print('执行更新失败', e)
        cnx.rollback()  # 回滚
    finally:
        cursor.close()
        cnx.close()

def select_datas(data_query,data):
    db_config = {
        "host": "127.0.0.1",
        "user": "root",
        "password": "root",
        "database": "a_historical_data",
    }

    # 建立数据库连接
    cnx = mysql.connector.connect(**db_config)
    # 创建游标对象
    cursor = cnx.cursor()
    try:
        # 执行查询
        cursor.execute(data_query,data)
        # 获取查询结果
        results = cursor.fetchall()
        return results
    except Exception as e:
        print('执行查询失败', e)
        cnx.rollback()  # 回滚
    finally:
        # 关闭游标和连接
        cursor.close()
        cnx.close()

def deleyeId(delete_sql,id):
    db_config = {
        "host": "127.0.0.1",
        "user": "root",
        "password": "root",
        "database": "a_historical_data",
    }

    # 建立数据库连接
    cnx = mysql.connector.connect(**db_config)
    # 创建游标对象
    cursor = cnx.cursor()
    try:
        # 执行查询
        cursor.execute(delete_sql, id)
        # 获取查询结果
        results = cursor.fetchall()
        return results
    except Exception as e:
        print('执行删除失败', e)
        cnx.rollback()  # 回滚
    finally:
        # 关闭游标和连接
        cursor.close()
        cnx.close()

def addData(add_sql,data):
    db_config = {
        "host": "127.0.0.1",
        "user": "root",
        "password": "root",
        "database": "a_historical_data",
    }

    # 建立数据库连接
    cnx = mysql.connector.connect(**db_config)
    # 创建游标对象
    cursor = cnx.cursor()
    try:
        # 执行新增
        cursor.execute(add_sql, data)
        # 获取结果
        results = cursor.fetchall()
        return results
    except Exception as e:
        print('执行新增失败', e)
        cnx.rollback()  # 回滚
    finally:
        # 关闭游标和连接
        cursor.close()
        cnx.close()