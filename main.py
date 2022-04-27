import threading
import pymysql
from dbutils.pooled_db import PooledDB
import datetime
import math
import queue
import requests
import configparser

# 创建配置类对象
cf = configparser.ConfigParser()

# 读取配置文件
cf.read("config.ini", encoding="utf-8")

x_pi = 3.14159265358979324 * 3000.0 / 180.0
pi = 3.1415926535897932384626  # π
a = 6378245.0  # 长半轴
ee = 0.00669342162296594323  # 扁率


# 高德转84
def gcj02towgs84(lng, lat):
    """
    GCJ02(火星坐标系)转GPS84
    :param lng:火星坐标系的经度
    :param lat:火星坐标系纬度
    :return:
    """
    dlat = transformlat(lng - 105.0, lat - 35.0)
    dlng = transformlng(lng - 105.0, lat - 35.0)
    radlat = lat / 180.0 * pi
    magic = math.sin(radlat)
    magic = 1 - ee * magic * magic
    sqrtmagic = math.sqrt(magic)
    dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * pi)
    dlng = (dlng * 180.0) / (a / sqrtmagic * math.cos(radlat) * pi)
    mglat = lat + dlat
    mglng = lng + dlng
    return [lng * 2 - mglng, lat * 2 - mglat]


# 纬度转换
def transformlat(lng, lat):
    ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + \
          0.1 * lng * lat + 0.2 * math.sqrt(math.fabs(lng))
    ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 *
            math.sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lat * pi) + 40.0 *
            math.sin(lat / 3.0 * pi)) * 2.0 / 3.0
    ret += (160.0 * math.sin(lat / 12.0 * pi) + 320 *
            math.sin(lat * pi / 30.0)) * 2.0 / 3.0
    return ret


# 经度转换
def transformlng(lng, lat):
    ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + \
          0.1 * lng * lat + 0.1 * math.sqrt(math.fabs(lng))
    ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 *
            math.sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lng * pi) + 40.0 *
            math.sin(lng / 3.0 * pi)) * 2.0 / 3.0
    ret += (150.0 * math.sin(lng / 12.0 * pi) + 300.0 *
            math.sin(lng / 30.0 * pi)) * 2.0 / 3.0
    return ret


class Processing:

    def __init__(self, host=cf.get("mysql", "host"), user_name=cf.get("mysql", "user"),
                 password=cf.get("mysql", "password"), db=cf.get("mysql", "db"),
                 max_connections=cf.getint("mysql", "max_connections"),
                 thread_num=cf.getint("thread", "thread_num")):

        # 创建数据库连接池
        self.pool = PooledDB(creator=pymysql, maxconnections=max_connections, maxshared=max_connections, host=host,
                             user=user_name,
                             passwd=password, db=db, port=3306)

        # 线程数
        self.thread_num = thread_num

        # 锁
        self.lock = threading.Lock()

        # 结果队列
        self.res = queue.Queue(10)

        # 队列锁
        self.qlock = threading.Lock()

        self.url = 'http://restapi.amap.com/v3/place/text'

        self.key = "16baa52d92eff3168020c04c3dd0957a"

    # 每个线程运行:从数据库读取分页数据，对每条数据进行加工，写入同一个文件
    # begin,num 分页
    def thread_doing(self, begin, num):
        print('begin:{},num:{}'.format(begin, num))
        conn = self.pool.connection()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cursor.execute('select * from tw limit %s,%s', [begin, num])
        c = 0
        results = cursor.fetchall()

        for rs in results:
            update_sql = "update tw set "
            addr_coor = []
            off_coor = []
            # self.lock.acquire()  # 加锁

            if rs['add_lon'] is None and rs['add_lat'] is None and rs['off_lon'] is None and rs['off_lat'] is None:
                if rs['address'] is not None:
                    address = rs['address'].strip()
                    param = {
                        'keywords': address,
                        'city': 710000,
                        'key': self.key
                    }
                    addr_coor = self.update_data(param, rs['_c0'])

                if rs['off_addr'] is not None:
                    off_address = rs['off_addr'].strip()
                    param_off = {
                        'keywords': off_address,
                        'city': 710000,
                        'key': self.key
                    }
                    off_coor = self.update_data(param_off, rs['_c0'])

                if len(addr_coor) > 0:
                    update_sql = update_sql + "add_lon=" + str(addr_coor[0]) + ", add_lat=" + str(addr_coor[1])
                else:
                    update_sql = update_sql + "add_lon=null, add_lat=null"

                if len(off_coor) > 0:
                    update_sql = update_sql + ", off_lon=" + str(off_coor[0]) + ", off_lat=" + str(off_coor[1])
                else:
                    update_sql = update_sql + ", off_lon=null, off_lat=null"

                update_sql = update_sql + " WHERE _c0 = " + "'" + rs['_c0'] + "'"

                print('update_sql:', update_sql)

                self.lock.acquire()  # 加锁

                try:
                    # 执行SQL语句
                    cursor.execute(update_sql)
                    # 提交修改
                    conn.commit()
                except Exception as e:
                    conn.rollback()  # 事务回滚
                    print('SQL执行有误,原因:', e)

                self.lock.release()

                c = c + 1

        self.write_res(begin, num, c)
        cursor.close()
        conn.close()  # 将连接放回连接池

    # 将运行结果写入队列
    def write_res(self, begin, num, c):
        res = '线程【{},{}】运行结束，写入总数：{}，结束时间：{}'.format(begin, num, c, datetime.datetime.now().strftime('%Y%m%d%H%M%S'))

        self.qlock.acquire()
        self.res.put(res)
        self.qlock.release()

    def update_data(self, param, _c0):
        response = requests.get(self.url, params=param)
        if response.json()["status"] == '1':
            if len(response.json()["pois"]) != 0:
                pois = response.json()["pois"]
                print("pois", pois[0]["location"])
                location = pois[0]["location"]
                location_lon = location.split(",")[0]
                location_lat = location.split(",")[1]
                new_coordinates = gcj02towgs84(float(location_lon), float(location_lat))
                print("new_coordinates", new_coordinates)
                return new_coordinates
            else:
                return []
        else:
            return []

    def test(self):
        start_time = datetime.datetime.now()
        print('开始时间：', start_time.strftime('%Y%m%d%H%M%S'))
        # 查找表中全部数据量
        conn = self.pool.connection()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cursor.execute('select * from tw limit 0,10')
        while 1:
            rs = cursor.fetchone()
            if rs is None:
                break
            print(rs)
        cursor.close()
        conn.close()
        end_time = datetime.datetime.now()
        print('{} 完成！耗时：{} '.format(end_time.strftime('%Y%m%d%H%M%S'), end_time - start_time))

    def run(self):
        start_time = datetime.datetime.now()
        print('开始时间：', start_time.strftime('%Y%m%d%H%M%S'))
        # 查找表中全部数据量
        conn = self.pool.connection()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cursor.execute('select count(*) count from tw')
        count = cursor.fetchone()['count']
        cursor.close()
        conn.close()
        # 分页，向上取整
        page = math.ceil(count / self.thread_num)
        print('表数据量：{}，线程数：{}，分页大小：{}'.format(count, self.thread_num, page))

        # 多线程
        ths = []
        # 创建线程
        for i in range(self.thread_num):
            # print(page*i,',',page)
            ths.append(threading.Thread(target=self.thread_doing, args=(page * i, page)))

        # 启动线程
        for i in range(self.thread_num):
            print('线程数-start：{}'.format(i))
            ths[i].start()
        print('等待中........')

        # 等待线程完成
        for i in range(self.thread_num):
            print('线程数-join：{}'.format(i))
            ths[i].join()

        end_time = datetime.datetime.now()
        print('{} 完成！耗时：{} '.format(end_time.strftime('%Y%m%d%H%M%S'), end_time - start_time))

        while not self.res.empty():
            print(self.res.get())


if __name__ == '__main__':
    p = Processing()
    # p.test()
    p.run()
