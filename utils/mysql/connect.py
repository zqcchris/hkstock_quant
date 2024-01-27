import sys, os
sys.path.append(os.getcwd())
import pymysql
from utils.utils import get_public_ip
from func_timeout import func_set_timeout
from utils.mysql.hkstock.create_table import create_hk_tables
from config.server_config import astock_server, hku_server, crypto_server
from utils.mysql.strategy.create_tables import create_tables as create_strategy_tables


"""
初始化：mysqld --defaults-file=/etc/my.cnf --basedir=/usr/local/mysql --datadir=/data/mysql --user=mysql --initialize-insecure
sudo service mysql start, service命令主要在/etc/init.d/目录里寻找对应的启动脚本，可以用于排查一些错误

mysql: 8.0.33; Linux: Ubuntu
安装mysql:  sudo apt-get install mysql-server
无密码登陆:  sudo mysql -uroot -p
开启远程连接:
            1、update user set host='%' where user='root';
            2、flush privileges;
            3、如果这两步还不行，需要修改配置文件的bind-address = 0.0.0.0
修改密码:    1、ALTER USER 'root'@'%' identified with mysql_native_password BY '****';
            2、flush privileges;
综合指导：   https://blog.csdn.net/qq_37120477/article/details/130653390
配置文件：   /etc/mysql/mysql.conf.d/mysqld.cnf
数据库迁移:  https://blog.csdn.net/Yu_Cblog/article/details/130484205
           https://www.php.cn/faq/577402.html
mysql卸载:  https://zhuanlan.zhihu.com/p/636012442
公钥私钥：   https://blog.csdn.net/m0_46897923/article/details/128306658， 配置公钥私钥实现免密登陆，从而构建mysql的tcpip连接
           https://juejin.cn/post/7252890861066190906

sudo apt-get clean
sudo apt-get purge 'mysql*'
sudo apt-get update
sudo apt-get install -f
sudo apt-get install mysql-server-8.0
sudo apt-get dist-upgrade

配置ali软件源：
[root@192 ~]# vim /etc/yum.repos.d/aliyun_yum.repo 
[ali_baseos]
name=ali_baseos
baseurl=https://mirrors.aliyun.com/centos-stream/9-stream/BaseOS/x86_64/os/
gpgcheck=0
[ali_appstream]
name=ali_appstream
baseurl=https://mirrors.aliyun.com/centos-stream/9-stream/AppStream/x86_64/os/
gpgcheck=0
————————————————
版权声明：本文为CSDN博主「维生素E」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
原文链接：https://blog.csdn.net/weixin_58297531/article/details/128934687

sudo apt-get install python3-pip
"""


class SQLUtils(object):
    def __init__(self, mode="debug"):
        self.mode = mode
        # 如果运行平台是macos，则视为调试程序，默认连接测试服务器：'18.141.167.105'
        if sys.platform == "darwin":
            ip = crypto_server
        else:
            ip = get_public_ip()
        if ip == hku_server:
            self.localhost = "localhost"
            self.host = hku_server
            self.user = '***'
            self.password = "***"
        else:
            print("服务器ip地址变更，请核查！")
        self.connection = pymysql.connect(host=self.host, user=self.user, password=self.password, charset='utf8', autocommit=True)

    def create_database(self):
        if not self.connection.open:
            self.connection = pymysql.connect(host=self.host, user=self.user, password=self.password, charset='utf8')
        cursor = self.connection.cursor()
        cursor.execute("create database if not exists hkstock_market")
        cursor.execute("create database if not exists strategy")
        cursor.execute("use strategy;")

    @func_set_timeout(60)
    def to_episode(self, symbol, direction, open_time, sl, close_time="", tp="", close_price="", exchange=None, uuid="",
                   trigger_time="", sl_price="", open_price="", comment="", vol="", tp_est=None, message_id=""):
        if not exchange: exchange = 'binance'

        if not self.connection.open:
            self.connection = pymysql.connect(host=self.host, user=self.user, password=self.password, charset='utf8')

        cursor = self.connection.cursor()
        cursor.execute("use strategy;")
        if self.mode == "debug":
            sql = "insert INTO backtest (uuid, exchange, symbol, message_id, open_direction, open_time, close_time," \
                  " sl, tp, close_price, trigger_time, sl_price, open_price, comment, vol, tp_est) VALUES ('%s', " \
                  "'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');" \
                  % (uuid, exchange, symbol, message_id, direction, open_time, close_time, sl, tp, close_price,
                     trigger_time, sl_price, open_price, comment, vol, tp_est)
        else:
            sql = "insert INTO transaction (uuid, exchange, symbol, message_id, open_direction, open_time, " \
                  "close_time, sl, tp, close_price, trigger_time, sl_price, open_price, comment, vol, tp_est) VALUES " \
                  "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');" \
                  % (uuid, exchange, symbol, message_id, direction, open_time, close_time, sl, tp, close_price,
                     trigger_time, sl_price, open_price, comment, vol, tp_est)

        cursor.execute(sql)
        self.connection.commit()

    @func_set_timeout(60)
    def to_change(self, symbol, chg, name, industry, price, change_time, change_interval="", exchange=None, uuid=""):
        if not exchange: exchange = 'hkstock'

        if not self.connection.open:
            self.connection = pymysql.connect(host=self.host, user=self.user, password=self.password, charset='utf8')
        cursor = self.connection.cursor()
        cursor.execute("use strategy;")
        sql = "replace INTO abnormal_change (uuid, exchange, symbol, chg, name, industry, change_time, " \
              "change_interval, price) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');" % (uuid,
              exchange, symbol, chg, name, industry, change_time, change_interval, price)

        cursor.execute(sql)
        self.connection.commit()

    @func_set_timeout(60)
    def get_abnormal_change(self, interval):
        if not self.connection.open:
            self.connection = pymysql.connect(host=self.host, user=self.user, password=self.password, charset='utf8')
        cursor = self.connection.cursor()
        cursor.execute("use strategy;")
        sql = "select symbol, name, industry, price, chg, change_interval, change_time from abnormal_change " \
              "where change_interval='%s' order by chg desc limit 500" % interval

        cursor.execute(sql)
        result = list(cursor.fetchall())
        return result

    @func_set_timeout(60)
    def update_episode(self, close_time, tp, uuid, max_tp, tp_mode, close_price=""):
        try:
            if not self.connection.open:
                self.connection = pymysql.connect(host=self.host, user=self.user, password=self.password, charset='utf8')

            c = self.connection.cursor()
            c.execute("use strategy;")

            if self.mode == "debug":
                sql = "update backtest set close_time='%s', tp='%s', max_tp='%s', tp_mode='%s' where " \
                      "uuid='%s';" % (close_time, tp, max_tp, tp_mode, uuid)
            else:
                sql = "update transaction set close_time='%s', tp='%s', max_tp='%s', tp_mode='%s' " \
                      "where uuid='%s';" % (close_time, tp, max_tp, tp_mode, uuid)
            c.execute(sql)
            self.connection.commit()
        except:
            print("update episode error")

    def create_strategy_tables(self):
        if not self.connection.open:
            self.connection = pymysql.connect(host=self.host, user=self.user, password=self.password, charset='utf8')
        create_strategy_tables(con=self.connection)

    def create_hkstock_tables(self):
        if not self.connection.open:
            self.connection = pymysql.connect(host=self.host, user=self.user, password=self.password, charset='utf8')
        create_hk_tables(con=self.connection)


if __name__ == '__main__':
    sql_utils = SQLUtils(mode="debug")
    # sql_utils.create_database()
    sql_utils.create_strategy_tables()
    sql_utils.create_hkstock_tables()
