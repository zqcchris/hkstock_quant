create_transaction = """ create TABLE if not exists transaction(
id int(11) not null auto_increment,
uuid varchar(128) not null,
exchange varchar(20) not null,
symbol varchar(20) default null,
message_id varchar(20) default null,
open_direction varchar(20) default null,
open_time varchar(20) default null,
open_price varchar(20) default null,
close_time varchar(20) default null,
sl_price varchar(20) default null,
close_price varchar(20) default null,
sl varchar(20) default null,
tp varchar(20) default null,
max_tp float(6,4) default null,
vol float(9,2) default null,
tp_est float(6,4) default null,
comment varchar(256) default null,
trigger_time varchar(20) default null,
tp_mode varchar(20) default null,
primary key(id)
)ENGINE=InnoDB AUTO_INCREMENT=2, DEFAULT CHARSET=utf8;
"""


create_backtest = """ create TABLE if not exists backtest(
id int(11) not null auto_increment,
uuid varchar(128) not null,
exchange varchar(20) not null,
symbol varchar(20) default null,
message_id varchar(20) default null,
open_direction varchar(20) default null,
open_time varchar(20) default null,
open_price varchar(20) default null,
close_time varchar(20) default null,
sl_price varchar(20) default null,
close_price varchar(20) default null,
sl varchar(20) default null,
tp varchar(20) default null,
max_tp float(6,4) default null,
vol float(9,2) default null,
tp_est float(6,4) default null,
comment varchar(256) default null,
trigger_time varchar(20) default null,
tp_mode varchar(20) default null,
primary key(id)
)ENGINE=InnoDB AUTO_INCREMENT=2, DEFAULT CHARSET=utf8;
"""


create_critic = """ create TABLE if not exists critic(
id int(11) not null auto_increment,
uuid varchar(128) not null,
exchange varchar(20) not null,
symbol varchar(20) default null,
critic_interval varchar(20) default null,
category varchar(20) default null,
open_direction varchar(20) default null,
critic_time varchar(20) default null,
comment varchar(256) default null,
primary key(id)
)ENGINE=InnoDB AUTO_INCREMENT=2, DEFAULT CHARSET=utf8;
"""


create_change = """ create TABLE if not exists abnormal_change(
id int(11) not null auto_increment primary key,
uuid varchar(128) not null,
exchange varchar(20) not null,
symbol varchar(20) default null,
name varchar(20) default null,
industry varchar(20) default null,
price float(6,2) default null,
chg float(6,4) default null,
change_interval varchar(20) default null,
change_time varchar(20) default null,
unique key(exchange, symbol, change_interval)
)ENGINE=InnoDB AUTO_INCREMENT=2, DEFAULT CHARSET=utf8;
"""


create_ustock_change = """ create TABLE if not exists ustock_change(
id int(11) not null auto_increment primary key,
uuid varchar(128) not null,
exchange varchar(20) not null,
symbol varchar(20) default null,
name varchar(20) default null,
industry varchar(20) default null,
price float(6,2) default null,
chg float(6,4) default null,
change_interval varchar(20) default null,
change_time varchar(20) default null,
unique key(exchange, symbol, change_interval)
)ENGINE=InnoDB AUTO_INCREMENT=2, DEFAULT CHARSET=utf8;
"""


create_crypto_change = """ create TABLE if not exists crypto_change(
id int(11) not null auto_increment primary key,
uuid varchar(128) not null,
exchange varchar(20) not null,
symbol varchar(20) default null,
price float(12,6) default null,
chg float(6,4) default null,
change_interval varchar(20) default null,
change_time varchar(20) default null,
unique key(exchange, symbol, change_interval)
)ENGINE=InnoDB AUTO_INCREMENT=2, DEFAULT CHARSET=utf8;
"""


create_crypto_pool = """ create TABLE if not exists crypto_pool(
id int(11) not null auto_increment primary key,
uuid varchar(128) not null,
exchange varchar(20) not null,
symbol varchar(20) default null,
price float(12,6) default null,
chg float(6,4) default null,
change_interval varchar(20) default null,
change_time varchar(20) default null,
pool_strategy varchar(20) default null,
unique key(exchange, symbol, change_interval)
)ENGINE=InnoDB AUTO_INCREMENT=2, DEFAULT CHARSET=utf8;
"""


create_desert = """ create TABLE if not exists desert(
id int(11) not null auto_increment,
uuid varchar(128) not null,
exchange varchar(20) not null,
symbol varchar(20) default null,
desert_interval varchar(20) default null,
desert_time varchar(20) default null,
psd varchar(20) default null,
comment varchar(256) default null,
primary key(id)
)ENGINE=InnoDB AUTO_INCREMENT=2, DEFAULT CHARSET=utf8;
"""


create_spot = """ create TABLE if not exists spot(
id int(11) not null auto_increment,
uuid varchar(128) not null,
exchange varchar(20) not null,
symbol varchar(20) default null,
open_direction varchar(20) default null,
open_time varchar(20) default null,
open_price varchar(20) default null,
close_time varchar(20) default null,
sl_price varchar(20) default null,
tp_price varchar(20) default null,
sl varchar(20) default null,
tp varchar(20) default null,
final_sl varchar(256) default null,
max_tp varchar(20) default null,
comment varchar(256) default null,
trigger_time varchar(20) default null,
tp_mode varchar(20) default null,
primary key(id)
)ENGINE=InnoDB AUTO_INCREMENT=2, DEFAULT CHARSET=utf8;
"""


create_settlement = """ create TABLE if not exists settlement(
id int(11) not null auto_increment,
username varchar(20) not null,
exchange varchar(20) not null,
symbol varchar(20) default null,
commission varchar(20) default null,
commissionAsset varchar(20) default null,
time varchar(20) default null,
tranid varchar(20) default null,
orderId varchar(20) default null,
price varchar(20) default null,
qty varchar(20) default null,
quoteQty varchar(20) default null,
realizedPnl varchar(20) default null,
positionSide varchar(20) default null,
primary key(id)
)ENGINE=InnoDB AUTO_INCREMENT=2, DEFAULT CHARSET=utf8;
"""


def create_tables(con):
    cursor = con.cursor()
    cursor.execute("use strategy;")
    cursor.execute(create_transaction)
    cursor.execute(create_settlement)
    cursor.execute(create_spot)
    cursor.execute(create_backtest)
    cursor.execute(create_critic)
    cursor.execute(create_desert)
    cursor.execute(create_change)
    cursor.execute(create_crypto_change)
    cursor.execute(create_ustock_change)
    cursor.execute(create_crypto_pool)
