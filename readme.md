功能介绍：
    mongodb 的一个同步工具，具备将一个数据源上的数据，同步到其它 mongodb 上，支持：
    1) mongos -> (mongos, mongod)
    2) mongod -> (mongos, mongod)

    如果源是 mongos，情况比较复杂，需要从 mongos 里将副本信息全部取出来，同步到 mongod 中；

    需要注意的是，源和目的 mongo，都需要使用 admin 账号，以取得所有权限；

    支持 oplog 格式为："ts" : Timestamp(1372320938000, 1)  目前的 2.6.4 版本是这种格式；

配置介绍：
    sync-info
        mode: 取值为 incr 表示增量同步；all 表示全量同步，会将源数据拷贝到目标库；smart 表示智能同步，会先全量拷贝，再进行增量；
        record_interval: 读取了一定量数据，会对 optime 进行更新；
        record_time_interval: 读取了一定的时间，会对 optime 进行更新；
        opt_file: optime 记录在 该文件中，all 模式在拷贝完更新它，incr, smart 模式不定期更新它，如果文件不存在则默认从 1 小时前同步；
        all_dbs: 为 true 表示同步全部的数据库, 不包括 admin，config，local, 否则同步 dbs；
        dbs: 需要同步的数据库集合，不包括 admin，config，local；
        queue_num: 队列最大数目，暂无使用该参数，代码里固定了是 20000；
        threads: 线程数，由于源和目的 的网络状况可能不一样，可以有多个连接去写目的地址；

    mongo-src
        addr: 源地址，可以是一个副本集，也可以是 mongos，如果是 mongos，系统会自动连接到相应的副本集；
        user: 管理员账号
        pwd: 管理员密码

    mongo-dest
        addr: 目的地址，可以是一个副本集，也可以是 mongos；
        user: 管理员账号
        pwd: 管理员密码

重点说明：
    1. 当目的没有数据库时和集合时：
        会自动创建，包括索引

    2. 当源为 mongos 时：
        所有副本集的内容会同步过去；

    3. 当源为 副本集 时：
        所有副本集的内容会同步过去；

    4. 增加、删除、修改 数据命令：
        OK；

    5. 增加、删除 collection 时：
        OK；

    6. db 级别的操作：
        忽略；

    7. 同步性能：
        取决于网络带宽；

    8. 容错能力：
        能处理源和目的的网络异常，系统有容错处理能力；

运行方式：
    python src/python/main.py conf/xxx.conf logs/xxx.log [is-debug]
    is-debug: 取值为 true，false, 为 true 日志会打印 debug 的日志，否则关闭 debug 日志

Change Log:
    [2014-12.01]: 支持了多线程，解决 源和目的 速度不一致的问题，当目的端较慢，采用多线程来提高性能；