分布式ID生成器
1 leafmysql.py 参考美团leaf算法，实现基于mysql组件的ID生成器。主要思想：
	a.每次从mysql中获取一个号段，只有号段使用到一定比例，才异步加载下一个号段，避免频繁读写数据库。
	b.使用双缓存策略，异步加载下一个号段时，不影响前一个号段的使用。
2 snowflake.py 实现了snowflake算法，基于时钟生成ID，但是ID比较长，趋势递增。
3 server.py和asyncserver.py基于tornado实现了http服务
4 getid_grpc_server.py和getid_client.py分别是基于grpc的rpc调用样例