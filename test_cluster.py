from rediscluster import RedisCluster

startup_nodes = [
    {"host": "172.19.0.2", "port": 7001},
    {"host": "172.19.0.3", "port": 7002},
    {"host": "172.19.0.4", "port": 7003}
]

client = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)
print("Ping:", client.ping())

