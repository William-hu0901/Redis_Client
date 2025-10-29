package org.daodao.redis;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.HashSet;
import java.util.Set;
import java.util.List;

@Slf4j(topic = "RedisClusterClient")
public class RedisClusterClient {
    private static final String REDIS_URI = "redis://default@127.0.0.1:7001";
    private static final String REDIS_PASSWORD = "your pwd";
    private static  JedisCluster jedisCluster = null;

    public static void main(String[] args) {
        try {
            log.info("Enter  main");
           // create node sets
            Set<HostAndPort> nodes = new HashSet<>();
            nodes.add(new HostAndPort("127.0.0.1", 7001));
            nodes.add(new HostAndPort("127.0.0.1", 7002));
            nodes.add(new HostAndPort("127.0.0.1", 7003));
            nodes.add(new HostAndPort("127.0.0.1", 7004));
            nodes.add(new HostAndPort("127.0.0.1", 7005));
            nodes.add(new HostAndPort("127.0.0.1", 7006));
            // can add more nodes

            // create connection pool
            GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
            poolConfig.setMaxTotal(10); // maximum connection
            poolConfig.setMaxIdle(5);   // maximum idle connection
            poolConfig.setMinIdle(1);   // minimum idle connection
            poolConfig.setTestOnBorrow(true); // Test every time when borrow a connection

            // create JedisCluster
           jedisCluster = new JedisCluster(nodes, 10000, 2000, 2, REDIS_PASSWORD, poolConfig);

            // test connection
            log.info("Connected to Redis cluster，cluster size: " + jedisCluster.getClusterNodes().size());

//            String pattern ="test*";
//            scanKeys(pattern);

            // CRUD operation
            String key = "testKey";
            String value = "testValue";

            // create key value pair
            jedisCluster.set(key, value);
            log.info("Set key: " + key + ", value: " + value);

            // read key value pair
            String retrievedValue = jedisCluster.get(key);
            log.info("Get key: " + key + ", value: " + retrievedValue);

            // update key value pair
            String newValue = "updatedValue";
            jedisCluster.set(key, newValue);
            log.info("Updated key: " + key + ", new value: " + newValue);

            // delete key value pair
            jedisCluster.del(key);
            log.info("Deleted key: " + key);

        } catch (JedisException e) {
           log.error("Redis operation failed: ", e);
        } catch (Exception e) {
           log.error("Unexpected error: " , e);
        } finally {
            // close connection
            if (jedisCluster != null) {
                jedisCluster.close();
            }
        }
    }

    public static void scanKeys(String pattern){
        log.info("Start scanning keys...");
        String cursor = "0";
        ScanParams scanParams = new ScanParams().match(pattern).count(100); // 每次返回的key数量，可以根据需要调整
        do {
            ScanResult<String> scanResult = jedisCluster.scan(cursor, scanParams);
            cursor = scanResult.getCursor();
            List<String> keys = scanResult.getResult();
            for (String key : keys) {
                log.info("Key: " + key);
                // get value
                String value = jedisCluster.get(key);
                log.info("Value: " + value);
            }
        } while (!cursor.equals("0")); // end loop while cursor equals 0

    }
}