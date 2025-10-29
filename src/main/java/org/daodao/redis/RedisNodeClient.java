package org.daodao.redis;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

@Slf4j
public class RedisNodeClient {
    private static final String REDIS_HOST = "127.0.0.1";
    private static final int REDIS_PORT = 7001;
    private static final String REDIS_PASSWORD = "your pwd";


    public static void main(String[] args) {
        log.info("enter  main");
        try (Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
            // authentication
            jedis.auth(REDIS_PASSWORD);

            // test connection
            log.info("Connected to Redis node: " + jedis.ping());

            // CRUD operation
            String key = "testKey";
            String value = "testValue";

            // create key value pair
            jedis.set(key, value);
            log.info("Set key: " + key + ", value: " + value);

            // read key value pair
            String retrievedValue = jedis.get(key);
            log.info("Get key: " + key + ", value: " + retrievedValue);

            // update key value pair
            String newValue = "updatedValue";
            jedis.set(key, newValue);
            log.info("Updated key: " + key + ", new value: " + jedis.get(key));

            // delete key value pair
            jedis.del(key);

            jedis.close();
            log.info("Deleted key: " + key);
        } catch (JedisException e) {
            log.error("Redis operation failed:", e);
        } catch (Exception e) {
            log.error("Unexpected error: ", e);
        }
    }
}