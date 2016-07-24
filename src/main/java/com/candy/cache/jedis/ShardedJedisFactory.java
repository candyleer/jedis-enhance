package com.candy.cache.jedis;

import java.util.List;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.util.Hashing;
import redis.clients.util.Pool;

/**
 * PoolableObjectFactory custom impl.
 */
public class ShardedJedisFactory implements PooledObjectFactory<ShardedJedis> {
    private List<JedisShardInfo> shards;
    private Hashing algo;
    private Pool<ShardedJedis> shardedJedisPool;

    public ShardedJedisFactory(List<JedisShardInfo> shards, Hashing algo, Pool<ShardedJedis> shardedJedisPool) {
        this.shards = shards;
        this.algo = algo;
        this.shardedJedisPool = shardedJedisPool;
    }

    public PooledObject<ShardedJedis> makeObject() throws Exception {
        ShardedJedis jedis = new ShardedJedis(shards, algo, null);
        jedis.setDataSource(shardedJedisPool);

        return new DefaultPooledObject<ShardedJedis>(jedis);
    }

    public void destroyObject(PooledObject<ShardedJedis> pooledShardedJedis) throws Exception {
        final ShardedJedis shardedJedis = pooledShardedJedis.getObject();
        for (Jedis jedis : shardedJedis.getAllShards()) {
            try {
                try {
                    jedis.quit();
                } catch (Exception e) {
                }
                jedis.disconnect();
            } catch (Exception e) {
            }
        }
    }

    public boolean validateObject(PooledObject<ShardedJedis> pooledShardedJedis) {
        try {
            ShardedJedis jedis = pooledShardedJedis.getObject();
            for (Jedis shard : jedis.getAllShards()) {
                if (!shard.ping().equals("PONG")) {
                    return false;
                }
            }
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    public void activateObject(PooledObject<ShardedJedis> p) throws Exception {

    }

    public void passivateObject(PooledObject<ShardedJedis> p) throws Exception {

    }
}