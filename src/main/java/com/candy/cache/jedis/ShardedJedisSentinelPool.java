package com.candy.cache.jedis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Hashing;
import redis.clients.util.Pool;

/**
 * 支持客户端主从切换和shard的jedis连接池实现
 * 
 * @author candy
 *
 */
public class ShardedJedisSentinelPool extends Pool<ShardedJedis> {
    private static final Logger logger = LoggerFactory.getLogger(ShardedJedisSentinelPool.class);

    /**
     * 连接Sentinel的最大重试次数
     */
    private final int MAX_RETRY_SENTINEL = 10;

    private GenericObjectPoolConfig poolConfig;

    private int timeout = Protocol.DEFAULT_TIMEOUT;

    private String password;

    /**
     * Sentinel监听器
     */
    private Set<MasterListener> masterListeners = new HashSet<MasterListener>();

    private final ReentrantLock reInitLock = new ReentrantLock();

    /**
     * 当前所有的master地址
     */
    private volatile List<HostAndPort> currentHostMasters;

    public ShardedJedisSentinelPool(String masterNames, String sentinels) {
        this(masterNames, sentinels, new GenericObjectPoolConfig(), Protocol.DEFAULT_TIMEOUT, null);
    }

    public ShardedJedisSentinelPool(String masterNames, String sentinels, String password) {
        this(masterNames, sentinels, new GenericObjectPoolConfig(), Protocol.DEFAULT_TIMEOUT, password);
    }

    public ShardedJedisSentinelPool(String masterNames, String sentinels, final GenericObjectPoolConfig poolConfig) {
        this(masterNames, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, null);
    }

    public ShardedJedisSentinelPool(String masterNames, String sentinels, final GenericObjectPoolConfig poolConfig,
            final int timeout) {
        this(masterNames, sentinels, poolConfig, timeout, null);
    }

    public ShardedJedisSentinelPool(String masterNames, String sentinels, final GenericObjectPoolConfig poolConfig, int timeout,
            final String password) {
        this.poolConfig = poolConfig;
        this.timeout = timeout;
        this.password = password;

        String[] masterNamesArray = masterNames.trim().split(",");
        List<String> masterNameList = new ArrayList<String>();
        for (String masterName : masterNamesArray) {
            masterNameList.add(masterName.trim());
        }
        logger.info("ShardedJedisPool master name list " + masterNameList);

        String[] sentinelArray = sentinels.trim().split(",");
        Set<String> allSentinels = new HashSet<String>();
        for (String sentinel : sentinelArray) {
            allSentinels.add(sentinel.trim());
        }
        logger.info("ShardedJedisPool sentinel list " + allSentinels);

        List<HostAndPort> masterAddrList = getAllMasterAddrFromSentinel(allSentinels, masterNameList);
        initPool(masterAddrList);
    }

    public void destroy() {
        for (MasterListener m : masterListeners) {
            m.shutdown();
        }

        super.destroy();
    }

    public List<HostAndPort> getCurrentHostMaster() {
        return currentHostMasters;
    }

    private void initPool(List<HostAndPort> masters) {
        if (!equalsList(currentHostMasters, masters)) {
            StringBuffer sb = new StringBuffer();
            for (HostAndPort master : masters) {
                sb.append(master.toString());
                sb.append(" ");
            }

            List<JedisShardInfo> shardMasters = makeShardInfoList(masters);

            initPool(poolConfig, new ShardedJedisFactory(shardMasters, Hashing.MURMUR_HASH, this));
            currentHostMasters = masters;

            logger.info("Created ShardedJedisPool to master at [" + sb.toString() + "]");
        }
    }

    private boolean equalsList(List<HostAndPort> currentShardMasters, List<HostAndPort> shardMasters) {
        if (currentShardMasters != null && shardMasters != null) {
            if (currentShardMasters.size() == shardMasters.size()) {
                for (int i = 0; i < currentShardMasters.size(); i++) {
                    if (!currentShardMasters.get(i).equals(shardMasters.get(i)))
                        return false;
                }
                return true;
            }
        }
        return false;
    }

    private List<JedisShardInfo> makeShardInfoList(List<HostAndPort> masters) {
        List<JedisShardInfo> shardMasters = new ArrayList<JedisShardInfo>();
        for (HostAndPort master : masters) {
            JedisShardInfo jedisShardInfo = new JedisShardInfo(master.getHost(), master.getPort(), timeout);
            jedisShardInfo.setPassword(password);

            shardMasters.add(jedisShardInfo);
        }
        return shardMasters;
    }

    private List<HostAndPort> getAllMasterAddrFromSentinel(Set<String> sentinels, final List<String> masters) {
        if (sentinels == null || sentinels.size() <= 0 || masters == null || masters.size() <= 0) {
            throw new IllegalArgumentException("'sentinels' & 'masters' must be not empty.");
        }

        Map<String, HostAndPort> masterMap = new HashMap<String, HostAndPort>();
        List<HostAndPort> shardMasters = new ArrayList<HostAndPort>();

        logger.info("Trying to find all master from available Sentinels...");

        int sentinelRetry = 0;
        for (String masterName : masters) {
            HostAndPort master = null;
            boolean fetched = false;

            while (!fetched && sentinelRetry < MAX_RETRY_SENTINEL) {
                for (String sentinel : sentinels) {
                    final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));

                    logger.info("Connecting to Sentinel " + hap);

                    Jedis jedis = null;
                    try {
                        jedis = new Jedis(hap.getHost(), hap.getPort());
                        master = masterMap.get(masterName);
                        if (master == null) {
                            List<String> hostAndPort = jedis.sentinelGetMasterAddrByName(masterName);
                            if (hostAndPort != null && hostAndPort.size() > 0) {
                                master = toHostAndPort(hostAndPort);

                                logger.info("Found Redis master at " + master);

                                shardMasters.add(master);
                                masterMap.put(masterName, master);
                                fetched = true;
                                jedis.disconnect();
                                break;
                            }
                        }
                    } catch (JedisConnectionException e) {
                        logger.warn("Cannot connect to sentinel running @ " + hap + ". Trying next one.");
                    } finally {
                        if (jedis != null) {
                            jedis.close();
                        }
                    }
                }

                if (master == null) {
                    try {
                        logger.error("All sentinels down, cannot determine where is " + masterName
                                + " master is running... sleeping 1000ms, Will try again.");
                        Thread.sleep(1000L);
                    } catch (InterruptedException e) {
                        logger.error("InterruptedException", e);
                    }

                    fetched = false;
                    sentinelRetry++;
                }
            }

            // Try MAX_RETRY_SENTINEL times.
            if (!fetched && sentinelRetry >= MAX_RETRY_SENTINEL) {
                logger.error("All sentinels down and try " + MAX_RETRY_SENTINEL + " times, Abort.");
                throw new JedisConnectionException("Cannot connect all sentinels, Abort.");
            }
        }

        // All shards master must been accessed.
        if (masters.size() == shardMasters.size()) {
            for (String sentinel : sentinels) {
                final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));
                MasterListener masterListener = new MasterListener(masters, hap.getHost(), hap.getPort());
                masterListeners.add(masterListener);
                masterListener.start();
                logger.info("Starting Sentinel[" + sentinel + "] listener...");
            }
        } else {
            throw new JedisConnectionException("Not all shards master can be accessed. Except " + masters.size()
                    + " masters, but " + shardMasters.size() + " masters can be accessed");
        }

        return shardMasters;
    }

    private HostAndPort toHostAndPort(List<String> getMasterAddrByNameResult) {
        String host = getMasterAddrByNameResult.get(0);
        int port = Integer.parseInt(getMasterAddrByNameResult.get(1));

        return new HostAndPort(host, port);
    }

    private class MasterListener extends Thread {

        private List<String> masters;
        private String host;
        private int port;
        private long subscribeRetryWaitTimeMillis = 3000L;
        private Jedis jedis;
        private AtomicBoolean running = new AtomicBoolean(false);

        public MasterListener(List<String> masters, String host, int port) {
            super("MasterListener-on-" + host + ":" + port);
            this.masters = masters;
            this.host = host;
            this.port = port;
        }

        public void run() {
            running.set(true);

            while (running.get()) {

                jedis = new Jedis(host, port);

                try {
                    jedis.subscribe(new JedisPubSub() {
                        @Override
                        public void onMessage(String channel, String message) {
                            logger.info("Sentinel " + host + ":" + port + " published: " + message + ".");

                            String[] switchMasterMsg = message.split(" ");

                            if (switchMasterMsg.length > 3) {
                                int index = masters.indexOf(switchMasterMsg[0]);
                                if (index >= 0) {
                                    try {
                                        reInitLock.lock();

                                        HostAndPort newHostMaster = toHostAndPort(Arrays.asList(switchMasterMsg[3],
                                                switchMasterMsg[4]));
                                        List<HostAndPort> newHostMasters = new ArrayList<HostAndPort>();

                                        for (int i = 0; i < masters.size(); i++) {
                                            newHostMasters.add(null);
                                        }

                                        Collections.copy(newHostMasters, currentHostMasters);
                                        newHostMasters.set(index, newHostMaster);

                                        if (!equalsList(newHostMasters, currentHostMasters)) {
                                            initPool(newHostMasters);

                                            logger.warn("SentinelSwitched M/S of [" + message
                                                    + "](from > to), completed reInitPool.");
                                        }
                                    } finally {
                                        reInitLock.unlock();
                                    }
                                } else {
                                    StringBuffer sb = new StringBuffer();
                                    for (String masterName : masters) {
                                        sb.append(masterName);
                                        sb.append(",");
                                    }
                                    logger.warn("Ignoring message on +switch-master for master name " + switchMasterMsg[0]
                                            + ", our monitor master name are [" + sb + "]");
                                }
                            } else {
                                logger.warn("Invalid message received on Sentinel " + host + ":" + port
                                        + " on channel +switch-master: " + message);
                            }
                        }
                    }, "+switch-master");
                } catch (JedisConnectionException e) {
                    if (running.get()) {
                        logger.error("Lost connection to Sentinel at " + host + ":" + port + ". Sleeping 3000ms and retrying.");
                        try {
                            Thread.sleep(subscribeRetryWaitTimeMillis);
                        } catch (InterruptedException e1) {
                            logger.error("InterruptedException", e1);
                        }
                    } else {
                        logger.warn("Unsubscribing from Sentinel at " + host + ":" + port);
                    }
                }
            }
        }

        public void shutdown() {
            try {
                logger.info("Shutting down listener on " + host + ":" + port);
                running.set(false);
                // This isn't good, the Jedis object is not thread safe
                jedis.disconnect();
            } catch (Exception e) {
                logger.error("Caught exception while shutting down", e);
            }
        }
    }
}