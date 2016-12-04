package com.candy.cache.jedis;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Pool;

/**
 * 获取redis链接；
 *
 * @author candyleer
 */
public class JedisSentinelPool extends Pool<Jedis> {
    private static final Logger logger = LoggerFactory.getLogger(JedisSentinelPool.class);

    private GenericObjectPoolConfig poolConfig;

    private int timeout = Protocol.DEFAULT_TIMEOUT;

    private String password;

    private HostAndPort currentHap;

    private static final String MASTER = "master";

    private static final String SLAVE = "slave";

    private String currentRole;

    /**
     * Sentinel监听器
     */
    private Set<MasterListener> masterListeners = new HashSet<MasterListener>();

    private final ReentrantLock reInitLock = new ReentrantLock();

    public JedisSentinelPool(String masterName, String sentinels) {
        this(masterName, sentinels, new GenericObjectPoolConfig(), Protocol.DEFAULT_TIMEOUT, MASTER, null);
    }

    public JedisSentinelPool(String masterName, String sentinels, String password) {
        this(masterName, sentinels, new GenericObjectPoolConfig(), Protocol.DEFAULT_TIMEOUT, MASTER, password);
    }

    public JedisSentinelPool(String masterName, String sentinels, final GenericObjectPoolConfig poolConfig) {
        this(masterName, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, MASTER, null);
    }

    public JedisSentinelPool(String masterName, String sentinels, final GenericObjectPoolConfig poolConfig, String role,
                             final int timeout) {
        this(masterName, sentinels, poolConfig, timeout, role, null);
    }

    public JedisSentinelPool(String masterName, String sentinels, final GenericObjectPoolConfig poolConfig, int timeout,
                             String role, final String password) {
        this.poolConfig = poolConfig;
        this.timeout = timeout;
        this.password = password;
        this.currentRole = role;

        logger.info("JedisPool master name " + masterName);

        String[] sentinelArray = sentinels.trim().split(",");
        Set<String> allSentinels = new HashSet<String>();
        for (String sentinel : sentinelArray) {
            allSentinels.add(sentinel.trim());
        }
        logger.info("JedisPool sentinel list " + allSentinels);

        HostAndPort hostAndPort = getAllMasterAddrFromSentinel(allSentinels, masterName);

        initPool(hostAndPort);
    }

    private void initPool(HostAndPort hap) {
        initPool(poolConfig, new JedisFactory(hap, timeout, password, this));
        logger.info("Created JedisPool  at [{}:{}]", hap.getHost(), hap.getPort());
        currentHap = hap;
    }

    private void initSlavePool(String sentinelHost, int sentinelPort, String masterName) {
        HostAndPort avalidHap = getAvalidHap(sentinelHost, sentinelPort, masterName, true);
        initPool(avalidHap);
        currentHap = avalidHap;
    }

    private HostAndPort getAvalidHap(String sentinelHost, int sentinelPort, String masterName, boolean slave) {
        Jedis jedis = null;
        try {
            jedis = new Jedis(sentinelHost, sentinelPort);
            if (slave) {
                List<Map<String, String>> sentinelSlaves = jedis.sentinelSlaves(masterName);
                Collections.shuffle(sentinelSlaves);
                for (Map<String, String> map : sentinelSlaves) {
                    if ("ok".equals(map.get("master-link-status"))) {
                        return new HostAndPort(map.get("ip"), Integer.valueOf(map.get("port")));
                    }
                }
            }
            // 确实找不到只有返回主master了；
            List<String> list = jedis.sentinelGetMasterAddrByName(masterName);
            return toHostAndPort(list);
        } catch (Exception e) {
            logger.error("someting error", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    private HostAndPort getAllMasterAddrFromSentinel(Set<String> sentinels, final String masterName) {
        if (sentinels == null || sentinels.size() <= 0 || masterName == null || masterName.trim().equals("")) {
            throw new IllegalArgumentException("'sentinels' & 'masters' must be not empty.");
        }

        logger.info("Trying to find all master from available Sentinels...");

        int sentinelRetry = 0;
        HostAndPort hostAndPort = null;
        boolean fetched = false;

		/*
      连接Sentinel的最大重试次数
	 */
        int MAX_RETRY_SENTINEL = 10;
        while (!fetched && sentinelRetry < MAX_RETRY_SENTINEL) {
            for (String sentinel : sentinels) {
                final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));
                logger.info("Connecting to Sentinel " + hap);
                Jedis jedis = null;
                try {
                    jedis = new Jedis(hap.getHost(), hap.getPort());
                    hostAndPort = getAvalidHap(hap.getHost(), hap.getPort(), masterName,
                            SLAVE.equals(currentRole));
                    if (hostAndPort != null) {
                        fetched = true;
                        break;
                    }
                } catch (JedisConnectionException e) {
                    logger.warn("Cannot connect to sentinel running @ " + hap + ". Trying next one.");
                } finally {
                    if (jedis != null) {
                        jedis.close();
                    }
                }
            }

            if (hostAndPort == null) {
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

            // Try MAX_RETRY_SENTINEL times.
            if (!fetched && sentinelRetry >= MAX_RETRY_SENTINEL) {
                logger.error("All sentinels down and try " + MAX_RETRY_SENTINEL + " times, Abort.");
                throw new JedisConnectionException("Cannot connect all sentinels, Abort.");
            }
            for (String sentinel : sentinels) {
                final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));
                MasterListener masterListener = new MasterListener(masterName, hap.getHost(), hap.getPort());
                masterListeners.add(masterListener);
                masterListener.start();
                logger.info("Starting Sentinel[" + sentinel + "] listener...");
            }
        }
        return hostAndPort;
    }

    private HostAndPort toHostAndPort(List<String> getMasterAddrByNameResult) {
        String host = getMasterAddrByNameResult.get(0);
        int port = Integer.parseInt(getMasterAddrByNameResult.get(1));
        return new HostAndPort(host, port);
    }

    public void destroy() {
        for (MasterListener m : masterListeners) {
            m.shutdown();
        }
        super.destroy();
    }

    /**
     * 监听主从切换；
     *
     * @author candy
     */
    private class MasterListener extends Thread {

        private String masterName;
        private String host;
        private int port;
        private long subscribeRetryWaitTimeMillis = 3000L;
        private Jedis jedis;
        private AtomicBoolean running = new AtomicBoolean(false);

        public MasterListener(String masterName, String host, int port) {
            super("MasterListener-on-" + host + ":" + port);
            this.masterName = masterName;
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
                                if (masterName.equals(switchMasterMsg[0])) {
                                    try {
                                        reInitLock.lock();

                                        HostAndPort newHostMaster = toHostAndPort(
                                                Arrays.asList(switchMasterMsg[3], switchMasterMsg[4]));
                                        if (SLAVE.equals(currentRole)) {
                                            initSlavePool(host, port, masterName);
                                        } else {
                                            initPool(newHostMaster);

                                        }

                                        logger.warn("SentinelSwitched M/S of [" + message
                                                + "](from > to), completed reInitPool.");

                                    } finally {
                                        reInitLock.unlock();
                                    }
                                } else {
                                    logger.warn(
                                            "Ignoring message on +switch-master for master name " + switchMasterMsg[0]);
                                }
                            } else {
                                logger.warn("Invalid message received on Sentinel " + host + ":" + port
                                        + " on channel +switch-master: " + message);
                            }
                        }
                    }, "+switch-master");
                } catch (JedisConnectionException e) {
                    if (running.get()) {
                        logger.error("Lost connection to Sentinel at " + host + ":" + port
                                + ". Sleeping 3000ms and retrying.");
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

    public GenericObjectPoolConfig getPoolConfig() {
        return poolConfig;
    }

    public void setPoolConfig(GenericObjectPoolConfig poolConfig) {
        this.poolConfig = poolConfig;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public HostAndPort getCurrentHap() {
        return currentHap;
    }
}