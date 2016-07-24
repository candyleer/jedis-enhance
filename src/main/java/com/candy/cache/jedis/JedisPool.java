package com.candy.cache.jedis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
/**
 * 将空字符串转换为null;
 * @author candyleer
 *
 */
public class JedisPool extends redis.clients.jedis.JedisPool {

	public JedisPool(GenericObjectPoolConfig poolConfig, String host, int port, int timeout, String password) {
		super(poolConfig, host, port, timeout, "".equals(password) ? null : password);
	}
}
