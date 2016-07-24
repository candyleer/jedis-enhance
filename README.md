##  jedis-enhance

jedis 功能强大,但是内部缺少对主从哨兵配置的读写分离简单配置,jedis-enhance支持
1. 一主多重的哨兵配置,可以指定获取role为master或者slave的jedis.
2. 支持哨兵模式shard redis分片配置;
- first git clone 

```
git clone https://github.com/candyleer/jedis-enhance.git

```
- maven build

```
maven clean install -Dmaven.test.skip=true

```
- maven pom config

```
<dependency>
    <groupId>com.candy.cache</groupId>
    <artifactId>candy-jedis</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>

```
- spring config

```
<bean id="jedisSentinelPool" class="com.candy.cache.jedis.JedisSentinelPool">
	<constructor-arg name="masterName" value="test"/>
	<constructor-arg name="sentinels" value="127.0.0.1:26279,127.0.0.1:36379" /> 
	<constructor-arg name="poolConfig" ref="poolConfig" />
	<constructor-arg name="role" value="slave" />
	<constructor-arg name="timeout" value="5000" />
</bean>
```