package com.ctrip.fx.octopus.server.redis;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.ctrip.fx.octopus.model.TaskTypes;
import com.ctrip.fx.octopus.server.Context;
import com.ctrip.fx.octopus.util.Conf;
import com.ctrip.fx.octopus.util.Numbers;
import com.ctrip.fx.octopus.util.redis.JedisPools;

public class RedisAccessor {
	static volatile int defaultTimeout = 600000, defaultExpire = 86400000;
	static volatile HashMap<Integer, Integer> timeoutMap = new HashMap<>();
	static volatile HashMap<Integer, Integer> expireMap = new HashMap<>();

	static JedisPools pools;
	static int POOL_NUM;

	static {
		Properties p = Conf.load("Redis");

		pools = new JedisPools(p.getProperty("pools"));
		POOL_NUM = pools.getPoolNum();
		defaultTimeout = Numbers.parseInt(p.getProperty("timeout"), 600000);
		defaultExpire = Numbers.parseInt(p.getProperty("expire"), 86400000);

		for (Map.Entry<?, ?> entry : p.entrySet()) {
			String key = (String) entry.getKey();
			String value = (String) entry.getValue();
			if (key.startsWith("timeout.")) {
				String typeName = key.substring(8);
				timeoutMap.put(Integer.valueOf(TaskTypes.parseType(typeName)),
						Integer.valueOf(Numbers.parseInt(value)));
				continue;
			}
			if (key.startsWith("expire.")) {
				String typeName = key.substring(7);
				expireMap.put(Integer.valueOf(TaskTypes.parseType(typeName)),
						Integer.valueOf(Numbers.parseInt(value)));
				continue;
			}
			// Other Configures ...
		}

		// If multiple servers share Redis pools, only server 0 need to schedule
		if (Numbers.parseInt(p.getProperty("server_id")) == 0) {
			Context.schedule(new Runnable() {
				@Override
				public void run() {
					final long now = System.currentTimeMillis();
					pools.invokeAll(new TaskSchedule(now));
					pools.invoke(0, new JobSchedule(now));
				}
			}, 0, 10000);
		}
	}

	public static JedisPools getPools() {
		return pools;
	}

	@Deprecated
	public static int getPoolNum() {
		return pools.getPoolNum();
	}

	@Deprecated
	public static void invoke(int poolId, Invocable invocable) {
		pools.invoke(poolId, invocable);
	}
}