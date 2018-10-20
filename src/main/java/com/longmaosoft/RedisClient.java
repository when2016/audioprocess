package com.longmaosoft;

import org.apache.commons.lang.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.*;

public class RedisClient {
    private static final long TSL_20140217 = 1392566400000L;
    private static String host = "127.0.0.1";
    private static int port = 6379;
    private static String password = null;
    private static JedisPool pool = new JedisPool(new JedisPoolConfig(), host, port, 2000, password);

    public static void addToList(String key, String... members) {
        addToList(0L, key, members);
    }

    /**
     * 增加到redis的list列表中去
     *
     * @param tsl     时间戳，为了计算list的score，一般是feed的created
     * @param key     list key
     * @param members list values
     */
    public static void addToList(final long tsl, final String key, final String... members) {
        execute(new RedisCallback<String>() {
            @Override
            public String execute(Jedis jedis) {
                Map<String, Double> scoreMembers = new HashMap<String, Double>();
                long time = (tsl <= 0 ? System.currentTimeMillis() : tsl) - TSL_20140217;

                for (String member : members) {
                    double key = new Long(time++).doubleValue() / 1000;
                    scoreMembers.put(member, key);
                }
                Long ret = jedis.zadd(key, scoreMembers);
                if (ret != members.length) {
                    //redisLogger.error("addToList ret:" + ret + " key:" + key + "," + Arrays.toString(members));
                    System.out.println("addToList ret:" + ret + " key:" + key + "," + Arrays.toString(members));
                }
                return null;
            }
        });
    }


    public static void removeFromList(final String key, final String... members) {
        execute(new RedisCallback<String>() {
            @Override
            public String execute(Jedis jedis) {
                Long ret = jedis.zrem(key, members);
                if (ret != members.length) {
                    //redisLogger.error("removeFromList ret:" + ret + " key:" + key + "," + Arrays.toString(members));
                    System.out.println("removeFromList ret:" + ret + " key:" + key + "," + Arrays.toString(members));
                }
                return null;
            }
        });
    }

    public static Set<String> listFromList(final String key) {
        return execute(new RedisCallback<Set<String>>() {
            @Override
            public Set<String> execute(Jedis jedis) {
                return jedis.zrange(key, 0, -1);
            }
        });
    }

    public static Set<String> listFromListByRevRange(final String key) {
        return execute(new RedisCallback<Set<String>>() {
            @Override
            public Set<String> execute(Jedis jedis) {
                return jedis.zrevrange(key, 0, -1);
            }
        });
    }

    public static Set<String> listFromListByRevRangeAndPage(final String key, final int page, final int pageSize) {
        return execute(new RedisCallback<Set<String>>() {
            @Override
            public Set<String> execute(Jedis jedis) {
                int start = (page - 1) * pageSize;
                start = start < 0 ? 0 : start;
                return jedis.zrevrange(key, start, start + pageSize - 1);
            }
        });
    }

    public static Set<String> listByScoreWithPagination(final String key, final double min, final double max, final int page, final int pageSize) {
        return execute(new RedisCallback<Set<String>>() {
            @Override
            public Set<String> execute(Jedis jedis) {
                //后面2个参数是offset and count
                int start = (page - 1) * pageSize;
                start = start < 0 ? 0 : start;
                return jedis.zrevrangeByScore(key, max <= 0 ? Double.MAX_VALUE : max, min, start, pageSize);
            }
        });
    }

    public static Long count(final String key) {
        return execute(new RedisCallback<Long>() {
            @Override
            public Long execute(Jedis jedis) {
                return jedis.zcard(key);
            }
        });
    }

    public static Long incr(final String key) {
        return execute(new RedisCallback<Long>() {
            @Override
            public Long execute(Jedis jedis) {
                return jedis.incr(key);
            }
        });
    }

    public static Long incr(final String key, final long value) {
        return execute(new RedisCallback<Long>() {
            @Override
            public Long execute(Jedis jedis) {
                return jedis.incrBy(key, value);
            }
        });
    }

    public static void set(final String key, final String value) {
        execute(new RedisCallback<String>() {
            @Override
            public String execute(Jedis jedis) {
                return jedis.set(key, value);
            }
        });
    }

    public static void rpush(final String key, final String... values) {
        execute(new RedisCallback<Long>() {
            @Override
            public Long execute(Jedis jedis) {
                return jedis.rpush(key, values);
            }
        });
    }

    public static String lpop(final String key) {
        return execute(new RedisCallback<String>() {
            @Override
            public String execute(Jedis jedis) {
                return jedis.lpop(key);
            }
        });
    }

    public static Long llen(final String key) {
        return execute(new RedisCallback<Long>() {
            @Override
            public Long execute(Jedis jedis) {
                return jedis.llen(key);
            }
        });
    }

    public static void pfadd(final String key, final String value) {
        execute(new RedisCallback<Long>() {
            @Override
            public Long execute(Jedis jedis) {
                return jedis.pfadd(key, value);
            }
        });
    }

    public static void setBit(final String key, final long offset, final boolean value) {
        execute(new RedisCallback<String>() {
            @Override
            public String execute(Jedis jedis) {
                jedis.setbit(key, offset, value);
                return null;
            }
        });
    }

    public static String get(final String key) {
        return execute(new RedisCallback<String>() {
            @Override
            public String execute(Jedis jedis) {
                return jedis.get(key);
            }
        });
    }

    public static void add(final String key, final String... members) {
        execute(new RedisCallback<String>() {
            @Override
            public String execute(Jedis jedis) {
                Long ret = jedis.sadd(key, members);
                if (ret != members.length) {
                    //redisLogger.error("add ret:" + ret + " key:" + key + "," + Arrays.toString(members));
                    System.out.println("add ret:" + ret + " key:" + key + "," + Arrays.toString(members));
                }
                return null;
            }
        });
    }

    public static Boolean ismember(final String key, final String member) {
        execute(new RedisCallback() {
            @Override
            public Boolean execute(Jedis jedis) {
                return jedis.sismember(key, member);
            }
        });
        return false;
    }

    public static Long len(final String key) {
        return execute(new RedisCallback<Long>() {
            @Override
            public Long execute(Jedis jedis) {
                return jedis.scard(key);
            }
        });
    }

    public static Long hset(final String key, final String field, final String value) {
        return execute(new RedisCallback<Long>() {
            @Override
            public Long execute(Jedis jedis) {
                return jedis.hset(key, field, value);
            }
        });
    }

    public static String hmset(final String key, final Map<String, String> hash) {
        return execute(new RedisCallback<String>() {
            @Override
            public String execute(Jedis jedis) {
                return jedis.hmset(key, hash);
            }
        });
    }

    public static String hget(final String key, final String field) {
        return execute(new RedisCallback<String>() {
            @Override
            public String execute(Jedis jedis) {
                return jedis.hget(key, field);
            }
        });
    }

    public static List<String> hmget(final String key, final String... fields) {
        return execute(new RedisCallback<List<String>>() {
            @Override
            public List<String> execute(Jedis jedis) {
                return jedis.hmget(key, fields);
            }
        });
    }

    public static Long hdel(final String key, final String field) {
        return execute(new RedisCallback<Long>() {
            @Override
            public Long execute(Jedis jedis) {
                return jedis.hdel(key, field);
            }
        });
    }

    public static Long hincr(final String key, final String field, final long value) {
        return execute(new RedisCallback<Long>() {
            @Override
            public Long execute(Jedis jedis) {
                return jedis.hincrBy(key, field, value);
            }
        });
    }

    public static Map<String, String> hgetall(final String key) {
        return execute(new RedisCallback<Map<String, String>>() {
            @Override
            public Map<String, String> execute(Jedis jedis) {
                return jedis.hgetAll(key);
            }
        });
    }

    public static List<String> hvals(final String key) {
        return execute(new RedisCallback<List<String>>() {
            @Override
            public List<String> execute(Jedis jedis) {
                return jedis.hvals(key);
            }
        });
    }

    public static void delete(final String key) {
        execute(new RedisCallback<String>() {
            @Override
            public String execute(Jedis jedis) {
                Long ret = jedis.del(key);
                if (ret != 1) {
                    //redisLogger.error("delete ret:" + ret + " key:" + key);
                    System.out.println("delete ret:" + ret + " key:" + key);
                }
                return null;
            }
        });
    }

    public static void remove(final String key, final String... members) {
        execute(new RedisCallback<String>() {
            @Override
            public String execute(Jedis jedis) {
                Long ret = jedis.srem(key, members);
                if (ret != members.length) {
                    //redisLogger.error("remove ret:" + ret + " key:" + key + "," + Arrays.toString(members));
                    System.out.println("remove ret:" + ret + " key:" + key + "," + Arrays.toString(members));
                }
                return null;
            }
        });
    }

    public static Set<String> list(final String key) {
        return execute(new RedisCallback<Set<String>>() {
            @Override
            public Set<String> execute(Jedis jedis) {
                return jedis.smembers(key);
            }
        });
    }


    public static void addToMap(final String key, final String field, final String value) {
        execute(new RedisCallback<String>() {
            @Override
            public String execute(Jedis jedis) {
                jedis.hset(key, field, value);
                return StringUtils.EMPTY;
            }
        });
    }

    public static Map<String, String> map(final String key) {
        return execute(new RedisCallback<Map<String, String>>() {
            @Override
            public Map<String, String> execute(Jedis jedis) {
                return jedis.hgetAll(key);
            }
        });
    }

    public static String getFromMap(final String key, final String field) {
        return execute(new RedisCallback<String>() {
            @Override
            public String execute(Jedis jedis) {
                return jedis.hget(key, field);
            }
        });
    }

    public static List<String> multiGet(final String... keys) {
        return execute(new RedisCallback<List<String>>() {
            @Override
            public List<String> execute(Jedis jedis) {
                return jedis.mget(keys);
            }
        });
    }

    public static Long expire(final String key, final int seconds) {
        return execute(new RedisCallback<Long>() {
            @Override
            public Long execute(Jedis jedis) {
                return jedis.expire(key, seconds);
            }
        });
    }

    public static void shutdown() {
        pool.destroy();
    }

    private static <T> T execute(RedisCallback<T> callback) {
        Jedis jedis = pool.getResource();
        try {
            if (callback != null) {
                return callback.execute(jedis);
            }
        } catch (JedisConnectionException e) {
            if (null != jedis) {
                jedis.close();
                jedis = null;
            }
        } finally {
            if (null != jedis)
                jedis.close();
        }
        return null;
    }

    public static interface RedisCallback<T> {
        T execute(Jedis jedis);
    }
}
