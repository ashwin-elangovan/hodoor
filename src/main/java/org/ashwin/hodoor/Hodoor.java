package org.ashwin.hodoor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class Hodoor {
    private JedisPool jedisPool;
    private TimeUnit timeUnit;
    private int permitsPerUnit;

    // Lua script for rate limiting in seconds
    private static final String LUA_SECOND_SCRIPT =
            " local current = redis.call('incr',KEYS[1]); "
            + " if tonumber(current) == 1 then "
            + " -- If the counter is 1, set the expiration time "
            + "     redis.call('expire',KEYS[1],ARGV[1]); "
            + "     return 1; "
            + " else"
            + "     if tonumber(current) <= tonumber(ARGV[2]) then "
            + "         return 1; "
            + "     else "
            + "         return -1; "
            + "     end "
            + " end ";

    // Lua script for rate limiting in a specified time unit
    private static final String LUA_PERIOD_SCRIPT =
            " local currentSectionCount = redis.call('zcount', KEYS[2], '-inf', '+inf'); " +
                    " local previousSectionCount = redis.call('zcount', KEYS[1], ARGV[3], '+inf'); " +
                    " local totalCountInPeriod = tonumber(currentSectionCount) + tonumber(previousSectionCount); " +
                    " if totalCountInPeriod < tonumber(ARGV[5]) then " +
                    "     redis.call('zadd', KEYS[2], ARGV[1], ARGV[2]); " +
                    "     if tonumber(currentSectionCount) == 0 then " +
                    "         redis.call('expire', KEYS[2], ARGV[4]); " +
                    "     end " +
                    "     return 1; " +
                    " else " +
                    "     return -1; " +
                    " end ";


    private static final int PERIOD_SECOND_TTL = 10;
    private static final int PERIOD_MINUTE_TTL = 2 * 60 + 10;
    private static final int PERIOD_HOUR_TTL = 2 * 3600 + 10;
    private static final int PERIOD_DAY_TTL = 2 * 3600 * 24 + 10;

    private static final long MICROSECONDS_IN_MINUTE = 60 * 1000000L;
    private static final long MICROSECONDS_IN_HOUR = 3600 * 1000000L;
    private static final long MICROSECONDS_IN_DAY = 24 * 3600 * 1000000L;

    public Hodoor(JedisPool jedisPool, TimeUnit timeUnit, int permitsPerUnit) {
        this.jedisPool = jedisPool;
        this.timeUnit = timeUnit;
        this.permitsPerUnit = permitsPerUnit;
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public int getPermitsPerUnit() {
        return permitsPerUnit;
    }

    public boolean acquire(String keyPrefix) {
        // Initialize the return value to false
        boolean rtv = false;

        // Check if the JedisPool is not null
        if (jedisPool != null) {
            Jedis jedis = null;
            try {
                // Get a Jedis resource from the pool
                jedis = jedisPool.getResource();

                // If the time unit is in seconds
                if (timeUnit == TimeUnit.SECONDS) {
                    // Generate the key name for the current second
                    String keyName = keyPrefix + ":" + jedis.time().get(0);

                    // Prepare a list of keys and arguments for Lua script
                    List<String> keys = new ArrayList<String>();
                    keys.add(keyName);

                    List<String> argvs = new ArrayList<String>();
                    argvs.add(String.valueOf(getExpire())); // Expiration time
                    argvs.add(String.valueOf(permitsPerUnit)); // Permits per unit

                    // Execute the Lua script for rate limiting in seconds
                    Long val = (Long) jedis.eval(LUA_SECOND_SCRIPT, keys, argvs);

                    // Update the return value based on the Lua script result
                    rtv = (val > 0);

                } else if (timeUnit == TimeUnit.MINUTES || timeUnit == TimeUnit.HOURS || timeUnit == TimeUnit.DAYS) {
                    // If the time unit is in minutes, hours, or days, delegate to doPeriod method
                    rtv = doPeriod(jedis, keyPrefix);
                }
            } finally {
                // Close the Jedis resource in a finally block to ensure proper resource management
                if (jedis != null) {
                    jedis.close();
                }
            }
        }

        // Return the final result of the rate limiting operation
        return rtv;
    }

    private boolean doPeriod(Jedis jedis, String keyPrefix) {
        // Get the current time from Redis in seconds and microseconds
        List<String> jedisTime = jedis.time();
        long currentSecond = Long.parseLong(jedisTime.get(0));
        long microSecondsElapseInCurrentSecond = Long.parseLong(jedisTime.get(1));

        // Calculate the current time in microseconds
        long currentTimeInMicroseconds = currentSecond * 1_000_000 + microSecondsElapseInCurrentSecond;

        // Calculate the score for the previous time window
        long previousSectionBeginScore = currentTimeInMicroseconds - getPeriodMicrosecond();

        // Get the expiration time as a string
        String expires = String.valueOf(getExpire());

        // Prepare the key names for the current time window
        String[] keyNames = getKeyNames(currentSecond, keyPrefix);

        // Prepare the list of keys
        List<String> keys = List.of(keyNames);

        // Prepare the list of arguments for the Lua script
        List<String> argvs = List.of(
                String.valueOf(currentTimeInMicroseconds),
                String.valueOf(currentTimeInMicroseconds),
                String.valueOf(previousSectionBeginScore),
                expires,
                String.valueOf(permitsPerUnit)
        );

        // Execute Lua script using Redis eval
        Long val = (Long) jedis.eval(LUA_PERIOD_SCRIPT, keys, argvs);

        // Return true if permits are available, otherwise false
        return val > 0;
    }

    private String[] getKeyNames(long currentSecond, String keyPrefix) {
        String[] keyNames = null;
        long index;

        switch (timeUnit) {
            case MINUTES:
                index = currentSecond / 60;
                break;
            case HOURS:
                index = currentSecond / 3600;
                break;
            case DAYS:
                index = currentSecond / (3600 * 24);
                break;
            default:
                throw new IllegalArgumentException("Don't support this TimeUnit: " + timeUnit);
        }

        String keyName1 = keyPrefix + ":" + (index - 1);
        String keyName2 = keyPrefix + ":" + index;
        keyNames = new String[]{keyName1, keyName2};

        return keyNames;
    }

    private int getExpire() {
        int expire = 0;

        switch (timeUnit) {
            case SECONDS:
                expire = PERIOD_SECOND_TTL;
                break;
            case MINUTES:
                expire = PERIOD_MINUTE_TTL;
                break;
            case HOURS:
                expire = PERIOD_HOUR_TTL;
                break;
            case DAYS:
                expire = PERIOD_DAY_TTL;
                break;
            default:
                throw new IllegalArgumentException("Don't support this TimeUnit: " + timeUnit);
        }

        return expire;
    }

    private long getPeriodMicrosecond() {
        switch (timeUnit) {
            case MINUTES:
                return MICROSECONDS_IN_MINUTE;
            case HOURS:
                return MICROSECONDS_IN_HOUR;
            case DAYS:
                return MICROSECONDS_IN_DAY;
            default:
                throw new IllegalArgumentException("Don't support this TimeUnit: " + timeUnit);
        }
    }
}
