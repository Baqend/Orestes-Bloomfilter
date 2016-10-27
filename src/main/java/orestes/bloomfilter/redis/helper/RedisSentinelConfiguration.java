package orestes.bloomfilter.redis.helper;

import java.util.Collections;
import java.util.Set;

public class RedisSentinelConfiguration {
    private final String master;
    private final Set<String> sentinels;
    private final int timeout;

    private final String password;

    public RedisSentinelConfiguration(String master, Set<String> sentinels, int timeout) {
        this.master = master;
        this.timeout = timeout;
        this.password = null;
        this.sentinels = sentinels;
    }

    public RedisSentinelConfiguration(String master, Set<String> sentinels, int timeout, String password) {
        this.master = master;
        this.timeout = timeout;
        this.sentinels = sentinels;
        this.password = password;
    }

    String getMasterName() {
        return this.master;
    }

    Set<String> getSentinels() {
        return Collections.unmodifiableSet(this.sentinels);
    }

    int getTimeout() {
        return this.timeout;
    }

    String getPassword() {
        return this.password;
    }


}
