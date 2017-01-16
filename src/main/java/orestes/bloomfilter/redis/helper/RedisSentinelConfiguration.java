package orestes.bloomfilter.redis.helper;

import java.util.Collections;
import java.util.Set;

import redis.clients.jedis.Protocol;

public class RedisSentinelConfiguration {
    private final String master;
    private final Set<String> sentinels;
    private final int timeout;
    private final String password;
    private final int database;

    public RedisSentinelConfiguration(String master, Set<String> sentinels, int timeout) {
        this(master, sentinels, timeout, null);
    }

    public RedisSentinelConfiguration(String master, Set<String> sentinels, int timeout, String password) {
        this(master, sentinels, timeout, password, Protocol.DEFAULT_DATABASE);
    }

    public RedisSentinelConfiguration(String master, Set<String> sentinels, int timeout, String password, int database) {
        this.master = master;
        this.timeout = timeout;
        this.sentinels = sentinels;
        this.password = password;
        this.database = database;
    }

    private RedisSentinelConfiguration(Builder builder) {
        this.master = builder.master;
        this.sentinels = builder.sentinels;
        this.timeout = builder.timeout;
        this.password = builder.password;
        this.database = builder.database;
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

    public int getDatabase() {
        return database;
    }

    /**
     * Creates builder to build {@link RedisSentinelConfiguration}.
     * @return created builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder to build {@link RedisSentinelConfiguration}.
     */
    public static final class Builder {
        private String master;
        private Set<String> sentinels;
        private int timeout;
        private String password;
        private int database;

        private Builder() {
        }

        public Builder master(String master) {
            this.master = master;
            return this;
        }

        public Builder sentinels(Set<String> sentinels) {
            this.sentinels = sentinels;
            return this;
        }

        public Builder timeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder database(int database) {
            this.database = database;
            return this;
        }

        public RedisSentinelConfiguration build() {
            return new RedisSentinelConfiguration(this);
        }
    }
}
