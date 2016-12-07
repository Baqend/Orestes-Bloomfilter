package orestes.bloomfilter.redis.helper;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Protocol;

public class RedisStandaloneConfiguration {
    private static final String NO_PASSWORD = null;
    private static final boolean NO_SSL = false;
    private static final Set<Map.Entry<String, Integer>> NO_READ_SLAVES = Collections.emptySet();

    private final String host;
    private final int port;
    private Set<Map.Entry<String, Integer>> readSlaves;
    private final String password;
    private final boolean ssl;
    private final int database;

    public RedisStandaloneConfiguration(String host, int port) {
        this(host, port, NO_PASSWORD, NO_SSL);
    }

    public RedisStandaloneConfiguration(String host, int port, String password, boolean ssl) {
        this(host, port, NO_READ_SLAVES, password, ssl);
    }

    public RedisStandaloneConfiguration(String host, int port, Set<Map.Entry<String, Integer>> readSlaves) {
        this(host, port, readSlaves, NO_PASSWORD);
    }

    public RedisStandaloneConfiguration(String host, int port, Set<Map.Entry<String, Integer>> readSlaves, String password) {
        this(host, port, readSlaves, password, NO_SSL, Protocol.DEFAULT_DATABASE);
    }

    public RedisStandaloneConfiguration(String host, int port, Set<Map.Entry<String, Integer>> readSlaves, String password, boolean ssl) {
        this(host, port, readSlaves, password, ssl, Protocol.DEFAULT_DATABASE);
    }

    public RedisStandaloneConfiguration(String host, int port, Set<Map.Entry<String, Integer>> readSlaves, String password, boolean ssl, int database) {
        this.host = host;
        this.port = port;
        this.readSlaves = readSlaves;
        this.password = password;
        this.ssl = ssl;
        this.database = database;
    }

    private RedisStandaloneConfiguration(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.readSlaves = builder.readSlaves;
        this.password = builder.password;
        this.ssl = builder.ssl;
        this.database = builder.database;
    }

    String getHost() {
        return host;
    }

    int getPort() {
        return port;
    }

    String getPassword() {
        return password;
    }

    boolean isSsl() {
        return ssl;
    }

    Set<Map.Entry<String, Integer>> getReadSlaves() {
        return readSlaves;
    }

    public int getDatabase() {
        return database;
    }

    /**
     * Creates builder to build {@link RedisStandaloneConfiguration}.
     * @return created builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder to build {@link RedisStandaloneConfiguration}.
     */
    public static final class Builder {
        private String host;
        private int port;
        private Set<Map.Entry<String, Integer>> readSlaves;
        private String password;
        private boolean ssl;
        private int database;

        private Builder() {
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder readSlaves(Set<Map.Entry<String, Integer>> readSlaves) {
            this.readSlaves = readSlaves;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder ssl(boolean ssl) {
            this.ssl = ssl;
            return this;
        }

        public Builder database(int database) {
            this.database = database;
            return this;
        }

        public RedisStandaloneConfiguration build() {
            return new RedisStandaloneConfiguration(this);
        }
    }
}
