package orestes.bloomfilter.redis.helper;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class RedisStandaloneConfiguration {
    private static final String NO_PASSWORD = "";
    private static final boolean NO_SSL = false;
    private static final Set<Map.Entry<String, Integer>> NO_READ_SLAVES = Collections.emptySet();

    private final String host;
    private final int port;
    private Set<Map.Entry<String, Integer>> readSlaves;
    private final String password;
    private final boolean ssl;

    public RedisStandaloneConfiguration(String host, int port) {
        this(host, port, NO_PASSWORD, NO_SSL);
    }

    public RedisStandaloneConfiguration(String host, int port, String password, boolean ssl) {
        this(host, port, NO_READ_SLAVES, password, ssl);
    }

    public RedisStandaloneConfiguration(String host, int port, Set<Map.Entry<String, Integer>> readSlaves) {
        this(host, port, readSlaves, NO_PASSWORD, NO_SSL);
    }


    public RedisStandaloneConfiguration(String host, int port, Set<Map.Entry<String, Integer>> readSlaves, String password, boolean ssl) {
        this.host = host;
        this.port = port;
        this.readSlaves = readSlaves;
        this.password = password;
        this.ssl = ssl;
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

}
