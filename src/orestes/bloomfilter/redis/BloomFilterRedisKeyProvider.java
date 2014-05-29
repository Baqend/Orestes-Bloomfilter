package orestes.bloomfilter.redis;

public class BloomFilterRedisKeyProvider {

    private String key;
    private String BLOOM;
    private String M;
    private String K;
    private String C;
    private String HASH;
    private String COUNTS;

    public BloomFilterRedisKeyProvider(String key) {
        BLOOM = key + ":bitcbloomfilter";
        M = BLOOM + ":m";
        K = BLOOM + ":k";
        C = BLOOM + ":c";
        HASH = BLOOM + ":hash";
        COUNTS = BLOOM + ":counts";
    }

    public String getKey() {
        return key;
    }

    public String getBloomKey() {
        return BLOOM;
    }

    public String getMKey() {
        return M;
    }

    public String getKKey() {
        return K;
    }

    public String getCKey() {
        return C;
    }

    public String getHashKey() {
        return HASH;
    }

    public String getCountsKey() {
        return COUNTS;
    }
}