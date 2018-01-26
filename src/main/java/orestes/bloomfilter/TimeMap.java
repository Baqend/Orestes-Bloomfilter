package orestes.bloomfilter;

import java.time.Clock;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Created on 24.11.17.
 *
 * @author Konstantin Simon Maria Möllers
 */
public final class TimeMap<T> implements Map<T, Long> {
    private static final Clock clock = Clock.systemUTC();
    private final Map<T, Long> map = new ConcurrentHashMap<>();

    public static <T, K> Collector<T, ?, TimeMap<K>> collectMillis(Function<? super T, ? extends K> keyMapper,
                                                                   Function<? super T, ? extends Long> timeMapper) {
        return Collectors.toMap(keyMapper, timeMapper, Math::max, TimeMap::new);
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    @Override
    public Long get(Object key) {
        return map.get(key);
    }
    
    public Long get(Object key, TimeUnit timeUnit) {
        Long sourceDuration = get(key);
        return sourceDuration == null ? null : timeUnit.convert(sourceDuration, MILLISECONDS);
    }

    public long putRemaining(T key, long value, TimeUnit timeUnit) {
        long absoluteValue = clock.instant().plusMillis(MILLISECONDS.convert(value, timeUnit)).toEpochMilli();
        return compute(key, (item, oldValue) -> oldValue == null ? absoluteValue : Math.max(oldValue, absoluteValue));
    }

    public Long getRemaining(Object key, TimeUnit timeUnit) {
        Long get = get(key);
        if (get == null) {
            return null;
        }

        long remainingMillis = get - now();
        return remainingMillis <= 0L ? null : timeUnit.convert(remainingMillis, MILLISECONDS);
    }

    @Override
    public Long put(T key, Long value) {
        return map.put(key, value);
    }

    public long put(T key, long value, TimeUnit timeUnit) {
        return put(key, MILLISECONDS.convert(value, timeUnit));
    }

    @Override
    public Long remove(Object key) {
        return map.remove(key);
    }

    @Override
    public void putAll(Map<? extends T, ? extends Long> m) {
        map.putAll(m);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public Set<T> keySet() {
        return map.keySet();
    }

    @Override
    public Collection<Long> values() {
        return map.values();
    }

    @Override
    public Set<Entry<T, Long>> entrySet() {
        return map.entrySet();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TimeMap) {
            TimeMap other = (TimeMap) o;
            return map.equals(other.map);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

    /**
     * Returns the time map's current time.
     *
     * @return The current time in milliseconds.
     */
    public long now() {
        return clock.millis();
    }
}
