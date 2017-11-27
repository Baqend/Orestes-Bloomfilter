package orestes.bloomfilter;

/**
 * Created on 24.11.17.
 *
 * @author Konstantin Simon Maria Möllers
 */
public interface ExpirationMapAware<T> {
    /**
     * Gets a map of items to their expiration times.
     *
     * @return A time map of TTLs.
     */
    TimeMap<T> getExpirationMap();

    /**
     * Sets a map of items to their expiration times.
     *
     * @param map A time map of TTLs.
     */
    void setExpirationMap(TimeMap<T> map);
}
