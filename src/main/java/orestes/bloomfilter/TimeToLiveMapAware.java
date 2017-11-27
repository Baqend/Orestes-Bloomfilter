package orestes.bloomfilter;

/**
 * Created on 24.11.17.
 *
 * @author Konstantin Simon Maria Möllers
 */
public interface TimeToLiveMapAware<T> {
    /**
     * Gets a map of items to their TTLs.
     *
     * @return A time map of TTLs.
     */
    TimeMap<T> getTimeToLiveMap();

    /**
     * Sets a map of items to their TTLs.
     *
     * @param map A time map of TTLs.
     */
    void setTimeToLiveMap(TimeMap<T> map);
}
