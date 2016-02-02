package com.jivesoftware.os.lab.collections;

/**
 *
 * @author jonathan.colt
 */
public class ConcurrentLHash<V> {

    //private final TLongObjectHashMap<V>[] maps;
    private final BAHash<V>[] maps;

    @SuppressWarnings("unchecked")
    public ConcurrentLHash(long capacity, long nilKey, long skipKey, int concurrency) {
        this.maps = new BAHash[concurrency];
        for (int i = 0; i < concurrency; i++) {
            this.maps[i] = new BAHash<>(new BAHMapState<>(capacity, nilKey, skipKey));
        }
    }

    public void put(long key, V value) {
        BAHash<V> hmap = hmap(key);
        synchronized (hmap) {
            hmap.put(key, value);
        }
    }

    private BAHash<V> hmap(long key) {
        return maps[Math.abs((Long.hashCode(key)) % maps.length)];
    }

    public V get(long key) {
        BAHash<V> hmap = hmap(key);
        synchronized (hmap) {
            return hmap.get(key);
        }
    }

    public void remove(long key) {
        BAHash<V> hmap = hmap(key);
        synchronized (hmap) {
            hmap.remove(key);
        }
    }

    public void clear() {
        for (BAHash<V> hmap : maps) {
            synchronized (hmap) {
                hmap.clear();
            }
        }
    }

    public int size() {
        int size = 0;
        for (BAHash<V> hmap : maps) {
            size += hmap.size();
        }
        return size;
    }

}
