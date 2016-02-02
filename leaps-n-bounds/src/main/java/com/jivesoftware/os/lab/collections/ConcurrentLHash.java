package com.jivesoftware.os.lab.collections;

import gnu.trove.map.hash.TLongObjectHashMap;

/**
 *
 * @author jonathan.colt
 */
public class ConcurrentLHash<V> {

    private final TLongObjectHashMap<V>[] maps;

    @SuppressWarnings("unchecked")
    public ConcurrentLHash(int capacity, int concurrency) {
        this.maps = new TLongObjectHashMap[concurrency];
        for (int i = 0; i < concurrency; i++) {
            this.maps[i] = new TLongObjectHashMap<>(capacity);
        }
    }

    public void put(long key, V value) {
        TLongObjectHashMap<V> hmap = hmap(key);
        synchronized (hmap) {
            hmap.put(key, value);
        }
    }

    private TLongObjectHashMap<V> hmap(long key) {
        return maps[Math.abs((Long.hashCode(key)) % maps.length)];
    }

    public V get(long key) {
        TLongObjectHashMap<V> hmap = hmap(key);
        synchronized (hmap) {
            return hmap.get(key);
        }
    }

    public void remove(long key) {
        TLongObjectHashMap<V> hmap = hmap(key);
        synchronized (hmap) {
            hmap.remove(key);
        }
    }

    public void clear() {
        for (TLongObjectHashMap<V> hmap : maps) {
            synchronized (hmap) {
                hmap.clear();
            }
        }
    }

    public int size() {
        int size = 0;
        for (TLongObjectHashMap<V> hmap : maps) {
            size += hmap.size();
        }
        return size;
    }

}
