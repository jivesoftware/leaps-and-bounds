package com.jivesoftware.os.lab.collections;

import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author jonathan.colt
 */
public class LHash<V> {

    private final AtomicReference<LHMapState< V>> state;

    /**
     *
     * @param capacity
     */
    public LHash(LHMapState<V> state) {
        this.state = new AtomicReference<>(state);
    }

    public long size() {
        return state.get().size();
    }

    /**
     *
     * @return
     */
    public void clear() {
        state.set(state.get().allocate(0));
    }

    private long hash(LHMapState state, long keyShuffle) {
        keyShuffle += keyShuffle >> 8; // shuffle bits to avoid worst case clustering

        if (keyShuffle < 0) {
            keyShuffle = -keyShuffle;
        }
        return keyShuffle % state.capacity();
    }

    public V get(long key) {
        return get(Long.hashCode(key), key);
    }

    public V get(long hashCode, long key) {
        LHMapState< V> s = state.get();
        long nil = s.nil();
        long skipped = s.skipped();
        if (key == nil || key == skipped) {
            return null;
        }
        if (s.size() == 0) {
            return null;
        }
        long capacity = s.capacity();
        long start = hash(s, hashCode);
        for (long i = start, j = 0, k = capacity; // stack vars for efficiency
            j < k; // max search for key
            i = (++i) % k, j++) { // wraps around table

            long storedKey = s.key(i);
            if (storedKey == skipped) {
                continue;
            }
            if (storedKey == nil) {
                return null;
            }
            if (storedKey == key) {
                return s.value(i);
            }
        }
        return null;

    }

    public void remove(long key) {
        remove(Long.hashCode(key), key);
    }

    @SuppressWarnings("unchecked")
    public void remove(long hashCode, long key) {
        LHMapState<V> s = state.get();
        long nil = s.nil();
        long skipped = s.skipped();
        if (key == nil || key == skipped) {
            return;
        }
        if (s.size() == 0) {
            return;
        }
        long capacity = s.capacity();
        long start = hash(s, hashCode);
        for (long i = start, j = 0, k = capacity; // stack vars for efficiency
            j < k; // max search for key
            i = (++i) % k, j++) {					// wraps around table

            long storedKey = s.key(i);
            if (storedKey == skipped) {
                continue;
            }
            if (storedKey == nil) {
                return;
            }

            if (storedKey == key) {
                long next = (i + 1) % k;

                s.remove(i, skipped, null);
                if (s.key(next) == nil) {
                    for (long z = i, y = 0; y < capacity; z = (z + capacity - 1) % k, y++) {
                        if (s.key(z) != skipped) {
                            break;
                        }
                        s.clear(z);
                    }
                }
                return;
            }
        }
    }

    public void put(long key, V value) {
        put(Long.hashCode(key), key, value);
    }

    @SuppressWarnings("unchecked")
    public void put(long hashCode, long key, V value) {
        LHMapState<V> s = state.get();
        internalPut(s, hashCode, key, value);
    }

    private void internalPut(LHMapState<V> s, long hashCode, long key, V value) {
        long capacity = s.capacity();
        if (s.size() * 2 >= capacity) {
            s = grow();
            capacity = s.capacity();
        }
        long start = hash(s, hashCode);
        long nil = s.nil();
        long skipped = s.skipped();
        for (long i = start, j = 0, k = capacity; // stack vars for efficiency
            j < k; // max search for available slot
            i = (++i) % k, j++) {
            // wraps around table

            long storedKey = s.key(i);
            if (storedKey == key) {
                s.link(i, key, value);
                return;
            }
            if (storedKey == nil || storedKey == skipped) {
                s.link(i, key, value);
                return;
            }
            if (storedKey == key) {
                s.update(i, key, value);
                return;
            }
        }
    }

    private LHMapState<V> grow() {
        LHMapState<V> s = state.get();
        LHMapState<V> grown = s.allocate(s.capacity() * 2);
        long i = s.first();
        long nil = grown.nil();
        long skipped = grown.skipped();
        while (i != -1) {
            long storedKey = s.key(i);
            if (storedKey != nil && storedKey != skipped) {
                long hash = Long.hashCode(storedKey);
                internalPut(grown, hash, storedKey, s.value(i));
            }
            i = s.next(i);
        }
        state.set(grown);
        return grown;
    }

}
