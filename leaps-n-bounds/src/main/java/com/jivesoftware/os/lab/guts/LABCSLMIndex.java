/*
 * Copyright 2016 jonathan.colt.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.rawhide.Rawhide;
import com.jivesoftware.os.lab.guts.allocators.LABCostChangeInBytes;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.Scanner;
import com.jivesoftware.os.lab.io.BolBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 *
 * @author jonathan.colt
 */
public class LABCSLMIndex implements LABIndex {

    private final ConcurrentSkipListMap<byte[], byte[]> map;
    private final StripingBolBufferLocks bolBufferLocks;
    private final int seed;

    public LABCSLMIndex(Rawhide rawhide, StripingBolBufferLocks bolBufferLocks) {
        this.map = new ConcurrentSkipListMap<>(rawhide.getKeyComparator());
        this.bolBufferLocks = bolBufferLocks;
        this.seed = System.identityHashCode(this);
    }

    @Override
    public void compute(FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        BolBuffer rawEntry,
        BolBuffer keyBytes,
        BolBuffer valueBuffer,
        Compute remappingFunction,
        LABCostChangeInBytes changeInBytes) {

        synchronized (bolBufferLocks.lock(keyBytes, seed)) {
            map.compute(keyBytes.copy(), // Grrr
                (k, v) -> {
                    long cost = 0;
                    try {
                        BolBuffer apply;
                        if (v != null) {
                            valueBuffer.bytes = v;
                            valueBuffer.offset = 0;
                            valueBuffer.length = v.length;
                            apply = remappingFunction.apply(readKeyFormatTransformer, readValueFormatTransformer, rawEntry, valueBuffer);
                            cost = v.length - ((apply == null || apply.length == -1) ? 0 : apply.length);
                        } else {
                            apply = remappingFunction.apply(readKeyFormatTransformer, readValueFormatTransformer, rawEntry, null);
                            if (apply != null && apply.length > -1) {
                                cost = k.length + apply.length;
                            }
                        }
                        if (apply != null && v == apply.bytes) {
                            return v;
                        } else {
                            return apply == null ? null : apply.copy();
                        }
                    } finally {
                        changeInBytes.cost(cost);
                    }
                });
        }
    }

    @Override
    public BolBuffer get(BolBuffer key, BolBuffer valueBuffer) {
        byte[] got = map.get(key.copy()); // Grrr
        if (got == null) {
            return null;
        }
        valueBuffer.set(got);
        return valueBuffer;
    }

    @Override
    public boolean contains(byte[] from, byte[] to) {
        return !subMap(from, to).isEmpty();
    }

    private Map<byte[], byte[]> subMap(byte[] from, byte[] to) {
        if (from != null && to != null) {
            return map.subMap(from, to);
        } else if (from != null) {
            return map.tailMap(from);
        } else if (to != null) {
            return map.headMap(to);
        } else {
            return map;
        }
    }

    @Override
    public Scanner scanner(byte[] from, byte[] to, BolBuffer entryBuffer) {
        Iterator<Map.Entry<byte[], byte[]>> iterator = subMap(from != null ? from : null, to != null ? to : null)
            .entrySet()
            .iterator();
        return new Scanner() {
            @Override
            public Scanner.Next next(RawEntryStream stream) throws Exception {
                if (iterator.hasNext()) {
                    Map.Entry<byte[], byte[]> next = iterator.next();
                    byte[] value = next.getValue();
                    entryBuffer.force(value, 0, value.length);
                    boolean more = stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, entryBuffer);
                    return more ? Scanner.Next.more : Scanner.Next.stopped;
                }
                return Scanner.Next.eos;
            }

            @Override
            public void close() {
            }
        };
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public byte[] firstKey() {
        return map.firstKey();
    }

    @Override
    public byte[] lastKey() {
        return map.lastKey();
    }

}
