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

import com.jivesoftware.os.lab.BolBuffer;
import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.Scanner;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 *
 * @author jonathan.colt
 */
public class LABCSLMIndex implements LABIndex {

    private final ConcurrentSkipListMap<byte[], byte[]> map;

    public LABCSLMIndex(Rawhide rawhide) {
        this.map = new ConcurrentSkipListMap<>(rawhide.getKeyComparator());
    }

    @Override
    public void compute(BolBuffer keyBytes, BolBuffer valueBuffer, Compute remappingFunction) {
        map.compute(keyBytes.copy(), // Grrr
            (k, v) -> {

                BolBuffer vb = null;
                if (v != null) {
                    vb.bytes = v;
                    vb.offset = 0;
                    vb.length = v.length;
                }
                BolBuffer apply = remappingFunction.apply(vb);
                if (apply != null && v == apply.bytes) {
                    return v;
                } else {
                    return apply == null ? null : apply.copy();
                }
            });
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
    public Scanner scanner(byte[] from, byte[] to) {
        Iterator<Map.Entry<byte[], byte[]>> iterator = subMap(from, to).entrySet().iterator();
        return new Scanner() {
            @Override
            public Scanner.Next next(RawEntryStream stream) throws Exception {
                if (iterator.hasNext()) {
                    Map.Entry<byte[], byte[]> next = iterator.next();
                    boolean more = stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, ByteBuffer.wrap(next.getValue()));
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
