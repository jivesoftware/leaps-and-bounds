package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.guts.allocators.LABCostChangeInBytes;
import com.jivesoftware.os.lab.guts.api.Scanner;
import com.jivesoftware.os.lab.io.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface LABIndex<E, B> {

    interface Compute<E, B> {

        BolBuffer apply(FormatTransformer readKeyFormatTransformer,
            FormatTransformer readValueFormatTransformer,
            E apply,
            E existing);
    }

    void compute(FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        E entry,
        B keyBuffer,
        B valueBuffer,
        Compute<E, B> computeFunction,
        LABCostChangeInBytes changeInBytes) throws Exception;

    E get(BolBuffer key, B valueBuffer) throws Exception;

    boolean contains(byte[] from, byte[] to) throws Exception;

    Scanner scanner(byte[] from, byte[] to, B entryBuffer, B entryKeyBuffer) throws Exception;

    void clear() throws Exception;

    boolean isEmpty() throws Exception;

    byte[] firstKey() throws Exception;

    byte[] lastKey() throws Exception;

    int poweredUpTo();
}
