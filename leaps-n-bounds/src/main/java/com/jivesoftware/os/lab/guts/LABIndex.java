package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.guts.allocators.LABCostChangeInBytes;
import com.jivesoftware.os.lab.guts.api.Scanner;
import com.jivesoftware.os.lab.io.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface LABIndex {

    interface Compute {

        BolBuffer apply(FormatTransformer readKeyFormatTransformer,
            FormatTransformer readValueFormatTransformer,
            BolBuffer rawEntry,
            BolBuffer existing);
    }

    void compute(FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        BolBuffer rawEntry,
        BolBuffer keyBytes,
        BolBuffer valueBuffer,
        Compute computeFunction,
        LABCostChangeInBytes changeInBytes) throws Exception;

    BolBuffer get(BolBuffer key, BolBuffer valueBuffer) throws Exception;

    boolean contains(byte[] from, byte[] to) throws Exception;

    Scanner scanner(byte[] from, byte[] to, BolBuffer entryBuffer, BolBuffer entryKeyBuffer) throws Exception;

    void clear() throws Exception;

    boolean isEmpty() throws Exception;

    byte[] firstKey() throws Exception;

    byte[] lastKey() throws Exception;

}
