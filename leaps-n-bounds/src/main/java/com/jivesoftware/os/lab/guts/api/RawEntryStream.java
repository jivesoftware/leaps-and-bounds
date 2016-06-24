package com.jivesoftware.os.lab.guts.api;

import com.jivesoftware.os.lab.api.FormatTransformer;

/**
 *
 * @author jonathan.colt
 */
public interface RawEntryStream {

    boolean stream(FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        byte[] rawEntry,
        int offset,
        int length) throws Exception;

//    boolean stream(FormatTransformer readKeyFormatTransformer,
//        FormatTransformer readValueFormatTransformer,
//        IReadable readable) throws Exception;
}
