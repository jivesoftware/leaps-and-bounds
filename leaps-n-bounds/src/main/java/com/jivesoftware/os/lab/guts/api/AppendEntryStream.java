package com.jivesoftware.os.lab.guts.api;

import com.jivesoftware.os.lab.api.FormatTransformer;

/**
 *
 * @author jonathan.colt
 */
public interface AppendEntryStream {

    boolean stream(FormatTransformer readKeyFormatTransformer, FormatTransformer readValueFormatTransformer, byte[] rawEntry, int offset, int length) throws Exception;
}
