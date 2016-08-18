package com.jivesoftware.os.lab.guts.api;

import com.jivesoftware.os.lab.api.FormatTransformer;
import java.nio.ByteBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface RawEntryStream {

    boolean stream(FormatTransformer readKeyFormatTransformer, FormatTransformer readValueFormatTransformer, ByteBuffer rawEntry) throws Exception;
}
