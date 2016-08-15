package com.jivesoftware.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface Ranges {

    interface RangeStream {

        boolean range(int index, byte[] key, byte[] to) throws Exception;
    }

    boolean ranges(RangeStream rangeStream) throws Exception;
}
