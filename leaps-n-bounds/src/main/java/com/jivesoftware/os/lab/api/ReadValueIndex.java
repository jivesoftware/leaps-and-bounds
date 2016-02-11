package com.jivesoftware.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface ReadValueIndex {

    interface ValueTx {

        boolean tx(NextValue nextValue) throws Exception;
    }

    boolean get(byte[] key, ValueTx tx) throws Exception;

    boolean rangeScan(byte[] from, byte[] to, ValueTx tx) throws Exception;

    boolean rowScan(ValueTx tx) throws Exception;

    void close() throws Exception;

    long count() throws Exception;

    boolean isEmpty() throws Exception;

}