package com.jivesoftware.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface ReadValueIndex {

    interface ValueTx<R> {

        R tx(NextValue nextValue) throws Exception;
    }

    <R> R get(byte[] key, ValueTx<R> tx) throws Exception;

    <R> R rangeScan(byte[] from, byte[] to, ValueTx<R> tx) throws Exception;

    <R> R rowScan(ValueTx<R> tx) throws Exception;

    void close() throws Exception;

    long count() throws Exception;

    boolean isEmpty() throws Exception;

}