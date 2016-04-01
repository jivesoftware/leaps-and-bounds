package com.jivesoftware.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface ReadValueIndex {

    void get(Keys keys, ValueStream stream) throws Exception;

    boolean get(byte[] key, ValueStream stream) throws Exception;

    boolean rangeScan(byte[] from, byte[] to, ValueStream stream) throws Exception;

    boolean rowScan(ValueStream stream) throws Exception;
    
    long count() throws Exception;

    boolean isEmpty() throws Exception;

}