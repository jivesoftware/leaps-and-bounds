package com.jivesoftware.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface ReadValueIndex {

    void get(Keys keys, ValueStream stream, boolean hydrateValues) throws Exception;

    boolean rangeScan(byte[] from, byte[] to, ValueStream stream, boolean hydrateValues) throws Exception;

    boolean rowScan(ValueStream stream, boolean hydrateValues) throws Exception;
    
    long count() throws Exception;

    boolean isEmpty() throws Exception;

}