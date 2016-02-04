package com.jivesoftware.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface AppendableValuesIndex {

    boolean append(Values values) throws Exception;

    void commit() throws Exception;
}
