package com.jivesoftware.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface ValueIndex extends ReadValueIndex, AppendableValuesIndex {

    int debt() throws Exception;

}
