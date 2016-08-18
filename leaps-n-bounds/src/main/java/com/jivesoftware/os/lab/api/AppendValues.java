package com.jivesoftware.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface AppendValues {

    boolean consume(AppendValueStream stream) throws Exception;
}
