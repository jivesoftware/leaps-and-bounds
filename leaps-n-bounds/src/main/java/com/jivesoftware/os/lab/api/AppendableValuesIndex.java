package com.jivesoftware.os.lab.api;

import java.util.List;
import java.util.concurrent.Future;

/**
 *
 * @author jonathan.colt
 */
public interface AppendableValuesIndex {

    boolean append(Values values, boolean fsyncOnFlush) throws Exception;

    List<Future<Object>> commit(boolean fsync) throws Exception;

    void close(boolean flushUncommited, boolean fsync) throws Exception;

}
