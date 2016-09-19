package com.jivesoftware.os.lab.api;

import com.jivesoftware.os.lab.io.BolBuffer;
import java.util.List;
import java.util.concurrent.Future;

/**
 *
 * @author jonathan.colt
 */
public interface AppendableValuesIndex<P> {

    boolean journaledAppend(AppendValues<P> values, boolean fsyncAfterAppend, BolBuffer rawEntryBuffer, BolBuffer keyBuffer) throws Exception;

    boolean append(AppendValues<P> values, boolean fsyncOnFlush, BolBuffer rawEntryBuffer, BolBuffer keyBuffer) throws Exception;

    List<Future<Object>> commit(boolean fsync, boolean waitIfToFarBehind) throws Exception;

    List<Future<Object>> compact(boolean fsync, int minDebt, int maxDebt, boolean waitIfToFarBehind) throws Exception;

    void close(boolean flushUncommited, boolean fsync) throws Exception;

}
