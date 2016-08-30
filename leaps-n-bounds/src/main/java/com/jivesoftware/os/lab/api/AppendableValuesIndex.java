package com.jivesoftware.os.lab.api;

import com.jivesoftware.os.lab.BolBuffer;
import java.util.List;
import java.util.concurrent.Future;

/**
 *
 * @author jonathan.colt
 */
public interface AppendableValuesIndex {

    boolean journaledAppend(AppendValues values, boolean fsyncAfterAppend, BolBuffer rawEntryBuffer) throws Exception;

    boolean append(AppendValues values, boolean fsyncOnFlush, BolBuffer rawEntryBuffer) throws Exception;

    List<Future<Object>> commit(boolean fsync, boolean waitIfToFarBehind) throws Exception;

    List<Future<Object>> compact(boolean fsync, int minDebt, int maxDebt, boolean waitIfToFarBehind) throws Exception;

    void close(boolean flushUncommited, boolean fsync) throws Exception;

}
