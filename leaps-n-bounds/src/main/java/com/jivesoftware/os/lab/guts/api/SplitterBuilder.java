package com.jivesoftware.os.lab.guts.api;

import java.util.concurrent.Callable;

/**
 *
 * @author jonathan.colt
 */
public interface SplitterBuilder {

    Callable<Void> buildSplitter(boolean fsync, SplitterBuilderCallback splitterBuilderCallback) throws Exception;

    public static interface SplitterBuilderCallback {

        Void call(IndexFactory leftHalfIndexFactory, IndexFactory rightHalfIndexFactory, CommitIndex commitIndex, boolean fsync) throws Exception;
    }

}
