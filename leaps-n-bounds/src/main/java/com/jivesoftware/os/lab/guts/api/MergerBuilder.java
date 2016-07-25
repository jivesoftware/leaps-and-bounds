package com.jivesoftware.os.lab.guts.api;

import java.util.concurrent.Callable;

/**
 *
 * @author jonathan.colt
 */
public interface MergerBuilder {

    Callable<Void> build(int minimumRun, boolean fsync, MergerBuilderCallback callback) throws Exception;

    public static interface MergerBuilderCallback {

        Callable<Void> call(int minimumRun, boolean fsync, IndexFactory indexFactory, CommitIndex commitIndex) throws Exception;
    }

}
