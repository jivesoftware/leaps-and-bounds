package com.jivesoftware.os.lab.guts.api;

import com.jivesoftware.os.lab.guts.api.CommitIndex;
import com.jivesoftware.os.lab.guts.api.IndexFactory;
import java.util.concurrent.Callable;

/**
 *
 * @author jonathan.colt
 */
public interface MergerBuilder {

    Callable<Void> build(int minimumRun, boolean fsync, MergerBuilderCallback callback) throws Exception;

    public static interface MergerBuilderCallback {

        Callable<Void> build(int minimumRun, boolean fsync, IndexFactory indexFactory, CommitIndex commitIndex) throws Exception;
    }

}
