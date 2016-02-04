package com.jivesoftware.os.lab;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.lab.api.ValueIndex;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author jonathan.colt
 */
public class LABEnvironment {

    private final File rootFile;
    private final ExecutorService merge = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("lab-merge-%d").build()); // TODO config 'maybe'
    private final ExecutorService destroy = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("lab-destroy-%d").build()); // TODO config 'maybe'
    private final LABValueMerger valueMerger;
    private final boolean useMemMap;
    private final int minMergeDebt;
    private final int maxMergeDebt;

    public LABEnvironment(File rootFile, LABValueMerger valueMerger, boolean useMemMap, int minMergeDebt, int maxMergeDebt) {
        this.rootFile = rootFile;
        this.valueMerger = valueMerger;
        this.useMemMap = useMemMap;
        this.minMergeDebt = minMergeDebt;
        this.maxMergeDebt = maxMergeDebt;
    }

    public ValueIndex open(String primaryName, int maxUpdatesBetweenCompactionHintMarker) throws Exception {
        File indexRoot = new File(rootFile, primaryName + File.separator);
        ensure(indexRoot);
        return new LAB(valueMerger, merge, destroy, indexRoot, useMemMap, maxUpdatesBetweenCompactionHintMarker, minMergeDebt, maxMergeDebt);
    }

    boolean ensure(File key) {
        return key.exists() || key.mkdirs();
    }

    public void rename(String oldName, String newName) throws IOException {
        File oldFileName = new File(rootFile, oldName + File.separator);
        File newFileName = new File(rootFile, newName + File.separator);
        FileUtils.moveDirectory(oldFileName, newFileName);
        FileUtils.deleteDirectory(oldFileName);
    }

    public void remove(String primaryName) throws IOException {
        File fileName = new File(rootFile, primaryName + File.separator);
        FileUtils.deleteDirectory(fileName);
    }

    public void shutdown() throws InterruptedException {
        merge.shutdown();
        destroy.shutdown();
        merge.awaitTermination(30, TimeUnit.SECONDS);
        destroy.awaitTermination(30, TimeUnit.SECONDS);
    }
}
