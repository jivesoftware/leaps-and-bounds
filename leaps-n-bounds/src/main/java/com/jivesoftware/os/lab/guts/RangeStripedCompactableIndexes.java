package com.jivesoftware.os.lab.guts;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.lab.guts.CompactableIndexes.MergerBuilder;
import com.jivesoftware.os.lab.guts.CompactableIndexes.SplitterBuilder;
import com.jivesoftware.os.lab.guts.api.NextRawEntry;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author jonathan.colt
 */
public class RangeStripedCompactableIndexes {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AtomicLong largestStripeId = new AtomicLong();
    private final AtomicLong largestIndexId = new AtomicLong();
    private final ExecutorService destroy;
    private final File root;
    private final boolean useMemMap;
    private final int entriesBetweenLeaps = 4096; // TODO expose to a config;
    private final Object copyIndexOnWrite = new Object();
    private volatile ConcurrentSkipListMap<byte[], FileBackMergableIndexs> indexes;
    private final long splitWhenKeysTotalExceedsNBytes;
    private final long splitWhenValuesTotalExceedsNBytes;
    private final long splitWhenValuesAndKeysTotalExceedsNBytes;
    private final Semaphore appendSemaphore = new Semaphore(Short.MAX_VALUE, true);

    public RangeStripedCompactableIndexes(ExecutorService destroy,
        File root,
        boolean useMemMap,
        long splitWhenKeysTotalExceedsNBytes,
        long splitWhenValuesTotalExceedsNBytes,
        long splitWhenValuesAndKeysTotalExceedsNBytes) throws Exception {

        this.destroy = destroy;
        this.root = root;
        this.useMemMap = useMemMap;
        this.splitWhenKeysTotalExceedsNBytes = splitWhenKeysTotalExceedsNBytes;
        this.splitWhenValuesTotalExceedsNBytes = splitWhenValuesTotalExceedsNBytes;
        this.splitWhenValuesAndKeysTotalExceedsNBytes = splitWhenValuesAndKeysTotalExceedsNBytes;
        this.indexes = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        File[] stripeDirs = root.listFiles();
        if (stripeDirs != null) {
            Map<File, Stripe> stripes = new HashMap<>();
            for (File stripeDir : stripeDirs) {
                Stripe stripe = loadStripe(stripeDir);
                if (stripe != null) {
                    stripes.put(stripeDir, stripe);
                }
            }

            @SuppressWarnings("unchecked")
            Map.Entry<File, Stripe>[] entries = stripes.entrySet().toArray(new Map.Entry[0]);
            for (int i = 0; i < entries.length; i++) {
                if (entries[i] == null) {
                    continue;
                }
                for (int j = i + 1; j < entries.length; j++) {
                    if (entries[j] == null) {
                        continue;
                    }
                    if (entries[i].getValue().keyRange.contains(entries[j].getValue().keyRange)) {
                        entries[j] = null;
                        FileUtils.forceDelete(entries[j].getKey());
                    }
                }
            }

            for (int i = 0; i < entries.length; i++) {
                Map.Entry<File, Stripe> entry = entries[i];
                if (entry != null) {
                    long stripeId = Long.parseLong(entry.getKey().getName());
                    if (largestStripeId.get() < stripeId) {
                        largestStripeId.set(stripeId);
                    }

                    indexes.put(entry.getValue().keyRange.start, new FileBackMergableIndexs(destroy,
                        largestStripeId,
                        largestIndexId,
                        root,
                        entry.getKey(),
                        entry.getValue().mergeableIndexes));
                }
            }
        }
    }

    private Stripe loadStripe(File stripeRoot) throws Exception {
        if (stripeRoot.isDirectory()) {
            File activeDir = new File(stripeRoot, "active");
            if (activeDir.exists()) {
                TreeSet<IndexRangeId> ranges = new TreeSet<>();
                File[] listFiles = activeDir.listFiles();
                if (listFiles != null) {
                    for (File file : listFiles) {
                        String rawRange = file.getName();
                        String[] range = rawRange.split("-");
                        long start = Long.parseLong(range[0]);
                        long end = Long.parseLong(range[1]);
                        long generation = Long.parseLong(range[2]);

                        ranges.add(new IndexRangeId(start, end, generation));
                        if (largestIndexId.get() < end) {
                            largestIndexId.set(end); //??
                        }
                    }
                }

                IndexRangeId active = null;
                TreeSet<IndexRangeId> remove = new TreeSet<>();
                for (IndexRangeId range : ranges) {
                    if (active == null || !active.intersects(range)) {
                        active = range;
                    } else {
                        LOG.info("Destroying index for overlaping range:" + range);
                        remove.add(range);
                    }
                }

                for (IndexRangeId range : remove) {
                    File file = range.toFile(activeDir);
                    FileUtils.deleteQuietly(file);
                }
                ranges.removeAll(remove);

                /**
                           0/1-1-0 a,b,c,d -append
                           0/2-2-0 x,y,z - append
                           0/1-2-1 a,b,c,d,x,y,z - merge
                           0/3-3-0 -a,-b - append
                           0/1-3-2 c,d,x,y,z - merge
                           - split
                           1/1-3-2 c,d
                           2/1-3-2 x,y,z
                           - delete 0/*
                 */
                CompactableIndexes mergeableIndexes = new CompactableIndexes();
                KeyRange keyRange = null;
                for (IndexRangeId range : ranges) {
                    File file = range.toFile(activeDir);
                    if (file.length() == 0) {
                        file.delete();
                        continue;
                    }
                    IndexFile indexFile = new IndexFile(file, "rw", useMemMap);
                    LeapsAndBoundsIndex lab = new LeapsAndBoundsIndex(destroy, range, indexFile);
                    if (keyRange == null) {
                        keyRange = new KeyRange(lab.minKey(), lab.maxKey());
                    } else {
                        keyRange.join(new KeyRange(lab.minKey(), lab.maxKey()));
                    }
                    if (!mergeableIndexes.append(lab)) {
                        throw new RuntimeException("Bueller");
                    }
                }
                if (keyRange != null) {
                    return new Stripe(keyRange, mergeableIndexes);
                }
            }
        }
        return null;
    }

    private class FileBackMergableIndexs implements SplitterBuilder, MergerBuilder {

        private final ExecutorService destroy;
        private final AtomicLong largestStripeId;
        private final AtomicLong largestIndexId;

        private final File root;
        private final File stripeRoot;
        private final File indexRoot;
        private final File splittingRoot;
        private final File mergingRoot;
        private final File commitingRoot;
        private final CompactableIndexes compactableIndexes;

        public FileBackMergableIndexs(ExecutorService destroy,
            AtomicLong largestStripeId,
            AtomicLong largestIndexId,
            File root,
            File stripeRoot,
            CompactableIndexes mergeableIndexes) throws IOException {

            this.destroy = destroy;
            this.largestStripeId = largestStripeId;
            this.largestIndexId = largestIndexId;
            this.compactableIndexes = mergeableIndexes;

            this.root = root;
            this.stripeRoot = stripeRoot;
            this.indexRoot = new File(stripeRoot, "active");
            this.mergingRoot = new File(stripeRoot, "merging");
            this.commitingRoot = new File(stripeRoot, "commiting");
            this.splittingRoot = new File(stripeRoot, "splitting");

            FileUtils.deleteQuietly(mergingRoot);
            FileUtils.deleteQuietly(commitingRoot);
            FileUtils.deleteQuietly(splittingRoot);
            FileUtils.forceMkdir(mergingRoot);
            FileUtils.forceMkdir(commitingRoot);
            FileUtils.forceMkdir(splittingRoot);
        }

        private void append(RawMemoryIndex rawMemoryIndex, byte[] minKey, byte[] maxKey) throws Exception {

            LeapsAndBoundsIndex lab = flushMemoryIndexToDisk(rawMemoryIndex, minKey, maxKey, largestIndexId.incrementAndGet(), 0, useMemMap);
            compactableIndexes.append(lab);
        }

        private LeapsAndBoundsIndex flushMemoryIndexToDisk(RawMemoryIndex index,
            byte[] minKey,
            byte[] maxKey,
            long nextIndexId,
            int generation,
            boolean fsync) throws Exception {
            LOG.debug("Commiting memory index (" + index.count() + ") to on disk index." + indexRoot);

            int maxLeaps = IndexUtil.calculateIdealMaxLeaps(index.count(), entriesBetweenLeaps);
            IndexRangeId indexRangeId = new IndexRangeId(nextIndexId, nextIndexId, generation);
            File commitingIndexFile = indexRangeId.toFile(commitingRoot);
            FileUtils.deleteQuietly(commitingIndexFile);
            IndexFile indexFile = new IndexFile(commitingIndexFile, "rw", useMemMap);
            LABAppenableIndex appendableIndex = new LABAppenableIndex(indexRangeId, indexFile, maxLeaps, entriesBetweenLeaps);
            appendableIndex.append((stream) -> {
                NextRawEntry rangeScan = index.reader().rangeScan(minKey, maxKey);
                while (rangeScan.next(stream) == NextRawEntry.Next.more);
                return true;
            });
            appendableIndex.closeAppendable(fsync);

            File commitedIndexFile = indexRangeId.toFile(indexRoot);
            return moveIntoPlace(commitingIndexFile, commitedIndexFile, indexRangeId);
        }

        private LeapsAndBoundsIndex moveIntoPlace(File commitingIndexFile, File commitedIndexFile, IndexRangeId indexRangeId) throws Exception {
            FileUtils.moveFile(commitingIndexFile, commitedIndexFile);
            LeapsAndBoundsIndex reopenedIndex = new LeapsAndBoundsIndex(destroy,
                indexRangeId, new IndexFile(commitedIndexFile, "r", useMemMap));
            reopenedIndex.flush(true);  // Sorry
            // TODO Files.fsync index when java 9 supports it.
            return reopenedIndex;
        }

        private boolean tx(ReaderTx tx) throws Exception {
            return compactableIndexes.tx(tx);
        }

        private long count() throws Exception {
            return compactableIndexes.count();
        }

        private void close() throws Exception {
            compactableIndexes.close();
        }

        private int debt(int minMergeDebt) {
            return compactableIndexes.debt(minMergeDebt);
        }

        private Callable<Void> compactor(int minMergeDebt, boolean fsync) throws Exception {
            return compactableIndexes.compactor(
                splitWhenKeysTotalExceedsNBytes,
                splitWhenValuesTotalExceedsNBytes,
                splitWhenValuesAndKeysTotalExceedsNBytes,
                this,
                minMergeDebt,
                fsync,
                this);
        }

        @Override
        public Callable<Void> build(boolean fsync, SplitterBuilderCallback callback) throws Exception {

            long nextStripeIdLeft = largestStripeId.incrementAndGet();
            long nextStripeIdRight = largestStripeId.incrementAndGet();
            FileBackMergableIndexs self = this;
            return () -> {
                appendSemaphore.acquire(Short.MAX_VALUE);
                try {
                    return callback.call((IndexRangeId id, long worstCaseCount) -> {
                        int maxLeaps = IndexUtil.calculateIdealMaxLeaps(worstCaseCount, entriesBetweenLeaps);
                        File splitIntoDir = new File(splittingRoot, String.valueOf(nextStripeIdLeft));
                        FileUtils.deleteQuietly(splitIntoDir);
                        FileUtils.forceMkdir(splitIntoDir);
                        File splittingIndexFile = id.toFile(splitIntoDir);
                        LOG.info("Creating new index for split:" + splittingIndexFile);
                        IndexFile indexFile = new IndexFile(splittingIndexFile, "rw", useMemMap);
                        LABAppenableIndex writeLeapsAndBoundsIndex = new LABAppenableIndex(id,
                            indexFile,
                            maxLeaps,
                            entriesBetweenLeaps);
                        return writeLeapsAndBoundsIndex;
                    }, (IndexRangeId id, long worstCaseCount) -> {
                        int maxLeaps = IndexUtil.calculateIdealMaxLeaps(worstCaseCount, entriesBetweenLeaps);
                        File splitIntoDir = new File(splittingRoot, String.valueOf(nextStripeIdRight));
                        FileUtils.deleteQuietly(splitIntoDir);
                        FileUtils.forceMkdir(splitIntoDir);
                        File splittingIndexFile = id.toFile(splitIntoDir);
                        LOG.info("Creating new index for split:" + splittingIndexFile);
                        IndexFile indexFile = new IndexFile(splittingIndexFile, "rw", useMemMap);
                        LABAppenableIndex writeLeapsAndBoundsIndex = new LABAppenableIndex(id,
                            indexFile,
                            maxLeaps,
                            entriesBetweenLeaps);
                        return writeLeapsAndBoundsIndex;
                    }, (ids) -> {
                        File left = new File(root, String.valueOf(nextStripeIdLeft));
                        File leftActive = new File(left, "active");
                        File right = new File(root, String.valueOf(nextStripeIdRight));
                        File rightActive = new File(right, "active");
                        LOG.info("Commiting split:" + stripeRoot + " became:" + left + " and " + right);

                        try {
                            FileUtils.moveDirectory(new File(splittingRoot, String.valueOf(nextStripeIdLeft)), leftActive);
                            FileUtils.moveDirectory(new File(splittingRoot, String.valueOf(nextStripeIdRight)), rightActive);

                            Stripe leftStripe = loadStripe(left);
                            Stripe rightStripe = loadStripe(right);
                            synchronized (copyIndexOnWrite) {
                                ConcurrentSkipListMap<byte[], FileBackMergableIndexs> copyOfIndexes = new ConcurrentSkipListMap<>(UnsignedBytes
                                    .lexicographicalComparator());
                                copyOfIndexes.putAll(indexes);

                                for (Iterator<Map.Entry<byte[], FileBackMergableIndexs>> iterator = copyOfIndexes.entrySet().iterator(); iterator.hasNext();) {
                                    Map.Entry<byte[], FileBackMergableIndexs> next = iterator.next();
                                    if (next.getValue() == self) {
                                        iterator.remove();
                                        break;
                                    }
                                }
                                if (leftStripe != null && leftStripe.keyRange != null && leftStripe.keyRange.start != null) {
                                    copyOfIndexes.put(leftStripe.keyRange.start,
                                        new FileBackMergableIndexs(destroy, largestStripeId, largestIndexId, root, left,
                                            leftStripe.mergeableIndexes));
                                }

                                if (rightStripe != null && rightStripe.keyRange != null && rightStripe.keyRange.start != null) {
                                    copyOfIndexes.put(rightStripe.keyRange.start,
                                        new FileBackMergableIndexs(destroy, largestStripeId, largestIndexId, root, right,
                                            rightStripe.mergeableIndexes));
                                }
                                indexes = copyOfIndexes;
                            }
                            compactableIndexes.destroy();
                            FileUtils.deleteQuietly(mergingRoot);
                            FileUtils.deleteQuietly(commitingRoot);
                            FileUtils.deleteQuietly(splittingRoot);

                            LOG.info("Completed split:" + stripeRoot + " became:" + left + " and " + right);
                            return null;
                        } catch (Exception x) {
                            FileUtils.deleteQuietly(left);
                            FileUtils.deleteQuietly(right);
                            LOG.error("Failed to split:" + stripeRoot + " became:" + left + " and " + right, x);
                            throw x;
                        }
                    }, fsync);

                } finally {
                    appendSemaphore.release(Short.MAX_VALUE);
                }
            };

        }

        @Override
        public Callable<Void> build(int minimumRun, boolean fsync, MergerBuilderCallback callback) throws Exception {
            return callback.build(minimumRun, fsync, (id, count) -> {
                int maxLeaps = IndexUtil.calculateIdealMaxLeaps(count, entriesBetweenLeaps);
                File mergingIndexFile = id.toFile(mergingRoot);
                FileUtils.deleteQuietly(mergingIndexFile);
                IndexFile indexFile = new IndexFile(mergingIndexFile, "rw", useMemMap);
                LABAppenableIndex writeLeapsAndBoundsIndex = new LABAppenableIndex(id,
                    indexFile,
                    maxLeaps,
                    entriesBetweenLeaps);
                return writeLeapsAndBoundsIndex;
            }, (ids) -> {
                File mergedIndexFile = ids.get(0).toFile(mergingRoot);
                File file = ids.get(0).toFile(indexRoot);
                FileUtils.deleteQuietly(file);
                return moveIntoPlace(mergedIndexFile, file, ids.get(0));
            });
        }

    }

    private static class Stripe {

        KeyRange keyRange;
        CompactableIndexes mergeableIndexes;

        public Stripe(KeyRange keyRange, CompactableIndexes mergeableIndexes) {
            this.keyRange = keyRange;
            this.mergeableIndexes = mergeableIndexes;
        }

    }

    public void append(RawMemoryIndex rawMemoryIndex, boolean fsync) throws Exception {
        appendSemaphore.acquire();
        try {
            byte[] minKey = rawMemoryIndex.minKey();
            byte[] maxKey = rawMemoryIndex.maxKey();

            if (indexes.isEmpty()) {
                File newStripeRoot = new File(root, String.valueOf(largestStripeId.incrementAndGet()));

                FileBackMergableIndexs index = new FileBackMergableIndexs(destroy,
                    largestStripeId,
                    largestIndexId,
                    root,
                    newStripeRoot,
                    new CompactableIndexes());
                synchronized (copyIndexOnWrite) {
                    ConcurrentSkipListMap<byte[], FileBackMergableIndexs> copyOfIndexes = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());
                    copyOfIndexes.putAll(indexes);
                    copyOfIndexes.put(minKey, index);
                    indexes = copyOfIndexes;
                }
                index.append(rawMemoryIndex, null, null);
                return;
            }

            SortedMap<byte[], FileBackMergableIndexs> tailMap = indexes.tailMap(minKey);
            if (tailMap.isEmpty()) {
                tailMap = indexes.tailMap(indexes.lastKey());
            } else {
                byte[] priorKey = indexes.lowerKey(tailMap.firstKey());
                if (priorKey == null) {
                    FileBackMergableIndexs moved;
                    synchronized (copyIndexOnWrite) {
                        ConcurrentSkipListMap<byte[], FileBackMergableIndexs> copyOfIndexes = new ConcurrentSkipListMap<>(UnsignedBytes
                            .lexicographicalComparator());
                        copyOfIndexes.putAll(indexes);
                        moved = copyOfIndexes.remove(tailMap.firstKey());
                        copyOfIndexes.put(minKey, moved);
                        indexes = copyOfIndexes;
                    }
                    tailMap = indexes.tailMap(minKey);
                } else {
                    tailMap = indexes.tailMap(priorKey);
                }
            }

            Map.Entry<byte[], FileBackMergableIndexs> priorEntry = null;
            for (Map.Entry<byte[], FileBackMergableIndexs> currentEntry : tailMap.entrySet()) {
                if (priorEntry == null) {
                    priorEntry = currentEntry;
                } else {
                    priorEntry.getValue().append(rawMemoryIndex, priorEntry.getKey(), currentEntry.getKey());
                    priorEntry = currentEntry;
                    if (UnsignedBytes.lexicographicalComparator().compare(maxKey, currentEntry.getKey()) < 0) {
                        priorEntry = null;
                        break;
                    }
                }
            }
            if (priorEntry != null) {
                priorEntry.getValue().append(rawMemoryIndex, priorEntry.getKey(), null);
            }

        } finally {
            appendSemaphore.release();
        }

    }

    public boolean tx(byte[] from, byte[] to, ReaderTx tx) throws Exception {
        if (indexes.isEmpty()) {
            return tx.tx(new ReadIndex[0]);
        }
        SortedMap<byte[], FileBackMergableIndexs> map;
        if (from != null && to != null) {
            byte[] floorKey = indexes.floorKey(from);
            if (floorKey != null) {
                map = indexes.subMap(floorKey, true, to, Arrays.equals(from, to));
            } else {
                map = indexes.headMap(to);
            }
        } else if (from != null) {
            byte[] floorKey = indexes.floorKey(from);
            if (floorKey != null) {
                map = indexes.tailMap(floorKey);
            } else {
                map = indexes;
            }
        } else if (to != null) {
            map = indexes.headMap(to);
        } else {
            map = indexes;
        }

        for (FileBackMergableIndexs index : map.values()) {
            if (!index.tx(tx)) {
                return false;
            }
        }
        return true;

    }

    public int debt(int minMergeDebt) throws Exception {
        int maxDebt = 0;
        for (FileBackMergableIndexs index : indexes.values()) {
            maxDebt = Math.max(index.debt(minMergeDebt), maxDebt);
        }
        return maxDebt;
    }

    public List<Callable<Void>> buildCompactors(int minimumRun,
        boolean fsync) throws Exception {
        List<Callable<Void>> compactors = null;
        for (FileBackMergableIndexs index : indexes.values()) {
            Callable<Void> compactor = index.compactor(minimumRun, fsync);
            if (compactor != null) {
                if (compactors == null) {
                    compactors = new ArrayList<>();
                }
                compactors.add(compactor);
            }
        }
        return compactors;
    }

    public boolean isEmpty() throws Exception {
        return indexes.isEmpty();
    }

    public long count() throws Exception {
        long count = 0;
        for (FileBackMergableIndexs index : indexes.values()) {
            count = index.count();
        }
        return count;
    }

    public void close() throws Exception {
        for (FileBackMergableIndexs index : indexes.values()) {
            index.close();
        }
    }

}
