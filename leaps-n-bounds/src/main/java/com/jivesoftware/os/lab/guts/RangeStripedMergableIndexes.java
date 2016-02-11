package com.jivesoftware.os.lab.guts;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.lab.guts.api.NextRawEntry;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author jonathan.colt
 */
public class RangeStripedMergableIndexes {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AtomicLong largestStripeId = new AtomicLong();
    private final AtomicLong largestIndexId = new AtomicLong();
    private final ExecutorService destroy;
    private final File root;
    private final boolean useMemMap;
    private final int entriesBetweenLeaps = 4096; // TODO expose to a config;
    private volatile ConcurrentSkipListMap<byte[], FileBackMergableIndexs> indexes;
    private final long splitWhenKeysTotalExceedsNBytes;
    private final long splitWhenValuesTotalExceedsNBytes;
    private final long splitWhenValuesAndKeysTotalExceedsNBytes;
    private final Semaphore splittingSemaphore = new Semaphore(1024, true);

    public RangeStripedMergableIndexes(ExecutorService destroy,
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

        File[] listedStripes = root.listFiles();
        if (listedStripes != null) {
            Map<File, Stripe> stripes = new HashMap<>();
            for (File listedStripe : listedStripes) {

                if (listedStripe.isDirectory()) {
                    File activeDir = new File(listedStripe,"active");
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
                        MergeableIndexes mergeableIndexes = new MergeableIndexes();
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
                            stripes.put(listedStripe, new Stripe(keyRange, mergeableIndexes));
                        }
                    }
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
                        largestIndexId,
                        entry.getKey(),
                        entry.getValue().mergeableIndexes, entriesBetweenLeaps, useMemMap));
                }
            }

        }
    }

    static private class FileBackMergableIndexs {

        private final ExecutorService destroy;
        private final AtomicLong largestIndexId;

        private final File mergingRoot;
        private final File commitingRoot;
        private final File indexRoot;
        private final MergeableIndexes mergeableIndexes;
        private final int entriesBetweenLeaps;
        private final boolean useMemMap;

        public FileBackMergableIndexs(ExecutorService destroy,
            AtomicLong largestIndexId,
            File root,
            MergeableIndexes mergeableIndexes,
            int entriesBetweenLeaps,
            boolean useMemMap) throws IOException {

            this.destroy = destroy;
            this.largestIndexId = largestIndexId;
            this.mergeableIndexes = mergeableIndexes;
            this.entriesBetweenLeaps = entriesBetweenLeaps;
            this.useMemMap = useMemMap;

            this.indexRoot = new File(root, "active");
            this.mergingRoot = new File(root, "merging");
            this.commitingRoot = new File(root, "commiting");

            FileUtils.deleteQuietly(mergingRoot);
            FileUtils.deleteQuietly(commitingRoot);
            FileUtils.forceMkdir(mergingRoot);
            FileUtils.forceMkdir(commitingRoot);
        }

        private void append(RawMemoryIndex rawMemoryIndex, byte[] minKey, byte[] maxKey) throws Exception {

            LeapsAndBoundsIndex lab = flushMemoryIndexToDisk(rawMemoryIndex, minKey, maxKey, largestIndexId.incrementAndGet(), 0, useMemMap);
            mergeableIndexes.append(lab);
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
            LABAppenableIndex write = new LABAppenableIndex(indexRangeId, indexFile, maxLeaps, entriesBetweenLeaps);
            write.append((RawEntryStream stream) -> {
                NextRawEntry rangeScan = index.reader().rangeScan(minKey, maxKey);
                while (rangeScan.next(stream));
                return true;
            });
            write.closeAppendable(fsync);

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
            return mergeableIndexes.tx(tx);
        }

        private long count() throws Exception {
            return mergeableIndexes.count();
        }

        private void close() throws Exception {
            mergeableIndexes.close();
        }

        private int hasMergeDebt(int minMergeDebt) throws IOException {
            return mergeableIndexes.hasMergeDebt(minMergeDebt);
        }

        private MergeableIndexes.Merger buildMerger(int minimumRun, boolean fsync) throws Exception {

            return mergeableIndexes.buildMerger(minimumRun,
                (id, count) -> {

                    int maxLeaps = IndexUtil.calculateIdealMaxLeaps(count, entriesBetweenLeaps);

                    File mergingIndexFile = id.toFile(mergingRoot);
                    FileUtils.deleteQuietly(mergingIndexFile);
                    IndexFile indexFile = new IndexFile(mergingIndexFile, "rw", useMemMap);
                    LABAppenableIndex writeLeapsAndBoundsIndex = new LABAppenableIndex(id,
                        indexFile,
                        maxLeaps,
                        entriesBetweenLeaps);
                    return writeLeapsAndBoundsIndex;
                },
                (ids) -> {
                    File mergedIndexFile = ids.get(0).toFile(mergingRoot);
                    File file = ids.get(0).toFile(indexRoot);
                    FileUtils.deleteQuietly(file);
                    return moveIntoPlace(mergedIndexFile, file, ids.get(0));
                }, fsync);
        }
    }

    private static class Stripe {

        KeyRange keyRange;
        MergeableIndexes mergeableIndexes;

        public Stripe(KeyRange keyRange, MergeableIndexes mergeableIndexes) {
            this.keyRange = keyRange;
            this.mergeableIndexes = mergeableIndexes;
        }

    }

    private final Object copyIndexOnWrite = new Object();

    public void append(RawMemoryIndex rawMemoryIndex, boolean fsync) throws Exception {

        splittingSemaphore.acquire();
        try {

            byte[] minKey = rawMemoryIndex.minKey();
            byte[] maxKey = rawMemoryIndex.maxKey();

            if (indexes.isEmpty()) {
                File newStripeRoot = new File(root, String.valueOf(largestStripeId.incrementAndGet()));

                FileBackMergableIndexs index = new FileBackMergableIndexs(destroy, largestIndexId, newStripeRoot, new MergeableIndexes(), entriesBetweenLeaps, useMemMap);
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
            splittingSemaphore.release();
        }
    }

    public boolean tx(byte[] from, byte[] to, ReaderTx tx) throws Exception {
        if (indexes.isEmpty()) {
            return tx.tx(new ReadIndex[0]);
        }
        splittingSemaphore.acquire();
        try {
            SortedMap<byte[], FileBackMergableIndexs> map;
            if (from != null && to != null) {
                map = indexes.tailMap(from);
                if (map.isEmpty()) {
                    map = indexes.tailMap(indexes.lastKey());
                } else {
                    byte[] priorKey = indexes.lowerKey(map.firstKey());
                    if (priorKey != null) {
                        map = indexes.subMap(priorKey, to);
                    }
                }
            } else if (from != null) {
                map = indexes.tailMap(from);
                if (map.isEmpty()) {
                    map = indexes.tailMap(indexes.lastKey());
                } else {
                    byte[] priorKey = indexes.lowerKey(map.firstKey());
                    if (priorKey != null) {
                        map = indexes.tailMap(priorKey);
                    }
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
        } finally {
            splittingSemaphore.release();
        }

    }

    public int hasMergeDebt(int minMergeDebt) throws IOException {
        int maxDebt = 0;
        for (FileBackMergableIndexs index : indexes.values()) {
            maxDebt = Math.max(index.hasMergeDebt(minMergeDebt), maxDebt);
        }
        return maxDebt;
    }

    public List<MergeableIndexes.Merger> buildMerger(int minimumRun,
        boolean fsync) throws Exception {
        List<MergeableIndexes.Merger> mergers = new ArrayList<>();
        for (FileBackMergableIndexs index : indexes.values()) {
            MergeableIndexes.Merger merger = index.buildMerger(minimumRun, fsync);
            if (merger != null) {
                mergers.add(merger);
            }
        }
        return mergers;
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
