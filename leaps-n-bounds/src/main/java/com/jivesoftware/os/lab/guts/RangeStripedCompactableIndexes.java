package com.jivesoftware.os.lab.guts;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.guts.api.MergerBuilder;
import com.jivesoftware.os.lab.guts.api.NextRawEntry;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.guts.api.SplitterBuilder;
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
 * @author jonathan.colt
 */
public class RangeStripedCompactableIndexes {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AtomicLong largestStripeId = new AtomicLong();
    private final AtomicLong largestIndexId = new AtomicLong();
    private final ExecutorService destroy;
    private final File root;
    private final String indexName;
    private final boolean useMemMap;
    private final int entriesBetweenLeaps;
    private final Object copyIndexOnWrite = new Object();
    private volatile ConcurrentSkipListMap<byte[], FileBackMergableIndexs> indexes;
    private final long splitWhenKeysTotalExceedsNBytes;
    private final long splitWhenValuesTotalExceedsNBytes;
    private final long splitWhenValuesAndKeysTotalExceedsNBytes;
    private final Rawhide rawhide;
    private final int concurrency;
    private final Semaphore appendSemaphore = new Semaphore(Short.MAX_VALUE, true);

    public RangeStripedCompactableIndexes(ExecutorService destroy,
        File root,
        String indexName,
        boolean useMemMap,
        int entriesBetweenLeaps,
        long splitWhenKeysTotalExceedsNBytes,
        long splitWhenValuesTotalExceedsNBytes,
        long splitWhenValuesAndKeysTotalExceedsNBytes,
        Rawhide rawhide,
        int concurrency) throws Exception {

        this.destroy = destroy;
        this.root = root;
        this.indexName = indexName;
        this.useMemMap = useMemMap;
        this.entriesBetweenLeaps = entriesBetweenLeaps;
        this.splitWhenKeysTotalExceedsNBytes = splitWhenKeysTotalExceedsNBytes;
        this.splitWhenValuesTotalExceedsNBytes = splitWhenValuesTotalExceedsNBytes;
        this.splitWhenValuesAndKeysTotalExceedsNBytes = splitWhenValuesAndKeysTotalExceedsNBytes;
        this.rawhide = rawhide;
        this.concurrency = concurrency;
        this.indexes = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        File indexRoot = new File(root, indexName);
        File[] stripeDirs = indexRoot.listFiles();
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
                        indexName,
                        stripeId,
                        entry.getValue().mergeableIndexes));
                }
            }
        }
    }


    public TimestampAndVersion maxTimeStampAndVersion() {
        TimestampAndVersion max = TimestampAndVersion.NULL;
        for (FileBackMergableIndexs indexs : indexes.values()) {
            TimestampAndVersion other = indexs.compactableIndexes.maxTimeStampAndVersion();
            if (rawhide.isNewerThan(other.maxTimestamp, other.maxTimestampVersion, max.maxTimestamp, max.maxTimestampVersion)) {
                max = other;
            }
        }
        return max;
    }

    @Override
    public String toString() {
        return "RangeStripedCompactableIndexes{"
            + "largestStripeId=" + largestStripeId
            + ", largestIndexId=" + largestIndexId
            + ", root=" + root
            + ", indexName=" + indexName
            + ", useMemMap=" + useMemMap
            + ", entriesBetweenLeaps=" + entriesBetweenLeaps
            + ", splitWhenKeysTotalExceedsNBytes=" + splitWhenKeysTotalExceedsNBytes
            + ", splitWhenValuesTotalExceedsNBytes=" + splitWhenValuesTotalExceedsNBytes
            + ", splitWhenValuesAndKeysTotalExceedsNBytes=" + splitWhenValuesAndKeysTotalExceedsNBytes
            + ", concurrency=" + concurrency
            + '}';
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
                CompactableIndexes mergeableIndexes = new CompactableIndexes(rawhide);
                KeyRange keyRange = null;
                for (IndexRangeId range : ranges) {
                    File file = range.toFile(activeDir);
                    if (file.length() == 0) {
                        file.delete();
                        continue;
                    }
                    IndexFile indexFile = new IndexFile(file, "rw", useMemMap);
                    LeapsAndBoundsIndex lab = new LeapsAndBoundsIndex(destroy, range, indexFile, rawhide, concurrency);
                    if (lab.minKey() != null && lab.maxKey() != null) {
                        if (keyRange == null) {
                            keyRange = new KeyRange(lab.minKey(), lab.maxKey());
                        } else {
                            keyRange.join(new KeyRange(lab.minKey(), lab.maxKey()));
                        }
                        if (!mergeableIndexes.append(lab)) {
                            throw new RuntimeException("Bueller");
                        }
                    } else {
                        lab.closeReadable();
                        indexFile.getFile().delete();
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

        final ExecutorService destroy;
        final AtomicLong largestStripeId;
        final AtomicLong largestIndexId;

        final File root;
        final String indexName;
        final long stripeId;
        final CompactableIndexes compactableIndexes;

        public FileBackMergableIndexs(ExecutorService destroy,
            AtomicLong largestStripeId,
            AtomicLong largestIndexId,
            File root,
            String indexName,
            long stripeId,
            CompactableIndexes mergeableIndexes) throws IOException {

            this.destroy = destroy;
            this.largestStripeId = largestStripeId;
            this.largestIndexId = largestIndexId;
            this.compactableIndexes = mergeableIndexes;

            this.root = root;
            this.indexName = indexName;
            this.stripeId = stripeId;

            File indexRoot = new File(root, indexName);
            File stripeRoot = new File(indexRoot, String.valueOf(stripeId));

            File mergingRoot = new File(stripeRoot, "merging");
            File commitingRoot = new File(stripeRoot, "commiting");
            File splittingRoot = new File(stripeRoot, "splitting");

            FileUtils.deleteQuietly(mergingRoot);
            FileUtils.deleteQuietly(commitingRoot);
            FileUtils.deleteQuietly(splittingRoot);
        }

        void append(RawMemoryIndex rawMemoryIndex, byte[] minKey, byte[] maxKey) throws Exception {

            LeapsAndBoundsIndex lab = flushMemoryIndexToDisk(rawMemoryIndex, minKey, maxKey, largestIndexId.incrementAndGet(), 0, useMemMap);
            compactableIndexes.append(lab);
        }

        private LeapsAndBoundsIndex flushMemoryIndexToDisk(RawMemoryIndex index,
            byte[] minKey,
            byte[] maxKey,
            long nextIndexId,
            int generation,
            boolean fsync) throws Exception {

            File indexRoot = new File(root, indexName);
            File stripeRoot = new File(indexRoot, String.valueOf(stripeId));
            File activeRoot = new File(stripeRoot, "active");
            File commitingRoot = new File(stripeRoot, "commiting");
            FileUtils.forceMkdir(commitingRoot);

            LOG.debug("Commiting memory index ({}) to on disk index: {}", index.count(), activeRoot);

            int maxLeaps = IndexUtil.calculateIdealMaxLeaps(index.count(), entriesBetweenLeaps);
            IndexRangeId indexRangeId = new IndexRangeId(nextIndexId, nextIndexId, generation);
            File commitingIndexFile = indexRangeId.toFile(commitingRoot);
            FileUtils.deleteQuietly(commitingIndexFile);
            IndexFile indexFile = new IndexFile(commitingIndexFile, "rw", useMemMap);
            LABAppendableIndex appendableIndex = new LABAppendableIndex(indexRangeId, indexFile, maxLeaps, entriesBetweenLeaps, rawhide);
            appendableIndex.append((stream) -> {
                ReadIndex reader = index.acquireReader();
                try {
                    NextRawEntry rangeScan = reader.rangeScan(minKey, maxKey);
                    while (rangeScan.next(stream) == NextRawEntry.Next.more) ;
                    return true;
                } finally {
                    reader.release();
                }
            });
            appendableIndex.closeAppendable(fsync);

            File commitedIndexFile = indexRangeId.toFile(activeRoot);
            return moveIntoPlace(commitingIndexFile, commitedIndexFile, indexRangeId);
        }

        private LeapsAndBoundsIndex moveIntoPlace(File commitingIndexFile, File commitedIndexFile, IndexRangeId indexRangeId) throws Exception {
            FileUtils.moveFile(commitingIndexFile, commitedIndexFile);
            LeapsAndBoundsIndex reopenedIndex = new LeapsAndBoundsIndex(destroy,
                indexRangeId, new IndexFile(commitedIndexFile, "r", useMemMap), rawhide, concurrency);
            reopenedIndex.flush(true);  // Sorry
            // TODO Files.fsync index when java 9 supports it.
            return reopenedIndex;
        }

        boolean tx(ReaderTx tx) throws Exception {
            return compactableIndexes.tx(tx);
        }

        long count() throws Exception {
            return compactableIndexes.count();
        }

        void close() throws Exception {
            compactableIndexes.close();
        }

        int debt(int minMergeDebt) {
            return compactableIndexes.debt(minMergeDebt);
        }

        Callable<Void> compactor(int minMergeDebt, boolean fsync) throws Exception {
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

            File indexRoot = new File(root, indexName);
            File stripeRoot = new File(indexRoot, String.valueOf(stripeId));
            File mergingRoot = new File(stripeRoot, "merging");
            File commitingRoot = new File(stripeRoot, "commiting");
            File splittingRoot = new File(stripeRoot, "splitting");
            FileUtils.forceMkdir(mergingRoot);
            FileUtils.forceMkdir(commitingRoot);
            FileUtils.forceMkdir(splittingRoot);

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
                        LOG.info("Creating new index for split: {}", splittingIndexFile);
                        IndexFile indexFile = new IndexFile(splittingIndexFile, "rw", useMemMap);
                        LABAppendableIndex writeLeapsAndBoundsIndex = new LABAppendableIndex(id,
                            indexFile,
                            maxLeaps,
                            entriesBetweenLeaps,
                            rawhide);
                        return writeLeapsAndBoundsIndex;
                    }, (IndexRangeId id, long worstCaseCount) -> {
                        int maxLeaps = IndexUtil.calculateIdealMaxLeaps(worstCaseCount, entriesBetweenLeaps);
                        File splitIntoDir = new File(splittingRoot, String.valueOf(nextStripeIdRight));
                        FileUtils.deleteQuietly(splitIntoDir);
                        FileUtils.forceMkdir(splitIntoDir);
                        File splittingIndexFile = id.toFile(splitIntoDir);
                        LOG.info("Creating new index for split: {}", splittingIndexFile);
                        IndexFile indexFile = new IndexFile(splittingIndexFile, "rw", useMemMap);
                        LABAppendableIndex writeLeapsAndBoundsIndex = new LABAppendableIndex(id,
                            indexFile,
                            maxLeaps,
                            entriesBetweenLeaps,
                            rawhide);
                        return writeLeapsAndBoundsIndex;
                    }, (ids) -> {
                        File left = new File(indexRoot, String.valueOf(nextStripeIdLeft));
                        File leftActive = new File(left, "active");
                        File right = new File(indexRoot, String.valueOf(nextStripeIdRight));
                        File rightActive = new File(right, "active");
                        LOG.info("Commiting split:{} became left:{} right:{}", stripeRoot, left, right);

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
                                        new FileBackMergableIndexs(destroy, largestStripeId, largestIndexId, root, indexName, nextStripeIdLeft,
                                            leftStripe.mergeableIndexes));
                                }

                                if (rightStripe != null && rightStripe.keyRange != null && rightStripe.keyRange.start != null) {
                                    copyOfIndexes.put(rightStripe.keyRange.start,
                                        new FileBackMergableIndexs(destroy, largestStripeId, largestIndexId, root, indexName, nextStripeIdRight,
                                            rightStripe.mergeableIndexes));
                                }
                                indexes = copyOfIndexes;
                            }
                            compactableIndexes.destroy();
                            FileUtils.deleteQuietly(mergingRoot);
                            FileUtils.deleteQuietly(commitingRoot);
                            FileUtils.deleteQuietly(splittingRoot);

                            LOG.info("Completed split:{} became left:{} right:{}", stripeRoot, left, right);
                            return null;
                        } catch (Exception x) {
                            FileUtils.deleteQuietly(left);
                            FileUtils.deleteQuietly(right);
                            LOG.error("Failed to split:{} became left:{} right:{}", new Object[]{stripeRoot, left, right}, x);
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
            File indexRoot = new File(root, indexName);
            File stripeRoot = new File(indexRoot, String.valueOf(stripeId));
            File activeRoot = new File(stripeRoot, "active");
            File mergingRoot = new File(stripeRoot, "merging");
            FileUtils.forceMkdir(mergingRoot);
            return callback.build(minimumRun, fsync, (id, count) -> {
                int maxLeaps = IndexUtil.calculateIdealMaxLeaps(count, entriesBetweenLeaps);
                File mergingIndexFile = id.toFile(mergingRoot);
                FileUtils.deleteQuietly(mergingIndexFile);
                IndexFile indexFile = new IndexFile(mergingIndexFile, "rw", useMemMap);
                LABAppendableIndex writeLeapsAndBoundsIndex = new LABAppendableIndex(id,
                    indexFile,
                    maxLeaps,
                    entriesBetweenLeaps,
                    rawhide);
                return writeLeapsAndBoundsIndex;
            }, (ids) -> {
                File mergedIndexFile = ids.get(0).toFile(mergingRoot);
                File file = ids.get(0).toFile(activeRoot);
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
                long stripeId = largestStripeId.incrementAndGet();
                FileBackMergableIndexs index = new FileBackMergableIndexs(destroy,
                    largestStripeId,
                    largestIndexId,
                    root,
                    indexName,
                    stripeId,
                    new CompactableIndexes(rawhide));
                synchronized (copyIndexOnWrite) {
                    ConcurrentSkipListMap<byte[], FileBackMergableIndexs> copyOfIndexes = new ConcurrentSkipListMap<>(
                        UnsignedBytes.lexicographicalComparator());
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

    public boolean tx(byte[] from,
        byte[] to,
        long newerThanTimestamp,
        long newerThanTimestampVersion,
        ReaderTx tx) throws Exception {

        if (indexes.isEmpty()) {
            return tx.tx(new ReadIndex[0]);
        }
        SortedMap<byte[], FileBackMergableIndexs> map;
        if (from != null && to != null) {
            byte[] floorKey = indexes.floorKey(from);
            if (floorKey != null) {
                map = indexes.subMap(floorKey, true, to, Arrays.equals(from, to));
            } else {
                map = indexes.headMap(to, Arrays.equals(from, to));
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

        if (map.isEmpty()) {
            return tx.tx(new ReadIndex[0]);
        } else {
            for (FileBackMergableIndexs index : map.values()) {
                TimestampAndVersion timestampAndVersion = index.compactableIndexes.maxTimeStampAndVersion();
                if (rawhide.mightContain(timestampAndVersion.maxTimestamp,
                    timestampAndVersion.maxTimestampVersion,
                    newerThanTimestamp,
                    newerThanTimestampVersion)) {
                    if (!index.tx(tx)) {
                        return false;
                    }
                }
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
