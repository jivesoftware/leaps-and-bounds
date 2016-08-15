package com.jivesoftware.os.lab.wal;

import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.collections.bah.BAHEqualer;
import com.jivesoftware.os.jive.utils.collections.bah.BAHMapState;
import com.jivesoftware.os.jive.utils.collections.bah.BAHash;
import com.jivesoftware.os.jive.utils.collections.bah.BAHasher;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.guts.IndexFile;
import com.jivesoftware.os.lab.io.api.IReadable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author jonathan.colt
 */
public class WAL {

    private final byte ROW = 0;
    private final byte WRITE_ISOLATION = 1;
    private final byte COMMIT_ISOLATION = 2;

    private final int[] MAGIC = {
        351126232,
        759984878,
        266850631
    };

    private final File walRoot;
    private final long maxRowSizeInBytes;
    private final List<ActiveWAL> openWALs = Lists.newArrayList();
    private final AtomicReference<ActiveWAL> activeWAL = new AtomicReference<>();
    private final AtomicLong walIdProvider = new AtomicLong();

    public WAL(File walRoot, long maxRowSizeInBytes) throws IOException {
        this.walRoot = walRoot;
        this.maxRowSizeInBytes = maxRowSizeInBytes;

    }

    public void open(LABEnvironment environment) throws IOException {

        File indexRoot = new File(walRoot, "wals");
        File[] walFiles = indexRoot.listFiles();
        Arrays.sort(walFiles, (wal1, wal2) -> Long.compare(Long.parseLong(wal1.getName()), Long.parseLong(wal2.getName())));

        for (File walFile : walFiles) {
            Long id = Long.parseLong(walFile.getName());

            IndexFile indexFile = new IndexFile(walFile, "rw", false);
            IReadable reader = indexFile.reader(null, indexFile.length(), true);
            reader.seek(0);

            int rowType = reader.read();
            if (rowType < 0 || rowType > 127) {
                throw new IllegalStateException("expected a row type greater than -1 and less than 128 but encountered " + rowType);
            }
            int magic = reader.readInt();
            if (magic != MAGIC[rowType]) {
                throw new IllegalStateException("expected a magic " + MAGIC[rowType] + " but encountered " + magic);
            }
            int length = reader.readInt();
            if (length >= maxRowSizeInBytes) {
                throw new IllegalStateException("expected row length less than " + maxRowSizeInBytes + " but encountered " + length);
            }



        }

    }

    public void append(byte[] valueIndexId, long version, byte[] entry) throws Exception {

    }

    public void flush(byte[] valueIndexId, long version, boolean fsync) {

    }

    private static final class ActiveWAL {

        private final IndexFile wal;
        private final BAHash<Long> valueIndexIds;

        public ActiveWAL(IndexFile wal) {
            this.wal = wal;
            this.valueIndexIds = new BAHash<>(new BAHMapState<>(10, true, BAHMapState.NIL), BAHasher.SINGLETON, BAHEqualer.SINGLETON);
        }

        public void append(byte[] valueIndexId, long version, boolean fsync, byte[] entry) throws Exception {
            
        }

        public void flushed(byte[] valueIndexId, long version) {

        }

    }
}
