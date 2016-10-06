package com.jivesoftware.os.lab;

/**
 *
 * @author jonathan.colt
 */
public class LalWALConfig {

    public final String walName;
    public final String metaName;
    public final long maxWALSizeInBytes;
    public final long maxEntriesPerWAL;
    public final long maxEntrySizeInBytes;

    public LalWALConfig(String walName, String metaName, long maxWALSizeInBytes, long maxEntriesPerWAL, long maxEntrySizeInBytes) {
        this.walName = walName;
        this.metaName = metaName;
        this.maxWALSizeInBytes = maxWALSizeInBytes;
        this.maxEntriesPerWAL = maxEntriesPerWAL;
        this.maxEntrySizeInBytes = maxEntrySizeInBytes;
    }

}
