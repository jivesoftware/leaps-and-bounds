package com.jivesoftware.os.lab;

/**
 *
 * @author jonathan.colt
 */
public class LabWALConfig {

    public final String walName;
    public final String metaName;
    public final long maxWALSizeInBytes;
    public final long maxEntriesPerWAL;
    public final long maxEntrySizeInBytes;
    public final long maxValueIndexHeapPressureOverride;


    public LabWALConfig(String walName, String metaName, long maxWALSizeInBytes, long maxEntriesPerWAL, long maxEntrySizeInBytes,
        long maxValueIndexHeapPressureOverride
    ) {
        this.walName = walName;
        this.metaName = metaName;
        this.maxWALSizeInBytes = maxWALSizeInBytes;
        this.maxEntriesPerWAL = maxEntriesPerWAL;
        this.maxEntrySizeInBytes = maxEntrySizeInBytes;
        this.maxValueIndexHeapPressureOverride = maxValueIndexHeapPressureOverride;
    }

}
