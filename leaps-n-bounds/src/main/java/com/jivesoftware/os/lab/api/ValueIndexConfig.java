package com.jivesoftware.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public class ValueIndexConfig {

    public final String primaryName;
    public final int entriesBetweenLeaps;
    public final long maxHeapPressureInBytes;
    public final long splitWhenKeysTotalExceedsNBytes;
    public final long splitWhenValuesTotalExceedsNBytes;
    public final long splitWhenValuesAndKeysTotalExceedsNBytes;
    public final String formatTransformerProviderName;
    public final String rawhideName;
    public final String rawEntryFormatName;

    public ValueIndexConfig(String primaryName,
        int entriesBetweenLeaps,
        long maxHeapPressureInBytes,
        long splitWhenKeysTotalExceedsNBytes,
        long splitWhenValuesTotalExceedsNBytes,
        long splitWhenValuesAndKeysTotalExceedsNBytes,
        String formatTransformerProviderName,
        String rawhideName,
        String rawEntryFormatName) {
        
        this.primaryName = primaryName;
        this.entriesBetweenLeaps = entriesBetweenLeaps;
        this.maxHeapPressureInBytes = maxHeapPressureInBytes;
        this.splitWhenKeysTotalExceedsNBytes = splitWhenKeysTotalExceedsNBytes;
        this.splitWhenValuesTotalExceedsNBytes = splitWhenValuesTotalExceedsNBytes;
        this.splitWhenValuesAndKeysTotalExceedsNBytes = splitWhenValuesAndKeysTotalExceedsNBytes;
        this.formatTransformerProviderName = formatTransformerProviderName;
        this.rawhideName = rawhideName;
        this.rawEntryFormatName = rawEntryFormatName;
    }

}
