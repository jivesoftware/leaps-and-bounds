package com.jivesoftware.os.lab.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
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
    public final int entryLengthPower;
    public final double hashIndexLoadFactor;

    @JsonCreator
    public ValueIndexConfig(@JsonProperty("primaryName") String primaryName,
        @JsonProperty("entriesBetweenLeaps") int entriesBetweenLeaps,
        @JsonProperty("maxHeapPressureInBytes") long maxHeapPressureInBytes,
        @JsonProperty("splitWhenKeysTotalExceedsNBytes") long splitWhenKeysTotalExceedsNBytes,
        @JsonProperty("splitWhenValuesTotalExceedsNBytes") long splitWhenValuesTotalExceedsNBytes,
        @JsonProperty("splitWhenValuesAndKeysTotalExceedsNBytes") long splitWhenValuesAndKeysTotalExceedsNBytes,
        @JsonProperty("formatTransformerProviderName") String formatTransformerProviderName,
        @JsonProperty("rawhideName") String rawhideName,
        @JsonProperty("rawEntryFormatName") String rawEntryFormatName,
        @JsonProperty("entryLengthPower") int entryLengthPower,
        @JsonProperty("hashIndexLoadFactor") double hashIndexLoadFactor
    ) {

        this.primaryName = primaryName;
        this.entriesBetweenLeaps = entriesBetweenLeaps;
        this.maxHeapPressureInBytes = maxHeapPressureInBytes;
        this.splitWhenKeysTotalExceedsNBytes = splitWhenKeysTotalExceedsNBytes;
        this.splitWhenValuesTotalExceedsNBytes = splitWhenValuesTotalExceedsNBytes;
        this.splitWhenValuesAndKeysTotalExceedsNBytes = splitWhenValuesAndKeysTotalExceedsNBytes;
        this.formatTransformerProviderName = formatTransformerProviderName;
        this.rawhideName = rawhideName;
        this.rawEntryFormatName = rawEntryFormatName;
        this.entryLengthPower = entryLengthPower;
        this.hashIndexLoadFactor = hashIndexLoadFactor;
    }

}
