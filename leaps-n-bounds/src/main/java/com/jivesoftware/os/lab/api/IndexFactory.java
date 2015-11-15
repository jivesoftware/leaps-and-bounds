package com.jivesoftware.os.lab.api;

import com.jivesoftware.os.lab.IndexRangeId;
import com.jivesoftware.os.lab.WriteLeapsAndBoundsIndex;

/**
 *
 * @author jonathan.colt
 */
public interface IndexFactory {

    WriteLeapsAndBoundsIndex createIndex(IndexRangeId id, long worstCaseCount) throws Exception;
}
