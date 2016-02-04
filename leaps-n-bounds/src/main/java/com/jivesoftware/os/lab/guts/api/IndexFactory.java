package com.jivesoftware.os.lab.guts.api;

import com.jivesoftware.os.lab.guts.IndexRangeId;
import com.jivesoftware.os.lab.guts.WriteLeapsAndBoundsIndex;

/**
 *
 * @author jonathan.colt
 */
public interface IndexFactory {

    WriteLeapsAndBoundsIndex createIndex(IndexRangeId id, long worstCaseCount) throws Exception;
}
