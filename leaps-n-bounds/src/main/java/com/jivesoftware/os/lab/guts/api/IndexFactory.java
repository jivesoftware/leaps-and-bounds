package com.jivesoftware.os.lab.guts.api;

import com.jivesoftware.os.lab.guts.IndexRangeId;
import com.jivesoftware.os.lab.guts.LABAppenableIndex;

/**
 *
 * @author jonathan.colt
 */
public interface IndexFactory {

    LABAppenableIndex createIndex(IndexRangeId id, long worstCaseCount) throws Exception;
}
