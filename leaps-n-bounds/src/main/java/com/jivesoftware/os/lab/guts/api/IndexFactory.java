package com.jivesoftware.os.lab.guts.api;

import com.jivesoftware.os.lab.guts.IndexRangeId;
import com.jivesoftware.os.lab.guts.LABAppendableIndex;

/**
 *
 * @author jonathan.colt
 */
public interface IndexFactory {

    LABAppendableIndex createIndex(IndexRangeId id, long worstCaseCount) throws Exception;
}
