package com.jivesoftware.os.lab.api;

import com.jivesoftware.os.lab.IndexRangeId;
import com.jivesoftware.os.lab.LeapsAndBoundsIndex;
import com.jivesoftware.os.lab.WriteLeapsAndBoundsIndex;

/**
 *
 * @author jonathan.colt
 */
public interface CommitIndex {

    LeapsAndBoundsIndex commit(IndexRangeId id, WriteLeapsAndBoundsIndex index) throws Exception;

}
