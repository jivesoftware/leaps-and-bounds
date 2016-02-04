package com.jivesoftware.os.lab.guts.api;

import com.jivesoftware.os.lab.guts.IndexRangeId;
import com.jivesoftware.os.lab.guts.LeapsAndBoundsIndex;
import com.jivesoftware.os.lab.guts.WriteLeapsAndBoundsIndex;

/**
 *
 * @author jonathan.colt
 */
public interface CommitIndex {

    LeapsAndBoundsIndex commit(IndexRangeId id, WriteLeapsAndBoundsIndex index) throws Exception;

}
