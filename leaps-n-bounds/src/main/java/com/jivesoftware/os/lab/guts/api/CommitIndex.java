package com.jivesoftware.os.lab.guts.api;

import com.jivesoftware.os.lab.guts.IndexRangeId;
import com.jivesoftware.os.lab.guts.ReadOnlyIndex;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public interface CommitIndex {

    ReadOnlyIndex commit(List<IndexRangeId> ids) throws Exception;

}
