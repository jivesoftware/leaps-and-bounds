package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.io.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public class PointGetRaw implements GetRaw {

    private final ReadIndex[] indexs;
    private final boolean leveragePointHashIndex;
    private boolean result;

    public PointGetRaw(ReadIndex[] indexes, boolean leveragePointHashIndex) {
        this.indexs = indexes;
        this.leveragePointHashIndex = leveragePointHashIndex;
    }

    @Override
    public boolean get(byte[] key, BolBuffer entryBuffer, BolBuffer entryKeyBuffer, RawEntryStream stream) throws Exception {
        for (ReadIndex index : indexs) {
            GetRaw pointGet = index.get(new ActiveScan(leveragePointHashIndex));
            if (pointGet.get(key, entryBuffer, entryKeyBuffer, stream)) {
                result = pointGet.result();
                return result;
            }
        }
        result = stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, null);
        return result;
    }

    @Override
    public boolean result() {
        return result;
    }

}
