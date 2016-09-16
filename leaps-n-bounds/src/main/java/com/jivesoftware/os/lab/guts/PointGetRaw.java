package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.ReadIndex;

/**
 *
 * @author jonathan.colt
 */
public class PointGetRaw implements GetRaw {

    private final ReadIndex[] indexs;
    private boolean result;

    public PointGetRaw(ReadIndex[] indexes) {
        this.indexs = indexes;
    }

    @Override
    public boolean get(byte[] key, BolBuffer entryBuffer, RawEntryStream stream) throws Exception {
        for (ReadIndex index : indexs) {
            GetRaw pointGet = index.get();
            if (pointGet.get(key, entryBuffer, stream)) {
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
