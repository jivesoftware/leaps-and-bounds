package com.jivesoftware.os.lab;

import com.jivesoftware.os.lab.api.LABIndexClosedException;
import com.jivesoftware.os.lab.api.LABIndexCorruptedException;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author jonathan.colt
 */
public class LabHeapPressure {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final long maxHeapPressureInBytes;
    private final AtomicLong globalHeapCostInBytes;
    private final Map<LAB, Boolean> labs = new ConcurrentHashMap<>();

    public LabHeapPressure(long maxHeapPressureInBytes, AtomicLong globalHeapCostInBytes) {
        this.maxHeapPressureInBytes = maxHeapPressureInBytes;
        this.globalHeapCostInBytes = globalHeapCostInBytes;
    }

    public AtomicLong globalHeapCostInBytes() {
        return globalHeapCostInBytes;
    }

    void commitIfNecessary(LAB lab, long labMaxHeapPressureInBytes, boolean fsyncOnFlush) throws Exception {
        if (lab.approximateHeapPressureInBytes() > labMaxHeapPressureInBytes) {
            labs.remove(lab);
            lab.commit(fsyncOnFlush);
        } else {
            labs.compute(lab, (LAB t, Boolean u) -> {
                return u == null ? fsyncOnFlush : (boolean) u || fsyncOnFlush;
            });
        }

        if (globalHeapCostInBytes.get() > maxHeapPressureInBytes) {
            LAB[] keys = labs.keySet().toArray(new LAB[0]);
            Arrays.sort(keys, (LAB o1, LAB o2) -> {
                return -Long.compare(o1.approximateHeapPressureInBytes(), o2.approximateHeapPressureInBytes());
            });
            int i = 0;
            while (i < keys.length && globalHeapCostInBytes.get() > maxHeapPressureInBytes) {
                Boolean efsyncOnFlush = labs.remove(keys[i]);
                if (efsyncOnFlush != null) {
                    try {
                        LOG.info("Forcing flush due to heap pressure. lab:{}", lab);
                        keys[i].commit(efsyncOnFlush);
                    } catch (LABIndexCorruptedException | LABIndexClosedException x) {
                    } catch (Exception x) {
                        labs.compute(keys[i], (LAB t, Boolean u) -> {
                            return u == null ? efsyncOnFlush : (boolean) u || efsyncOnFlush;
                        });
                        throw x;
                    }
                }
                i++;
            }
        }
    }

    void close(LAB lab) {
        labs.remove(lab);
    }

}
