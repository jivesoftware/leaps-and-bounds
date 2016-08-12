package com.jivesoftware.os.lab;

import com.jivesoftware.os.lab.api.LABIndexClosedException;
import com.jivesoftware.os.lab.api.LABIndexCorruptedException;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final AtomicBoolean running = new AtomicBoolean();

    public LabHeapPressure(long maxHeapPressureInBytes, AtomicLong globalHeapCostInBytes) {
        this.maxHeapPressureInBytes = maxHeapPressureInBytes;
        this.globalHeapCostInBytes = globalHeapCostInBytes;
    }

    public AtomicLong globalHeapCostInBytes() {
        return globalHeapCostInBytes;
    }

    void commitIfNecessary(LAB lab, long labMaxHeapPressureInBytes, boolean fsyncOnFlush) throws Exception {
        if (lab.approximateHeapPressureInBytes() > labMaxHeapPressureInBytes) {
            LOG.inc("lab>commit>instance>" + fsyncOnFlush);
            labs.remove(lab);
            lab.commit(fsyncOnFlush, false); // todo config
        } else {
            labs.compute(lab, (LAB t, Boolean u) -> {
                return u == null ? fsyncOnFlush : (boolean) u || fsyncOnFlush;
            });
        }
        long globalHeap = globalHeapCostInBytes.get();
        LOG.set(ValueType.VALUE, "lab>heap>pressure", globalHeap);
        LOG.set(ValueType.VALUE, "lab>commitable", labs.size());
        if (globalHeap > maxHeapPressureInBytes && running.compareAndSet(false, true)) {
            lab.schedule.submit(() -> {
                try {
                    if (globalHeapCostInBytes.get() < maxHeapPressureInBytes) {
                        LOG.inc("lab>commit>global>skip");
                        return null;
                    }
                    LAB[] keys = labs.keySet().toArray(new LAB[0]);
                    Arrays.sort(keys, (LAB o1, LAB o2) -> {
                        return -Long.compare(o1.approximateHeapPressureInBytes(), o2.approximateHeapPressureInBytes());
                    });
                    int i = 0;
                    while (i < keys.length && globalHeapCostInBytes.get() > maxHeapPressureInBytes) {
                        Boolean efsyncOnFlush = labs.remove(keys[i]);
                        if (efsyncOnFlush != null) {
                            try {
                                LOG.inc("lab>commit>global>" + efsyncOnFlush);
                                LOG.set(ValueType.VALUE, "lab>commitable", labs.size());
                                LOG.debug("Forcing flush due to heap pressure. lab:{}", lab);
                                keys[i].commit(efsyncOnFlush, false); // todo config
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
                    return null;
                } finally {
                    running.set(false);
                }
            });
        }
    }

    void close(LAB lab) {
        labs.remove(lab);
    }

}
