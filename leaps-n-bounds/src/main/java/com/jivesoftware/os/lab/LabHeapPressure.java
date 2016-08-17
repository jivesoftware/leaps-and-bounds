package com.jivesoftware.os.lab;

import com.jivesoftware.os.lab.api.LABIndexClosedException;
import com.jivesoftware.os.lab.api.LABIndexCorruptedException;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
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
                    long debtInBytes = globalHeapCostInBytes.get() - maxHeapPressureInBytes;
                    if (debtInBytes <= 0) {
                        LOG.inc("lab>commit>global>skip");
                        return null;
                    }
                    LAB[] keys = labs.keySet().toArray(new LAB[0]);
                    long[] pressures = new long[keys.length];
                    for (int i = 0; i < keys.length; i++) {
                        pressures[i] = Long.MAX_VALUE - keys[i].approximateHeapPressureInBytes();
                    }
                    USort.mirrorSort(pressures, keys);

                    int i = 0;
                    while (i < keys.length && debtInBytes > 0) {
                        long pressure = Long.MAX_VALUE - pressures[i];
                        LOG.inc("lab>commit>global>pressure>" + UIO.chunkPower(pressure, 0));
                        debtInBytes -= pressure;
                        Boolean efsyncOnFlush = this.labs.remove(keys[i]);
                        if (efsyncOnFlush != null) {
                            try {
                                LOG.inc("lab>commit>global>" + efsyncOnFlush);
                                LOG.set(ValueType.VALUE, "lab>commitable", this.labs.size());
                                LOG.debug("Forcing flush due to heap pressure. lab:{}", lab);
                                keys[i].commit(efsyncOnFlush, false); // todo config
                            } catch (LABIndexCorruptedException | LABIndexClosedException x) {
                            } catch (Exception x) {
                                this.labs.compute(keys[i], (LAB t, Boolean u) -> {
                                    return u == null ? efsyncOnFlush : (boolean) u || efsyncOnFlush;
                                });
                                throw x;
                            }
                        }
                        i++;
                    }
                    LOG.inc("lab>commit>global>depth>" + UIO.chunkPower(i, 0));
                    return null;
                } finally {
                    running.set(false);
                }
            });
        }
    }

    private static class Key {
        LAB key;
        long pressure;

        public Key(LAB key, long pressure) {
            this.key = key;
            this.pressure = pressure;
        }
    }

    void close(LAB lab) {
        labs.remove(lab);
    }

}
