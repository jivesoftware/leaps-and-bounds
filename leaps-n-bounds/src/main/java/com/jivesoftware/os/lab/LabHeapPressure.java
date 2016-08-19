package com.jivesoftware.os.lab;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.lab.api.LABIndexClosedException;
import com.jivesoftware.os.lab.api.LABIndexCorruptedException;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author jonathan.colt
 */
public class LabHeapPressure {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ExecutorService schedule;
    private final String name;
    private final long maxHeapPressureInBytes;
    private final long blockOnHeapPressureInBytes;
    private final AtomicLong globalHeapCostInBytes;
    private final Map<LAB, Boolean> labs = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean();

    public LabHeapPressure(ExecutorService schedule,
        String name,
        long maxHeapPressureInBytes,
        long blockOnHeapPressureInBytes,
        AtomicLong globalHeapCostInBytes) {

        this.schedule = schedule;
        this.name = name;
        this.maxHeapPressureInBytes = maxHeapPressureInBytes;
        this.blockOnHeapPressureInBytes = blockOnHeapPressureInBytes;
        this.globalHeapCostInBytes = globalHeapCostInBytes;

        Preconditions.checkArgument(maxHeapPressureInBytes <= blockOnHeapPressureInBytes,
            "maxHeapPressureInBytes must be less than or equal to blockOnHeapPressureInBytes");
    }

    public AtomicLong globalHeapCostInBytes() {
        return globalHeapCostInBytes;
    }

    public void change(long delta) {
        globalHeapCostInBytes.addAndGet(delta);
        if (delta < 0) {
            synchronized (globalHeapCostInBytes) {
                globalHeapCostInBytes.notifyAll();
            }
        }
    }

    void commitIfNecessary(LAB lab, long labMaxHeapPressureInBytes, boolean fsyncOnFlush) throws Exception {
        if (lab.approximateHeapPressureInBytes() > labMaxHeapPressureInBytes) {
            LOG.inc("lab>pressure>commit>" + name);
            labs.remove(lab);
            lab.commit(fsyncOnFlush, false); // todo config
        } else {
            labs.compute(lab, (LAB t, Boolean u) -> {
                return u == null ? fsyncOnFlush : (boolean) u || fsyncOnFlush;
            });
        }
        long globalHeap = globalHeapCostInBytes.get();
        LOG.set(ValueType.VALUE, "lab>heap>pressure>" + name, globalHeap);
        LOG.set(ValueType.VALUE, "lab>commitable>" + name, labs.size());
        if (globalHeap > maxHeapPressureInBytes) {

            if (running.compareAndSet(false, true)) {
                schedule.submit(() -> {
                    try {
                        LOG.incAtomic("lab>heap>flushing>" + name);
                        long debtInBytes = globalHeapCostInBytes.get() - maxHeapPressureInBytes;
                        if (debtInBytes <= 0) {
                            LOG.inc("lab>commit>global>skip>" + name);
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
                            LOG.inc("lab>commit>global>pressure>" + name + ">" + UIO.chunkPower(pressure, 0));
                            debtInBytes -= pressure;
                            Boolean efsyncOnFlush = this.labs.remove(keys[i]);
                            if (efsyncOnFlush != null) {
                                try {
                                    LOG.inc("lab>global>pressure>commit>" + name);
                                    LOG.set(ValueType.VALUE, "lab>commitable>" + name, this.labs.size());
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
                        LOG.inc("lab>commit>global>depth>" + name + ">" + UIO.chunkPower(i, 0));
                        return null;
                    } finally {
                        running.set(false);
                        LOG.decAtomic("lab>heap>flushing>" + name);
                    }
                });
            }

            while (globalHeap > blockOnHeapPressureInBytes) {
                try {
                    LOG.incAtomic("lab>heap>blocking>" + name);
                    synchronized (globalHeapCostInBytes) {
                        globalHeapCostInBytes.wait();
                    }
                    globalHeap = globalHeapCostInBytes.get();
                } finally {
                    LOG.decAtomic("lab>heap>blocking>" + name);
                }
            }
        }
    }

    void close(LAB lab) {
        labs.remove(lab);
    }

}
