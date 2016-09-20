package com.jivesoftware.os.lab;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.lab.api.exceptions.LABClosedException;
import com.jivesoftware.os.lab.api.exceptions.LABCorruptedException;
import com.jivesoftware.os.lab.guts.USort;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
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
    private volatile boolean running = false;
    private final AtomicLong changed = new AtomicLong();
    private final AtomicLong waiting = new AtomicLong();

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

    public void change(long delta) {
        changed.incrementAndGet();
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

            synchronized (globalHeapCostInBytes) {
                waiting.incrementAndGet();
            }
            try {
                long version = changed.get();
                freeHeap();
                boolean nudgeFreeHeap = false;
                while (globalHeap > blockOnHeapPressureInBytes) {
                    LOG.debug("BLOCKING for heap to go down...{} > {}", globalHeap, blockOnHeapPressureInBytes);
                    try {
                        LOG.incAtomic("lab>heap>blocking>" + name);
                        synchronized (globalHeapCostInBytes) {
                            long got = changed.get();
                            if (version == got) {
                                long start = System.currentTimeMillis();
                                globalHeapCostInBytes.wait(60_000);
                                if (System.currentTimeMillis() - start > 60_000) {
                                    LOG.warn("Taking more than 60sec to free heap.");
                                    nudgeFreeHeap = true;
                                }
                            } else {
                                version = got;
                                nudgeFreeHeap = true;
                            }
                        }
                        if (nudgeFreeHeap) {
                            nudgeFreeHeap = false;
                            LOG.info("Nudging freeHeap()  {} > {}", globalHeap, blockOnHeapPressureInBytes);
                            version = changed.get();
                            freeHeap();
                            Thread.yield();
                        }

                        globalHeap = globalHeapCostInBytes.get();
                    } finally {
                        LOG.decAtomic("lab>heap>blocking>" + name);
                    }
                }
            } finally {
                waiting.decrementAndGet();
            }
        }
    }

    public void freeHeap() {
        synchronized (globalHeapCostInBytes) {
            if (running != true) {
                running = true;
                schedule.submit(() -> {
                    LOG.incAtomic("lab>heap>flushing>" + name);
                    while (true) {
                        try {
                            long debtInBytes = globalHeapCostInBytes.get() - maxHeapPressureInBytes;
                            if (debtInBytes <= 0) {
                                LOG.inc("lab>commit>global>skip>" + name);
                                boolean yieldAndContinue = false;
                                synchronized (globalHeapCostInBytes) {
                                    if (waiting.get() == 0) {
                                        LOG.decAtomic("lab>heap>flushing>" + name);
                                        running = false;
                                        return null;
                                    } else {
                                        yieldAndContinue = true;
                                    }
                                }
                                if (yieldAndContinue) {
                                    LOG.warn("yieldAndContinue debt:{} waiting:{}", new Object[]{debtInBytes, waiting.get()});
                                    Thread.yield();
                                    continue;
                                }
                            }
                            LAB[] keys = labs.keySet().toArray(new LAB[0]);
                            long[] pressures = new long[keys.length];
                            for (int i = 0; i < keys.length; i++) {
                                pressures[i] = Long.MAX_VALUE - keys[i].approximateHeapPressureInBytes();
                            }
                            USort.mirrorSort(pressures, keys);
                            if (keys.length == 0) {
                                LOG.error("LAB has a memory accounting leak. debt:{} waiting:{}", new Object[]{debtInBytes, waiting.get()});
                                Thread.sleep(1000);
                            }

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
                                        keys[i].commit(efsyncOnFlush, false); // todo config

                                    } catch (LABCorruptedException | LABClosedException x) {
                                        LOG.error("Failed to commit.", x);

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
                        } catch (InterruptedException ie) {
                            synchronized (globalHeapCostInBytes) {
                                running = false;
                            }
                            throw ie;
                        } catch (Exception x) {
                            LOG.warn("Free heap encountered an error.", x);
                            Thread.sleep(1000);
                        }
                        synchronized (globalHeapCostInBytes) {
                            if (waiting.get() == 0) {
                                LOG.decAtomic("lab>heap>flushing>" + name);
                                running = false;
                                return null;
                            }
                        }
                    }

                });
            }
        }
    }

    void close(LAB lab) {
        labs.remove(lab);
    }

}
