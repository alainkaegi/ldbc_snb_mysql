/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.queries;

import java.io.PrintStream;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

/**
 * The HeapUsage class defines functions to print heap usage.
 */
public class HeapUsage {

    /** Construct a heap usage monitoring object. */
    HeapUsage() {}

    /**
     * Print heap usage to a print stream.
     * @param o  The output stream to which the information should be printed
     */
    public void print(PrintStream o) {
        memBean = ManagementFactory.getMemoryMXBean();
        heap = memBean.getHeapMemoryUsage();
        o.println(String.format("Heap: Init: %,d, Used: %,d, Committed: %,d, Max.: %,d", heap.getInit(), heap.getUsed(), heap.getCommitted(), heap.getMax()));
        o.println(String.format("      Init: %s, Used: %s, Committed: %s, Max.: %s", initPercentageIncrease(), usedPercentageIncrease(), committedPercentageIncrease(), maxPercentageIncrease()));
        init = heap.getInit();
        used = heap.getUsed();
        committed = heap.getCommitted();
        max = heap.getMax();
    }

    private MemoryMXBean memBean;
    private MemoryUsage heap;

    private long init = -1;
    private long used = -1;
    private long committed = -1;
    private long max = -1;

    private String percentageIncrease(long base, long next) {
        if (base == -1 || next == -1)
            return "n/a";
        return String.format("%+,.1f%%", ((double)next/base - 1)*100.0);
    }

    private String initPercentageIncrease() {
        return percentageIncrease(init, heap.getInit());
    }

    private String usedPercentageIncrease() {
        return percentageIncrease(used, heap.getUsed());
    }

    private String committedPercentageIncrease() {
        return percentageIncrease(committed, heap.getCommitted());
    }

    private String maxPercentageIncrease() {
        return percentageIncrease(max, heap.getMax());
    }

}
