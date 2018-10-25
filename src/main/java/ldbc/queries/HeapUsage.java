package ldbc.queries;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

public class HeapUsage {

    static public void print() {
        memBean = ManagementFactory.getMemoryMXBean();
        heap = memBean.getHeapMemoryUsage();
        System.err.println(String.format("Heap: Init: %,d, Used: %,d, Committed: %,d, Max.: %,d",
                                         heap.getInit(), heap.getUsed(), heap.getCommitted(), heap.getMax()));
        System.err.println(String.format("      Init: %s, Used: %s, Committed: %s, Max.: %s", initPercentageIncrease(), usedPercentageIncrease(), committedPercentageIncrease(), maxPercentageIncrease()));
        init = heap.getInit();
        used = heap.getUsed();
        committed = heap.getCommitted();
        max = heap.getMax();
    }

    static MemoryMXBean memBean;
    static MemoryUsage heap;

    static long init = -1;
    static long used = -1;
    static long committed = -1;
    static long max = -1;

    static private String percentageIncrease(long base, long next) {
        if (base == -1 || next == -1)
            return "n/a";
        return String.format("%+,.1f%%", ((double)next/base - 1)*100.0);
    }

    static private String initPercentageIncrease() {
        return percentageIncrease(init, heap.getInit());
    }

    static private String usedPercentageIncrease() {
        return percentageIncrease(used, heap.getUsed());
    }

    static private String committedPercentageIncrease() {
        return percentageIncrease(committed, heap.getCommitted());
    }

    static private String maxPercentageIncrease() {
        return percentageIncrease(max, heap.getMax());
    }

}
