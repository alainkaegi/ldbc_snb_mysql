/**
 * A utility for timing a benchmark.
 *
 * Copyright © 2017 Alain Kägi
 */

package ldbc.queries;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

/** A convenient class to measure and to print elapsed time. */
public class Timer {

    private ThreadMXBean timeBean;
    private long userStartTime;
    private long userStopTime;
    private long totalStartTime;
    private long totalStopTime;

    public Timer() {
        timeBean = ManagementFactory.getThreadMXBean();
    }

    public void start()
    {
        userStartTime = timeBean.getCurrentThreadUserTime();
        totalStartTime = timeBean.getCurrentThreadCpuTime();
    }

    public void stop()
    {
        userStopTime = timeBean.getCurrentThreadUserTime();
        totalStopTime = timeBean.getCurrentThreadCpuTime();
    }

    public void print() {
        long userTime = (userStopTime - userStartTime)/1000;
        long totalTime = (totalStopTime - totalStartTime)/1000;
        long sysTime = totalTime - userTime;
        float upercent = 100 * (userTime / (float)totalTime);
        float spercent = 100 * (sysTime / (float)totalTime);
        System.out.println("Elapsed time is " + totalTime + " microseconds");
        System.out.println("User time is " + userTime + " microseconds (" + upercent + "%)");
        System.out.println("System time is " + sysTime + " microseconds (" + spercent + "%)");
    }

}
