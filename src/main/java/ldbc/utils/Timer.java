/*
 * Copyright © 2017-2018 Alain Kägi
 */

package ldbc.queries;

import java.io.PrintStream;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

/**
 * The Timer class defines functions to measure and to print elapsed time.
 */
public class Timer {

    private ThreadMXBean timeBean;
    private long userStartTime;
    private long userStopTime;
    private long totalStartTime;
    private long totalStopTime;

    /** Construct a time object. */
    public Timer() {
        timeBean = ManagementFactory.getThreadMXBean();
    }

    /** Start an execution time measurement. */
    public void start()
    {
        userStartTime = timeBean.getCurrentThreadUserTime();
        totalStartTime = timeBean.getCurrentThreadCpuTime();
    }

    /** End an execution time measurement. */
    public void stop()
    {
        userStopTime = timeBean.getCurrentThreadUserTime();
        totalStopTime = timeBean.getCurrentThreadCpuTime();
    }

    /**
     * Output timing information to a print stream.
     * @param o  The output stream to which the information should be printed
     */
    public void print(PrintStream o) {
        long userTime = (userStopTime - userStartTime)/1000;
        long totalTime = (totalStopTime - totalStartTime)/1000;
        long sysTime = totalTime - userTime;
        float upercent = 100 * (userTime / (float)totalTime);
        float spercent = 100 * (sysTime / (float)totalTime);
        o.println("Elapsed time is " + totalTime + " microseconds");
        o.println("User time is " + userTime + " microseconds (" + upercent + "%)");
        o.println("System time is " + sysTime + " microseconds (" + spercent + "%)");
    }

}
