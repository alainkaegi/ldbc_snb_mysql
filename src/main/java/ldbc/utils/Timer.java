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
    private long startTime;
    private long stopTime;
    private long threadUserStartTime;
    private long threadUserStopTime;
    private long threadTotalStartTime;
    private long threadTotalStopTime;

    /** Construct a time object. */
    public Timer() {
        timeBean = ManagementFactory.getThreadMXBean();
    }

    /** Start an execution time measurement. */
    public void start()
    {
        startTime = System.nanoTime();
        threadUserStartTime = timeBean.getCurrentThreadUserTime();
        threadTotalStartTime = timeBean.getCurrentThreadCpuTime();
    }

    /** End an execution time measurement. */
    public void stop()
    {
        stopTime = System.nanoTime();
        threadUserStopTime = timeBean.getCurrentThreadUserTime();
        threadTotalStopTime = timeBean.getCurrentThreadCpuTime();
    }

    /**
     * Output timing information to a print stream.
     * @param o  The output stream to which the information should be printed
     */
    public void print(PrintStream o) {
        long elapsedTime = (stopTime - startTime)/1000;
        long threadUserTime = (threadUserStopTime - threadUserStartTime)/1000;
        long threadTotalTime = (threadTotalStopTime - threadTotalStartTime)/1000;
        long threadSysTime = threadTotalTime - threadUserTime;
        float tpercent = 100 * (threadTotalTime / (float)elapsedTime);
        float upercent = 100 * (threadUserTime / (float)elapsedTime);
        float spercent = 100 * (threadSysTime / (float)elapsedTime);
        o.println("Elapsed time is " + elapsedTime + " microseconds");
        o.println("Thread total time is " + threadTotalTime + " microseconds (" + tpercent + "%)");
        o.println("Thread user time is " + threadUserTime + " microseconds (" + upercent + "%)");
        o.println("Thread system time is " + threadSysTime + " microseconds (" + spercent + "%)");
    }

}
