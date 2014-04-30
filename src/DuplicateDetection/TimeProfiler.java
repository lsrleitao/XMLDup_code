package DuplicateDetection;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

public class TimeProfiler {

    private long starttimeCPU, stoptimeCPU, timeusedCPU;
    private long starttimeUSER, stoptimeUSER, timeusedUSER;
    private long starttimeSYSTEM, stoptimeSYSTEM, timeusedSYSTEM;
    private long wallClockTime;

    public void start() {

        wallClockTime = System.currentTimeMillis();

        starttimeCPU = getCpuTime();

        starttimeSYSTEM = getSystemTime();

        starttimeUSER = getUserTime();
    }

    public void stop() {

        wallClockTime = System.currentTimeMillis() - wallClockTime;

        stoptimeCPU = getCpuTime();
        timeusedCPU = stoptimeCPU - starttimeCPU;

        stoptimeSYSTEM = getSystemTime();
        timeusedSYSTEM = stoptimeSYSTEM - starttimeSYSTEM;

        stoptimeUSER = getUserTime();
        timeusedUSER = stoptimeUSER - starttimeUSER;

    }

    public void writeTimeToFile(String resultsPath) throws IOException {

        FileHandler fh = new FileHandler();

        fh.writeTimeToFile("Wall Clock time", getWallClockTime(), resultsPath);
        fh.writeTimeToFile("CPU time", timeusedCPU, resultsPath);
        fh.writeTimeToFile("System time", timeusedSYSTEM, resultsPath);
        fh.writeTimeToFile("User time", timeusedUSER, resultsPath);
    }

    public long getWallClockTime() {
        return wallClockTime;
    }

    public long getCpuTime() {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        long[] ids = bean.getAllThreadIds();
        if (!bean.isThreadCpuTimeSupported())
            return 0L;
        long time = 0L;
        for (int i = 0; ids.length > i; i++) {
            long t = bean.getThreadCpuTime(ids[i]);
            if (t != -1)
                time += t;
        }
        return time / 1000000;
    }

    public long getUserTime() {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        long[] ids = bean.getAllThreadIds();
        if (!bean.isThreadCpuTimeSupported())
            return 0L;
        long time = 0L;
        for (int i = 0; ids.length > i; i++) {
            long t = bean.getThreadUserTime(ids[i]);
            if (t != -1)
                time += t;
        }
        return time / 1000000;
    }

    public long getSystemTime() {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        long[] ids = bean.getAllThreadIds();
        if (!bean.isThreadCpuTimeSupported())
            return 0L;
        long time = 0L;
        for (int i = 0; ids.length > i; i++) {
            long tc = bean.getThreadCpuTime(ids[i]);
            long tu = bean.getThreadUserTime(ids[i]);
            if (tc != -1 && tu != -1)
                time += (tc - tu);
        }
        return time / 1000000;
    }

}
