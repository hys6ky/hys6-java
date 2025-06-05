package hyren.serv6.m.util;

import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Set;

public class IdGenerator {

    private final static long beginTs = 100000000L;

    private static long lastTs = 0L;

    private static long processId = 1023;

    private final static int processIdBits = 10;

    private static long sequence = 0L;

    private static int sequenceBits = 12;

    public IdGenerator() {
        if (processId > ((1 << processIdBits) - 1)) {
            throw new RuntimeException("进程ID超出范围，设置位数" + processIdBits + "，最大" + ((1 << processIdBits) - 1));
        }
        this.processId = processId;
    }

    protected static long timeGen() {
        return System.currentTimeMillis();
    }

    public static synchronized long nextId() {
        long ts = timeGen();
        if (ts < lastTs) {
            throw new RuntimeException("时间戳顺序错误");
        }
        if (ts == lastTs) {
            sequence = (sequence + 1) & ((1 << sequenceBits) - 1);
            if (sequence == 0) {
                ts = nextTs(lastTs);
            }
        } else {
            sequence = 0L;
        }
        lastTs = ts;
        return ((ts - beginTs) << (processIdBits + sequenceBits)) | (processId << sequenceBits) | sequence;
    }

    protected static long nextTs(long lastTs) {
        long ts = timeGen();
        while (ts <= lastTs) {
            ts = timeGen();
        }
        return ts;
    }

    public static void main(String[] args) throws Exception {
        IdGenerator ig = new IdGenerator();
        String str = "20170101";
        System.out.println(new SimpleDateFormat("YYYYMMDD").parse(str).getTime());
        Set<Long> set = new HashSet<Long>();
        long begin = System.nanoTime();
        for (int i = 0; i < 10; i++) {
            set.add(ig.nextId());
        }
        System.out.println("time=" + (System.nanoTime() - begin) / 1000.0 + " us");
        System.out.println(set.size());
        System.out.println(set);
    }
}
