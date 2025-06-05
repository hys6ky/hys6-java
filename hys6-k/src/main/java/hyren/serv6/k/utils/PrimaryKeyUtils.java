package hyren.serv6.k.utils;

public class PrimaryKeyUtils {

    private final static long START_TIMESTAMP = 1480166465631L;

    private final static long SEQUENCE_BIT = 12;

    private final static long MACHINE_BIT = 5;

    private final static long DATACENTER_BIT = 5;

    private final static long MAX_SEQUENCE = ~(-1L << SEQUENCE_BIT);

    private final static long MAX_MACHINE_NUM = ~(-1L << MACHINE_BIT);

    private final static long MAX_DATACENTER_NUM = ~(-1L << DATACENTER_BIT);

    private final static long MACHINE_LEFT = SEQUENCE_BIT;

    private final static long DATACENTER_LEFT = SEQUENCE_BIT + MACHINE_BIT;

    private final static long TIMESTAMP_LEFT = DATACENTER_LEFT + DATACENTER_BIT;

    private static long datacenterId = 1;

    private static long machineId = 1;

    private static long sequence = 0L;

    private static long lastTimestamp = -1L;

    public PrimaryKeyUtils() {
    }

    public synchronized static long nextId() {
        long currTimestamp = getTimestamp();
        if (currTimestamp < lastTimestamp) {
            throw new RuntimeException("Clock moved backwards. Refusing to generate id");
        }
        if (currTimestamp == lastTimestamp) {
            sequence = (sequence + 1) & MAX_SEQUENCE;
            if (sequence == 0L) {
                currTimestamp = getNextTimestamp();
            }
        } else {
            sequence = 0L;
        }
        lastTimestamp = currTimestamp;
        return (currTimestamp - START_TIMESTAMP) << TIMESTAMP_LEFT | datacenterId << DATACENTER_LEFT | machineId << MACHINE_LEFT | sequence;
    }

    private static long getNextTimestamp() {
        long timestamp = getTimestamp();
        while (timestamp <= lastTimestamp) {
            timestamp = getTimestamp();
        }
        return timestamp;
    }

    private static long getTimestamp() {
        return System.currentTimeMillis();
    }
}
