package hyren.serv6.commons.key;

import fd.ng.core.utils.JsonUtil;
import hyren.serv6.commons.hadoop.util.ClassBase;
import lombok.extern.slf4j.Slf4j;
import java.util.Iterator;
import java.util.Random;
import java.util.TreeSet;

@Slf4j
public class HashChoreWoker {

    private int baseRecord;

    private IRowKeyGenerator rkGen;

    private String[] split;

    private int splitKeysBase;

    private int splitKeysNumber;

    private byte[][] splitKeys;

    public HashChoreWoker(int baseRecord, int prepareRegions) {
        this.baseRecord = baseRecord;
        rkGen = new HashRowKeyGeneratorImpl();
        splitKeysNumber = prepareRegions - 1;
        splitKeysBase = baseRecord / prepareRegions;
    }

    public HashChoreWoker(String coustomSplitKeys) {
        split = coustomSplitKeys.split(",");
        this.baseRecord = split.length;
        rkGen = new HashRowKeyGeneratorImpl();
    }

    public byte[][] coustomSplitKeys() {
        splitKeys = new byte[baseRecord][];
        TreeSet<byte[]> rows = ClassBase.hadoopInstance().coustomByteAddData(baseRecord, split);
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int index = 0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            splitKeys[index] = tempRow;
            index++;
        }
        rows.clear();
        return splitKeys;
    }

    public byte[][] calcSplitKeys() {
        splitKeys = new byte[splitKeysNumber][];
        TreeSet<byte[]> rows = ClassBase.hadoopInstance().calcByteAddData(baseRecord, rkGen);
        int pointer = 0;
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int index = 0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            if ((pointer != 0) && (splitKeysBase % pointer == 0)) {
                if (index < splitKeysNumber) {
                    splitKeys[index] = tempRow;
                    index++;
                }
            }
            pointer++;
        }
        rows.clear();
        return splitKeys;
    }

    public interface IRowKeyGenerator {

        byte[] nextId();
    }

    public static class HashRowKeyGeneratorImpl implements IRowKeyGenerator {

        private long currentId = 1;

        private long currentTime = System.currentTimeMillis();

        private Random random = new Random();

        public byte[] nextId() {
            return ClassBase.hadoopInstance().nextId(currentId, currentTime, random);
        }
    }

    public static void main(String[] args) throws ClassNotFoundException {
        String key = "dsd,rere,afa,sgh,try,lkj,hjk";
        HashChoreWoker worker = new HashChoreWoker(1, 10);
        HashChoreWoker worker2 = new HashChoreWoker(key);
        byte[][] splitKeys = worker.calcSplitKeys();
        byte[][] splitKey2 = worker2.coustomSplitKeys();
        for (byte[] dd : splitKeys) {
            System.out.println(new String(dd));
        }
        for (byte[] dd : splitKey2) {
            System.out.println("+++++++++" + new String(dd));
        }
    }
}
