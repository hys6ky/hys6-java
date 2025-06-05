package hyren.serv6.hadoop.commons.algorithms.helper;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class PLIBuilder {

    private int numRecords = 0;

    private long inputRowLimit;

    public int getNumLastRecords() {
        return this.numRecords;
    }

    public PLIBuilder(long inputRowLimit) {
        this.inputRowLimit = inputRowLimit;
    }

    public List<PositionListIndex> getPLIs(List<List<String>> relationalInput, int numAttributes, boolean isNullEqualNull) {
        List<HashMap<String, IntArrayList>> clusterMaps = this.calculateClusterMaps(relationalInput, numAttributes);
        return this.fetchPositionListIndexes(clusterMaps, isNullEqualNull);
    }

    protected List<HashMap<String, IntArrayList>> calculateClusterMaps(List<List<String>> relationalInput, int numAttributes) {
        List<HashMap<String, IntArrayList>> clusterMaps = new ArrayList<>();
        for (int i = 0; i < numAttributes; i++) clusterMaps.add(new HashMap<String, IntArrayList>());
        this.numRecords = 0;
        for (List<String> record : relationalInput) {
            int attributeId = 0;
            for (String value : record) {
                HashMap<String, IntArrayList> clusterMap = clusterMaps.get(attributeId);
                if (clusterMap.containsKey(value)) {
                    clusterMap.get(value).add(this.numRecords);
                } else {
                    IntArrayList newCluster = new IntArrayList();
                    newCluster.add(this.numRecords);
                    clusterMap.put(value, newCluster);
                }
                attributeId++;
            }
            this.numRecords++;
            if (this.numRecords == Integer.MAX_VALUE - 1)
                throw new RuntimeException("PLI encoding into integer based PLIs is not possible, because the number of records in the dataset exceeds Integer.MAX_VALUE. Use long based plis instead! (NumRecords = " + this.numRecords + " and Integer.MAX_VALUE = " + Integer.MAX_VALUE);
        }
        return clusterMaps;
    }

    protected List<PositionListIndex> fetchPositionListIndexes(List<HashMap<String, IntArrayList>> clusterMaps, boolean isNullEqualNull) {
        List<PositionListIndex> clustersPerAttribute = new ArrayList<>();
        for (int columnId = 0; columnId < clusterMaps.size(); columnId++) {
            List<IntArrayList> clusters = new ArrayList<>();
            HashMap<String, IntArrayList> clusterMap = clusterMaps.get(columnId);
            if (!isNullEqualNull)
                clusterMap.remove(null);
            for (IntArrayList cluster : clusterMap.values()) if (cluster.size() > 1)
                clusters.add(cluster);
            clustersPerAttribute.add(new PositionListIndex(columnId, clusters));
        }
        return clustersPerAttribute;
    }

    public static List<PositionListIndex> getPLIs(ObjectArrayList<List<String>> records, int numAttributes, boolean isNullEqualNull) {
        if (records.size() > Integer.MAX_VALUE)
            throw new RuntimeException("PLI encoding into integer based PLIs is not possible, because the number of records in the dataset exceeds Integer.MAX_VALUE. Use long based plis instead! (NumRecords = " + records.size() + " and Integer.MAX_VALUE = " + Integer.MAX_VALUE);
        List<HashMap<String, IntArrayList>> clusterMaps = calculateClusterMapsStatic(records, numAttributes);
        return fetchPositionListIndexesStatic(clusterMaps, isNullEqualNull);
    }

    protected static List<HashMap<String, IntArrayList>> calculateClusterMapsStatic(ObjectArrayList<List<String>> records, int numAttributes) {
        List<HashMap<String, IntArrayList>> clusterMaps = new ArrayList<>();
        for (int i = 0; i < numAttributes; i++) clusterMaps.add(new HashMap<String, IntArrayList>());
        int recordId = 0;
        for (List<String> record : records) {
            int attributeId = 0;
            for (String value : record) {
                HashMap<String, IntArrayList> clusterMap = clusterMaps.get(attributeId);
                if (clusterMap.containsKey(value)) {
                    clusterMap.get(value).add(recordId);
                } else {
                    IntArrayList newCluster = new IntArrayList();
                    newCluster.add(recordId);
                    clusterMap.put(value, newCluster);
                }
                attributeId++;
            }
            recordId++;
        }
        return clusterMaps;
    }

    protected static List<PositionListIndex> fetchPositionListIndexesStatic(List<HashMap<String, IntArrayList>> clusterMaps, boolean isNullEqualNull) {
        List<PositionListIndex> clustersPerAttribute = new ArrayList<>();
        for (int columnId = 0; columnId < clusterMaps.size(); columnId++) {
            List<IntArrayList> clusters = new ArrayList<>();
            HashMap<String, IntArrayList> clusterMap = clusterMaps.get(columnId);
            if (!isNullEqualNull)
                clusterMap.remove(null);
            for (IntArrayList cluster : clusterMap.values()) if (cluster.size() > 1)
                clusters.add(cluster);
            clustersPerAttribute.add(new PositionListIndex(columnId, clusters));
        }
        return clustersPerAttribute;
    }
}
