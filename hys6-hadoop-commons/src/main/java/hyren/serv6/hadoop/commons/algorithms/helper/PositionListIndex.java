package hyren.serv6.hadoop.commons.algorithms.helper;

import hyren.serv6.commons.hadoop.algorithms.helper.ClusterIdentifier;
import hyren.serv6.commons.hadoop.algorithms.helper.ClusterIdentifierWithRecord;
import hyren.serv6.commons.hadoop.algorithms.helper.IntegerPair;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.lucene.util.OpenBitSet;
import java.io.Serializable;
import java.util.*;

public class PositionListIndex implements Serializable {

    protected final int attribute;

    protected final List<IntArrayList> clusters;

    protected final int numNonUniqueValues;

    public int getAttribute() {
        return this.attribute;
    }

    public List<IntArrayList> getClusters() {
        return this.clusters;
    }

    public int getNumNonUniqueValues() {
        return this.numNonUniqueValues;
    }

    public PositionListIndex(int attribute, List<IntArrayList> clusters) {
        this.attribute = attribute;
        this.clusters = clusters;
        this.numNonUniqueValues = this.countNonUniqueValuesIn(clusters);
    }

    protected int countNonUniqueValuesIn(List<IntArrayList> clusters) {
        int numNonUniqueValues = 0;
        for (IntArrayList cluster : clusters) numNonUniqueValues += cluster.size();
        return numNonUniqueValues;
    }

    public long size() {
        return this.clusters.size();
    }

    public boolean isUnique() {
        return this.size() == 0;
    }

    public boolean isConstant(int numRecords) {
        if (numRecords <= 1)
            return true;
        if ((this.clusters.size() == 1) && (this.clusters.get(0).size() == numRecords))
            return true;
        return false;
    }

    public PositionListIndex intersect(int[]... plis) {
        List<IntArrayList> clusters = new ArrayList<>();
        for (IntArrayList pivotCluster : this.clusters) {
            HashMap<IntArrayList, IntArrayList> clustersMap = new HashMap<IntArrayList, IntArrayList>(pivotCluster.size());
            for (int recordId : pivotCluster) {
                IntArrayList subClusters = new IntArrayList(plis.length);
                boolean isUnique = false;
                for (int i = 0; i < plis.length; i++) {
                    if (plis[i][recordId] == -1) {
                        isUnique = true;
                        break;
                    }
                    subClusters.add(plis[i][recordId]);
                }
                if (isUnique)
                    continue;
                if (!clustersMap.containsKey(subClusters))
                    clustersMap.put(subClusters, new IntArrayList());
                clustersMap.get(subClusters).add(recordId);
            }
            for (IntArrayList cluster : clustersMap.values()) if (cluster.size() > 1)
                clusters.add(cluster);
        }
        return new PositionListIndex(-1, clusters);
    }

    public PositionListIndex intersect(int[] otherPLI) {
        Int2ObjectMap<Int2ObjectMap<IntArrayList>> intersectMap = this.buildIntersectMap(otherPLI);
        List<IntArrayList> clusters = new ArrayList<>();
        for (Int2ObjectMap<IntArrayList> cluster1 : intersectMap.values()) for (IntArrayList cluster2 : cluster1.values()) if (cluster2.size() > 1)
            clusters.add(cluster2);
        return new PositionListIndex(-1, clusters);
    }

    protected Int2ObjectMap<Int2ObjectMap<IntArrayList>> buildIntersectMap(int[] hashedPLI) {
        Int2ObjectMap<Int2ObjectMap<IntArrayList>> intersectMap = new Int2ObjectOpenHashMap<>();
        for (int cluster1Id = 0; cluster1Id < this.clusters.size(); cluster1Id++) {
            IntArrayList cluster = this.clusters.get(cluster1Id);
            for (int recordId : cluster) {
                if (hashedPLI[recordId] >= 0) {
                    int cluster2Id = hashedPLI[recordId];
                    Int2ObjectMap<IntArrayList> cluster1 = intersectMap.get(cluster1Id);
                    if (cluster1 == null) {
                        cluster1 = new Int2ObjectOpenHashMap<IntArrayList>();
                        intersectMap.put(cluster1Id, cluster1);
                    }
                    IntArrayList cluster2 = cluster1.get(cluster2Id);
                    if (cluster2 == null) {
                        cluster2 = new IntArrayList();
                        cluster1.put(cluster2Id, cluster2);
                    }
                    cluster2.add(recordId);
                }
            }
        }
        return intersectMap;
    }

    public boolean refines(int[][] compressedRecords, int rhsAttr) {
        for (IntArrayList cluster : this.clusters) if (!this.probe(compressedRecords, rhsAttr, cluster))
            return false;
        return true;
    }

    public boolean refines(Map<Integer, Integer[]> compressedRecords, int rhsAttr) {
        for (IntArrayList cluster : this.clusters) if (!this.probe(compressedRecords, rhsAttr, cluster))
            return false;
        return true;
    }

    protected boolean probe(int[][] compressedRecords, int rhsAttr, IntArrayList cluster) {
        int rhsClusterId = compressedRecords[cluster.getInt(0)][rhsAttr];
        if (rhsClusterId == -1)
            return false;
        for (int recordId : cluster) if (compressedRecords[recordId][rhsAttr] != rhsClusterId)
            return false;
        return true;
    }

    protected boolean probe(Map<Integer, Integer[]> compressedRecords, int rhsAttr, IntArrayList cluster) {
        int rhsClusterId = compressedRecords.get(cluster.getInt(0))[rhsAttr];
        if (rhsClusterId == -1)
            return false;
        for (int recordId : cluster) if (compressedRecords.get(recordId)[rhsAttr] != rhsClusterId)
            return false;
        return true;
    }

    public boolean refines(int[] rhsInvertedPli) {
        for (IntArrayList cluster : this.clusters) if (!this.probe(rhsInvertedPli, cluster))
            return false;
        return true;
    }

    protected boolean probe(int[] rhsInvertedPli, IntArrayList cluster) {
        int rhsClusterId = rhsInvertedPli[cluster.getInt(0)];
        if (rhsClusterId == -1)
            return false;
        for (int recordId : cluster) if (rhsInvertedPli[recordId] != rhsClusterId)
            return false;
        return true;
    }

    public OpenBitSet refines(int[][] compressedRecords, OpenBitSet lhs, OpenBitSet rhs, List<IntegerPair> comparisonSuggestions) {
        int rhsSize = (int) rhs.cardinality();
        int lhsSize = (int) lhs.cardinality();
        OpenBitSet refinedRhs = rhs.clone();
        int[] rhsAttrId2Index = new int[compressedRecords[0].length];
        int[] rhsAttrIndex2Id = new int[rhsSize];
        int index = 0;
        for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
            rhsAttrId2Index[rhsAttr] = index;
            rhsAttrIndex2Id[index] = rhsAttr;
            index++;
        }
        for (IntArrayList cluster : this.clusters) {
            Object2ObjectOpenHashMap<ClusterIdentifier, ClusterIdentifierWithRecord> subClusters = new Object2ObjectOpenHashMap<>(cluster.size());
            for (int recordId : cluster) {
                ClusterIdentifier subClusterIdentifier = this.buildClusterIdentifier(lhs, lhsSize, compressedRecords[recordId]);
                if (subClusterIdentifier == null)
                    continue;
                if (subClusters.containsKey(subClusterIdentifier)) {
                    ClusterIdentifierWithRecord rhsClusters = subClusters.get(subClusterIdentifier);
                    for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
                        int rhsCluster = compressedRecords[recordId][rhsAttr];
                        if ((rhsCluster == -1) || (rhsCluster != rhsClusters.get(rhsAttrId2Index[rhsAttr]))) {
                            comparisonSuggestions.add(new IntegerPair(recordId, rhsClusters.getRecord()));
                            refinedRhs.clear(rhsAttr);
                            if (refinedRhs.isEmpty())
                                return refinedRhs;
                        }
                    }
                } else {
                    int[] rhsClusters = new int[rhsSize];
                    for (int rhsAttr = 0; rhsAttr < rhsSize; rhsAttr++) rhsClusters[rhsAttr] = compressedRecords[recordId][rhsAttrIndex2Id[rhsAttr]];
                    subClusters.put(subClusterIdentifier, new ClusterIdentifierWithRecord(rhsClusters, recordId));
                }
            }
        }
        return refinedRhs;
    }

    public OpenBitSet refines(Map<Integer, Integer[]> compressedRecords, OpenBitSet lhs, OpenBitSet rhs, List<IntegerPair> comparisonSuggestions) {
        int rhsSize = (int) rhs.cardinality();
        int lhsSize = (int) lhs.cardinality();
        OpenBitSet refinedRhs = rhs.clone();
        int[] rhsAttrId2Index = new int[compressedRecords.get(0).length];
        int[] rhsAttrIndex2Id = new int[rhsSize];
        int index = 0;
        for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
            rhsAttrId2Index[rhsAttr] = index;
            rhsAttrIndex2Id[index] = rhsAttr;
            index++;
        }
        for (IntArrayList cluster : this.clusters) {
            Object2ObjectOpenHashMap<ClusterIdentifier, ClusterIdentifierWithRecord> subClusters = new Object2ObjectOpenHashMap<>(cluster.size());
            for (int recordId : cluster) {
                ClusterIdentifier subClusterIdentifier = this.buildClusterIdentifier(lhs, lhsSize, compressedRecords.get(recordId));
                if (subClusterIdentifier == null)
                    continue;
                if (subClusters.containsKey(subClusterIdentifier)) {
                    ClusterIdentifierWithRecord rhsClusters = subClusters.get(subClusterIdentifier);
                    for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
                        int rhsCluster = compressedRecords.get(recordId)[rhsAttr];
                        if ((rhsCluster == -1) || (rhsCluster != rhsClusters.get(rhsAttrId2Index[rhsAttr]))) {
                            comparisonSuggestions.add(new IntegerPair(recordId, rhsClusters.getRecord()));
                            refinedRhs.clear(rhsAttr);
                            if (refinedRhs.isEmpty())
                                return refinedRhs;
                        }
                    }
                } else {
                    int[] rhsClusters = new int[rhsSize];
                    for (int rhsAttr = 0; rhsAttr < rhsSize; rhsAttr++) rhsClusters[rhsAttr] = compressedRecords.get(recordId)[rhsAttrIndex2Id[rhsAttr]];
                    subClusters.put(subClusterIdentifier, new ClusterIdentifierWithRecord(rhsClusters, recordId));
                }
            }
        }
        return refinedRhs;
    }

    public OpenBitSet refines(int[][] invertedPlis, OpenBitSet lhs, OpenBitSet rhs, int numAttributes, ArrayList<IntegerPair> comparisonSuggestions) {
        int rhsSize = (int) rhs.cardinality();
        int lhsSize = (int) lhs.cardinality();
        OpenBitSet refinedRhs = rhs.clone();
        int[] rhsAttrId2Index = new int[numAttributes];
        int[] rhsAttrIndex2Id = new int[rhsSize];
        int index = 0;
        for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
            rhsAttrId2Index[rhsAttr] = index;
            rhsAttrIndex2Id[index] = rhsAttr;
            index++;
        }
        for (IntArrayList cluster : this.clusters) {
            Object2ObjectOpenHashMap<ClusterIdentifier, ClusterIdentifierWithRecord> subClusters = new Object2ObjectOpenHashMap<>(cluster.size());
            for (int recordId : cluster) {
                ClusterIdentifier subClusterIdentifier = this.buildClusterIdentifier(recordId, invertedPlis, lhs, lhsSize);
                if (subClusterIdentifier == null)
                    continue;
                if (subClusters.containsKey(subClusterIdentifier)) {
                    ClusterIdentifierWithRecord rhsClusters = subClusters.get(subClusterIdentifier);
                    for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
                        int rhsCluster = invertedPlis[rhsAttr][recordId];
                        if ((rhsCluster == -1) || (rhsCluster != rhsClusters.get(rhsAttrId2Index[rhsAttr]))) {
                            comparisonSuggestions.add(new IntegerPair(recordId, rhsClusters.getRecord()));
                            refinedRhs.clear(rhsAttr);
                            if (refinedRhs.isEmpty())
                                return refinedRhs;
                        }
                    }
                } else {
                    int[] rhsClusters = new int[rhsSize];
                    for (int rhsAttr = 0; rhsAttr < rhsSize; rhsAttr++) rhsClusters[rhsAttr] = invertedPlis[rhsAttrIndex2Id[rhsAttr]][recordId];
                    subClusters.put(subClusterIdentifier, new ClusterIdentifierWithRecord(rhsClusters, recordId));
                }
            }
        }
        return refinedRhs;
    }

    public boolean refines(int[][] compressedRecords, OpenBitSet lhs, int[] rhs) {
        for (IntArrayList cluster : this.clusters) {
            ClusterTree clusterTree = new ClusterTree();
            for (int recordId : cluster) if (!clusterTree.add(compressedRecords, lhs, recordId, rhs[recordId]))
                return false;
        }
        return true;
    }

    public boolean refines(int[][] lhsInvertedPlis, int[] rhs) {
        for (IntArrayList cluster : this.clusters) {
            Object2IntOpenHashMap<IntArrayList> clustersMap = new Object2IntOpenHashMap<>(cluster.size());
            for (int recordId : cluster) {
                IntArrayList additionalLhsCluster = this.buildClusterIdentifier(recordId, lhsInvertedPlis);
                if (additionalLhsCluster == null)
                    continue;
                if (clustersMap.containsKey(additionalLhsCluster)) {
                    if ((rhs[recordId] == -1) || (clustersMap.getInt(additionalLhsCluster) != rhs[recordId]))
                        return false;
                } else {
                    clustersMap.put(additionalLhsCluster, rhs[recordId]);
                }
            }
        }
        return true;
    }

    protected ClusterIdentifier buildClusterIdentifier(OpenBitSet lhs, int lhsSize, int[] record) {
        int[] cluster = new int[lhsSize];
        int index = 0;
        for (int lhsAttr = lhs.nextSetBit(0); lhsAttr >= 0; lhsAttr = lhs.nextSetBit(lhsAttr + 1)) {
            int clusterId = record[lhsAttr];
            if (clusterId < 0)
                return null;
            cluster[index] = clusterId;
            index++;
        }
        return new ClusterIdentifier(cluster);
    }

    protected ClusterIdentifier buildClusterIdentifier(OpenBitSet lhs, int lhsSize, Integer[] record) {
        int[] cluster = new int[lhsSize];
        int index = 0;
        for (int lhsAttr = lhs.nextSetBit(0); lhsAttr >= 0; lhsAttr = lhs.nextSetBit(lhsAttr + 1)) {
            int clusterId = record[lhsAttr];
            if (clusterId < 0)
                return null;
            cluster[index] = clusterId;
            index++;
        }
        return new ClusterIdentifier(cluster);
    }

    protected ClusterIdentifier buildClusterIdentifier(int recordId, int[][] invertedPlis, OpenBitSet lhs, int lhsSize) {
        int[] cluster = new int[lhsSize];
        int index = 0;
        for (int lhsAttr = lhs.nextSetBit(0); lhsAttr >= 0; lhsAttr = lhs.nextSetBit(lhsAttr + 1)) {
            int clusterId = invertedPlis[lhsAttr][recordId];
            if (clusterId < 0)
                return null;
            cluster[index] = clusterId;
            index++;
        }
        return new ClusterIdentifier(cluster);
    }

    protected IntArrayList buildClusterIdentifier(int recordId, int[][] lhsInvertedPlis) {
        IntArrayList clusterIdentifier = new IntArrayList(lhsInvertedPlis.length);
        for (int attributeIndex = 0; attributeIndex < lhsInvertedPlis.length; attributeIndex++) {
            int clusterId = lhsInvertedPlis[attributeIndex][recordId];
            if (clusterId < 0)
                return null;
            clusterIdentifier.add(clusterId);
        }
        return clusterIdentifier;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        List<IntOpenHashSet> setCluster = this.convertClustersToSets(this.clusters);
        Collections.sort(setCluster, new Comparator<IntSet>() {

            @Override
            public int compare(IntSet o1, IntSet o2) {
                return o1.hashCode() - o2.hashCode();
            }
        });
        result = prime * result + (setCluster.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        PositionListIndex other = (PositionListIndex) obj;
        if (this.clusters == null) {
            if (other.clusters != null) {
                return false;
            }
        } else {
            List<IntOpenHashSet> setCluster = this.convertClustersToSets(this.clusters);
            List<IntOpenHashSet> otherSetCluster = this.convertClustersToSets(other.clusters);
            for (IntOpenHashSet cluster : setCluster) {
                if (!otherSetCluster.contains(cluster)) {
                    return false;
                }
            }
            for (IntOpenHashSet cluster : otherSetCluster) {
                if (!setCluster.contains(cluster)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("{ ");
        for (IntArrayList cluster : this.clusters) {
            builder.append("{");
            builder.append(CollectionUtils.concat(cluster, ","));
            builder.append("} ");
        }
        builder.append("}");
        return builder.toString();
    }

    protected List<IntOpenHashSet> convertClustersToSets(List<IntArrayList> listCluster) {
        List<IntOpenHashSet> setClusters = new LinkedList<>();
        for (IntArrayList cluster : listCluster) {
            setClusters.add(new IntOpenHashSet(cluster));
        }
        return setClusters;
    }
}
