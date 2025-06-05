package hyren.serv6.commons.hadoop.algorithms.helper;

public class ClusterIdentifierWithRecord extends ClusterIdentifier {

    private final int record;

    public ClusterIdentifierWithRecord(final int[] cluster, final int record) {
        super(cluster);
        this.record = record;
    }

    public int getRecord() {
        return this.record;
    }
}
