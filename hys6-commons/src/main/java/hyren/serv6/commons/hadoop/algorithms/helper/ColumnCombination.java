package hyren.serv6.commons.hadoop.algorithms.helper;

import com.google.common.base.Joiner;
import java.io.Serializable;
import java.util.*;

public class ColumnCombination implements Serializable, Comparable<Object> {

    public static final String COLUMN_CONNECTOR = ",";

    private static final long serialVersionUID = -1675606730574675390L;

    protected Set<ColumnIdentifier> columnIdentifiers;

    public ColumnCombination() {
        columnIdentifiers = new TreeSet<>();
    }

    public ColumnCombination(ColumnIdentifier... columnIdentifier) {
        columnIdentifiers = new TreeSet<>(Arrays.asList(columnIdentifier));
    }

    public Set<ColumnIdentifier> getColumnIdentifiers() {
        return columnIdentifiers;
    }

    public void setColumnIdentifiers(Set<ColumnIdentifier> identifiers) {
        this.columnIdentifiers = identifiers;
    }

    @Override
    public String toString() {
        return columnIdentifiers.toString();
    }

    public String toString(Map<String, String> tableMapping, Map<String, String> columnMapping) throws NullPointerException {
        List<String> cis = new ArrayList<>();
        for (ColumnIdentifier ci : this.columnIdentifiers) {
            cis.add(ci.toString(tableMapping, columnMapping));
        }
        return Joiner.on(COLUMN_CONNECTOR).join(cis);
    }

    public static ColumnCombination fromString(Map<String, String> tableMapping, Map<String, String> columnMapping, String str) throws NullPointerException, IndexOutOfBoundsException {
        String[] parts = str.split(COLUMN_CONNECTOR);
        ColumnIdentifier[] identifiers = new ColumnIdentifier[parts.length];
        for (int i = 0; i < parts.length; i++) {
            identifiers[i] = ColumnIdentifier.fromString(tableMapping, columnMapping, parts[i].trim());
        }
        return new ColumnCombination(identifiers);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((columnIdentifiers == null) ? 0 : columnIdentifiers.hashCode());
        return result;
    }

    @Override
    public int compareTo(Object o) {
        if (o != null && o instanceof ColumnCombination) {
            ColumnCombination other = (ColumnCombination) o;
            int lengthComparison = this.columnIdentifiers.size() - other.columnIdentifiers.size();
            if (lengthComparison != 0) {
                return lengthComparison;
            } else {
                Iterator<ColumnIdentifier> otherIterator = other.columnIdentifiers.iterator();
                int equalCount = 0;
                int negativeCount = 0;
                int positiveCount = 0;
                while (otherIterator.hasNext()) {
                    ColumnIdentifier currentOther = otherIterator.next();
                    for (ColumnIdentifier currentThis : this.columnIdentifiers) {
                        int currentComparison = currentThis.compareTo(currentOther);
                        if (currentComparison == 0) {
                            equalCount++;
                        } else if (currentComparison > 0) {
                            positiveCount++;
                        } else if (currentComparison < 0) {
                            negativeCount++;
                        }
                    }
                }
                if (equalCount == this.columnIdentifiers.size()) {
                    return 0;
                } else if (positiveCount > negativeCount) {
                    return 1;
                } else {
                    return -1;
                }
            }
        } else {
            return 1;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ColumnCombination other = (ColumnCombination) obj;
        if (columnIdentifiers == null) {
            if (other.columnIdentifiers != null) {
                return false;
            }
        } else if (!columnIdentifiers.equals(other.columnIdentifiers)) {
            return false;
        }
        return true;
    }
}
