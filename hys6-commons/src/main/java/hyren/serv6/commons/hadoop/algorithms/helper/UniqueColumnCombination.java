package hyren.serv6.commons.hadoop.algorithms.helper;

import java.io.Serializable;
import java.util.Map;

public class UniqueColumnCombination implements Serializable {

    private static final long serialVersionUID = -8782723135088616653L;

    protected ColumnCombination columnCombination;

    protected UniqueColumnCombination() {
        this.columnCombination = new ColumnCombination();
    }

    public UniqueColumnCombination(ColumnIdentifier... columnIdentifier) {
        this.columnCombination = new ColumnCombination(columnIdentifier);
    }

    public UniqueColumnCombination(ColumnCombination columnCombination) {
        this.columnCombination = columnCombination;
    }

    public ColumnCombination getColumnCombination() {
        return columnCombination;
    }

    public void setColumnCombination(ColumnCombination columnCombination) {
        this.columnCombination = columnCombination;
    }

    @Override
    public String toString() {
        return columnCombination.toString();
    }

    public String toString(Map<String, String> tableMapping, Map<String, String> columnMapping) {
        return this.columnCombination.toString(tableMapping, columnMapping);
    }

    public static UniqueColumnCombination fromString(Map<String, String> tableMapping, Map<String, String> columnMapping, String str) throws NullPointerException, IndexOutOfBoundsException {
        return new UniqueColumnCombination(ColumnCombination.fromString(tableMapping, columnMapping, str));
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((columnCombination == null) ? 0 : columnCombination.hashCode());
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
        if (getClass() != obj.getClass()) {
            return false;
        }
        UniqueColumnCombination other = (UniqueColumnCombination) obj;
        if (columnCombination == null) {
            return other.columnCombination == null;
        } else
            return columnCombination.equals(other.columnCombination);
    }
}
