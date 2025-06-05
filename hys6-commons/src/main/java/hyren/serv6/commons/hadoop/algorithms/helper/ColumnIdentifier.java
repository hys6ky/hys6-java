package hyren.serv6.commons.hadoop.algorithms.helper;

import java.io.Serializable;
import java.util.Map;

public class ColumnIdentifier implements Comparable<ColumnIdentifier>, Serializable {

    private static final long serialVersionUID = -3199299021265706919L;

    public static final String TABLE_COLUMN_CONCATENATOR = ".";

    public static final String TABLE_COLUMN_CONCATENATOR_ESC = "\\.";

    protected String tableIdentifier;

    protected String columnIdentifier;

    public ColumnIdentifier() {
        this.tableIdentifier = "";
        this.columnIdentifier = "";
    }

    public ColumnIdentifier(String tableIdentifier, String columnIdentifier) {
        this.tableIdentifier = tableIdentifier;
        this.columnIdentifier = columnIdentifier;
    }

    public String getTableIdentifier() {
        return tableIdentifier;
    }

    public void setTableIdentifier(String tableIdentifier) {
        this.tableIdentifier = tableIdentifier;
    }

    public String getColumnIdentifier() {
        return columnIdentifier;
    }

    public void setColumnIdentifier(String columnIdentifier) {
        this.columnIdentifier = columnIdentifier;
    }

    @Override
    public String toString() {
        if (this.tableIdentifier.isEmpty() && this.columnIdentifier.isEmpty())
            return "";
        return tableIdentifier + TABLE_COLUMN_CONCATENATOR + columnIdentifier;
    }

    public String toString(Map<String, String> tableMapping, Map<String, String> columnMapping) {
        String tableValue = tableMapping.get(this.tableIdentifier);
        String columnStr = tableValue + TABLE_COLUMN_CONCATENATOR + this.columnIdentifier;
        return columnMapping.get(columnStr);
    }

    public static ColumnIdentifier fromString(Map<String, String> tableMapping, Map<String, String> columnMapping, String str) throws NullPointerException, IndexOutOfBoundsException {
        if (str.isEmpty()) {
            return new ColumnIdentifier();
        }
        String[] parts = columnMapping.get(str).split(TABLE_COLUMN_CONCATENATOR_ESC, 2);
        String tableKey = parts[0];
        String columnName = parts[1];
        String tableName = tableMapping.get(tableKey);
        return new ColumnIdentifier(tableName, columnName);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((columnIdentifier == null) ? 0 : columnIdentifier.hashCode());
        result = prime * result + ((tableIdentifier == null) ? 0 : tableIdentifier.hashCode());
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
        ColumnIdentifier other = (ColumnIdentifier) obj;
        if (columnIdentifier == null) {
            if (other.columnIdentifier != null) {
                return false;
            }
        } else if (!columnIdentifier.equals(other.columnIdentifier)) {
            return false;
        }
        if (tableIdentifier == null) {
            if (other.tableIdentifier != null) {
                return false;
            }
        } else if (!tableIdentifier.equals(other.tableIdentifier)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(ColumnIdentifier other) {
        int tableIdentifierComparison;
        if (this.tableIdentifier == null) {
            if (other.tableIdentifier == null)
                tableIdentifierComparison = 0;
            else
                tableIdentifierComparison = 1;
        } else if (other.tableIdentifier == null)
            tableIdentifierComparison = -1;
        else
            tableIdentifierComparison = this.tableIdentifier.compareTo(other.tableIdentifier);
        if (0 != tableIdentifierComparison) {
            return tableIdentifierComparison;
        } else {
            return columnIdentifier.compareTo(other.columnIdentifier);
        }
    }
}
