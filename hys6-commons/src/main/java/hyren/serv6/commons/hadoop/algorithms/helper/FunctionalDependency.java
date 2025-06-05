package hyren.serv6.commons.hadoop.algorithms.helper;

import java.io.Serializable;
import java.util.Map;

public class FunctionalDependency implements Serializable {

    public static final String FD_SEPARATOR = "->";

    private static final long serialVersionUID = 7625471410289776666L;

    protected ColumnCombination determinant;

    protected ColumnIdentifier dependant;

    public FunctionalDependency() {
        this.dependant = new ColumnIdentifier();
        this.determinant = new ColumnCombination();
    }

    public FunctionalDependency(ColumnCombination determinant, ColumnIdentifier dependant) {
        this.determinant = determinant;
        this.dependant = dependant;
    }

    public ColumnCombination getDeterminant() {
        return determinant;
    }

    public void setDependant(ColumnIdentifier dependant) {
        this.dependant = dependant;
    }

    public ColumnIdentifier getDependant() {
        return dependant;
    }

    public void setDeterminant(ColumnCombination determinant) {
        this.determinant = determinant;
    }

    @Override
    public String toString() {
        return determinant.toString() + FD_SEPARATOR + dependant.toString();
    }

    public String toString(Map<String, String> tableMapping, Map<String, String> columnMapping) {
        return determinant.toString(tableMapping, columnMapping) + FD_SEPARATOR + dependant.toString(tableMapping, columnMapping);
    }

    public static FunctionalDependency fromString(Map<String, String> tableMapping, Map<String, String> columnMapping, String str) throws NullPointerException, IndexOutOfBoundsException {
        String[] parts = str.split(FD_SEPARATOR);
        ColumnCombination determinant = ColumnCombination.fromString(tableMapping, columnMapping, parts[0]);
        ColumnIdentifier dependant = ColumnIdentifier.fromString(tableMapping, columnMapping, parts[1]);
        return new FunctionalDependency(determinant, dependant);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dependant == null) ? 0 : dependant.hashCode());
        result = prime * result + ((determinant == null) ? 0 : determinant.hashCode());
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
        FunctionalDependency other = (FunctionalDependency) obj;
        if (dependant == null) {
            if (other.dependant != null) {
                return false;
            }
        } else if (!dependant.equals(other.dependant)) {
            return false;
        }
        if (determinant == null) {
            return other.determinant == null;
        } else
            return determinant.equals(other.determinant);
    }
}
