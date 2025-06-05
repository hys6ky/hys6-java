package hyren.serv6.hadoop.commons.algorithms.helper;

import hyren.serv6.commons.hadoop.algorithms.helper.IntegerPair;
import org.apache.lucene.util.OpenBitSet;
import java.util.ArrayList;
import java.util.List;

public class ValidationResult {

    public int validations = 0;

    public int intersections = 0;

    public List<FD> invalidFDs = new ArrayList<>();

    public List<OpenBitSet> invalidUCCs = new ArrayList<>();

    public List<IntegerPair> comparisonSuggestions = new ArrayList<>();

    public void add(ValidationResult other) {
        this.validations += other.validations;
        this.intersections += other.intersections;
        this.invalidFDs.addAll(other.invalidFDs);
        this.invalidUCCs.addAll(other.invalidUCCs);
        this.comparisonSuggestions.addAll(other.comparisonSuggestions);
    }
}
