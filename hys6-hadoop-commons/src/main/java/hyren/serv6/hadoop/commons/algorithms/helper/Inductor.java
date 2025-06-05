package hyren.serv6.hadoop.commons.algorithms.helper;

import org.apache.lucene.util.OpenBitSet;
import java.util.List;

public class Inductor {

    private FDSet negCover;

    private FDTree posCover;

    private MemoryGuardian memoryGuardian;

    public Inductor(FDSet negCover, FDTree posCover, MemoryGuardian memoryGuardian) {
        this.negCover = negCover;
        this.posCover = posCover;
        this.memoryGuardian = memoryGuardian;
    }

    public void updatePositiveCover(FDList nonFds) {
        for (int i = nonFds.getFdLevels().size() - 1; i >= 0; i--) {
            if (i >= nonFds.getFdLevels().size())
                continue;
            List<OpenBitSet> nonFdLevel = nonFds.getFdLevels().get(i);
            for (OpenBitSet lhs : nonFdLevel) {
                OpenBitSet fullRhs = lhs.clone();
                fullRhs.flip(0, this.posCover.getNumAttributes());
                for (int rhs = fullRhs.nextSetBit(0); rhs >= 0; rhs = fullRhs.nextSetBit(rhs + 1)) this.specializePositiveCover(lhs, rhs, nonFds);
            }
            nonFdLevel.clear();
        }
    }

    protected int specializePositiveCover(OpenBitSet lhs, int rhs, FDList nonFds) {
        int numAttributes = this.posCover.getChildren().length;
        int newFDs = 0;
        List<OpenBitSet> specLhss;
        if (!(specLhss = this.posCover.getFdAndGeneralizations(lhs, rhs)).isEmpty()) {
            for (OpenBitSet specLhs : specLhss) {
                this.posCover.removeFunctionalDependency(specLhs, rhs);
                if ((this.posCover.getMaxDepth() > 0) && (specLhs.cardinality() >= this.posCover.getMaxDepth()))
                    continue;
                for (int attr = numAttributes - 1; attr >= 0; attr--) {
                    if (!lhs.get(attr) && (attr != rhs)) {
                        specLhs.set(attr);
                        if (!this.posCover.containsFdOrGeneralization(specLhs, rhs)) {
                            this.posCover.addFunctionalDependency(specLhs, rhs);
                            newFDs++;
                            this.memoryGuardian.memoryChanged(1);
                            this.memoryGuardian.match(this.negCover, this.posCover, nonFds);
                        }
                        specLhs.clear(attr);
                    }
                }
            }
        }
        return newFDs;
    }
}
