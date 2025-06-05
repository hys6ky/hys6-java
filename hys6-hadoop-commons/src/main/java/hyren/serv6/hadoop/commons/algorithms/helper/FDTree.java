package hyren.serv6.hadoop.commons.algorithms.helper;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.lucene.util.OpenBitSet;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FDTree extends FDTreeElement implements Serializable {

    private static final long serialVersionUID = 1L;

    private int depth = 0;

    private int maxDepth;

    public FDTree(int numAttributes, int maxDepth) {
        super(numAttributes);
        this.maxDepth = maxDepth;
        this.children = new FDTreeElement[numAttributes];
    }

    public int getDepth() {
        return this.depth;
    }

    public int getMaxDepth() {
        return this.maxDepth;
    }

    @Override
    public String toString() {
        return "[" + this.depth + " depth, " + this.maxDepth + " maxDepth]";
    }

    public void trim(int newDepth) {
        this.trimRecursive(0, newDepth);
        this.depth = newDepth;
        this.maxDepth = newDepth;
    }

    public void addMostGeneralDependencies() {
        this.rhsAttributes.set(0, this.numAttributes);
        this.rhsFds.set(0, this.numAttributes);
    }

    public FDTreeElement addFunctionalDependency(OpenBitSet lhs, int rhs) {
        FDTreeElement currentNode = this;
        if (lhs.cardinality() > maxDepth)
            return currentNode;
        currentNode.addRhsAttribute(rhs);
        int lhsLength = 0;
        for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
            lhsLength++;
            if (currentNode.getChildren() == null) {
                currentNode.setChildren(new FDTreeElement[this.numAttributes]);
                currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
            } else if (currentNode.getChildren()[i] == null) {
                currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
            }
            currentNode = currentNode.getChildren()[i];
            currentNode.addRhsAttribute(rhs);
        }
        currentNode.markFd(rhs);
        this.depth = Math.max(this.depth, lhsLength);
        return currentNode;
    }

    public FDTreeElement addFunctionalDependency(OpenBitSet lhs, OpenBitSet rhs) {
        FDTreeElement currentNode = this;
        currentNode.addRhsAttributes(rhs);
        int lhsLength = 0;
        for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
            lhsLength++;
            if (currentNode.getChildren() == null) {
                currentNode.setChildren(new FDTreeElement[this.numAttributes]);
                currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
            } else if (currentNode.getChildren()[i] == null) {
                currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
            }
            currentNode = currentNode.getChildren()[i];
            currentNode.addRhsAttributes(rhs);
        }
        currentNode.markFds(rhs);
        this.depth = Math.max(this.depth, lhsLength);
        return currentNode;
    }

    public FDTreeElement addFunctionalDependencyGetIfNew(OpenBitSet lhs, int rhs) {
        FDTreeElement currentNode = this;
        if (lhs.cardinality() > maxDepth)
            return null;
        currentNode.addRhsAttribute(rhs);
        boolean isNew = false;
        int lhsLength = 0;
        for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
            lhsLength++;
            if (currentNode.getChildren() == null) {
                currentNode.setChildren(new FDTreeElement[this.numAttributes]);
                currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
                isNew = true;
            } else if (currentNode.getChildren()[i] == null) {
                currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
                isNew = true;
            }
            currentNode = currentNode.getChildren()[i];
            currentNode.addRhsAttribute(rhs);
        }
        currentNode.markFd(rhs);
        this.depth = Math.max(this.depth, lhsLength);
        if (isNew)
            return currentNode;
        return null;
    }

    public FDTreeElement addFunctionalDependencyIfNotInvalid(OpenBitSet lhs, OpenBitSet rhs) {
        FDTreeElement currentNode = this;
        if (lhs.cardinality() > maxDepth)
            return currentNode;
        currentNode.addRhsAttributes(rhs);
        OpenBitSet invalidFds = currentNode.rhsAttributes.clone();
        int lhsLength = 0;
        for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
            lhsLength++;
            if (currentNode.getChildren() == null) {
                currentNode.setChildren(new FDTreeElement[this.numAttributes]);
                currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
            } else if (currentNode.getChildren()[i] == null) {
                currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
            }
            currentNode = currentNode.getChildren()[i];
            invalidFds.and(currentNode.rhsFds);
            currentNode.addRhsAttributes(rhs);
        }
        rhs.andNot(invalidFds);
        currentNode.markFds(rhs);
        rhs.or(invalidFds);
        this.depth = Math.max(this.depth, lhsLength);
        return currentNode;
    }

    public boolean containsFd(OpenBitSet lhs, int rhs) {
        FDTreeElement element = this;
        for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
            if ((element.getChildren() == null) || (element.getChildren()[i] == null))
                return false;
            element = element.getChildren()[i];
        }
        return element.isFd(rhs);
    }

    public FDTreeElement addGeneralization(OpenBitSet lhs, int rhs) {
        FDTreeElement currentNode = this;
        currentNode.addRhsAttribute(rhs);
        boolean newElement = false;
        for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
            if (currentNode.getChildren() == null) {
                currentNode.setChildren(new FDTreeElement[this.numAttributes]);
                currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
                newElement = true;
            } else if (currentNode.getChildren()[i] == null) {
                currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
                newElement = true;
            }
            currentNode = currentNode.getChildren()[i];
            currentNode.addRhsAttribute(rhs);
        }
        if (newElement)
            return currentNode;
        return null;
    }

    public FDTreeElement addGeneralization(OpenBitSet lhs, OpenBitSet rhs) {
        FDTreeElement currentNode = this;
        currentNode.addRhsAttributes(rhs);
        boolean newElement = false;
        for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
            if (currentNode.getChildren() == null) {
                currentNode.setChildren(new FDTreeElement[this.numAttributes]);
                currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
                newElement = true;
            } else if (currentNode.getChildren()[i] == null) {
                currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
                newElement = true;
            }
            currentNode = currentNode.getChildren()[i];
            currentNode.addRhsAttributes(rhs);
        }
        if (newElement)
            return currentNode;
        return null;
    }

    public boolean containsFdOrGeneralization(OpenBitSet lhs, int rhs) {
        int nextLhsAttr = lhs.nextSetBit(0);
        return this.containsFdOrGeneralization(lhs, rhs, nextLhsAttr);
    }

    public OpenBitSet getFdOrGeneralization(OpenBitSet lhs, int rhs) {
        OpenBitSet foundLhs = new OpenBitSet();
        int nextLhsAttr = lhs.nextSetBit(0);
        if (this.getFdOrGeneralization(lhs, rhs, nextLhsAttr, foundLhs))
            return foundLhs;
        return null;
    }

    public List<OpenBitSet> getFdAndGeneralizations(OpenBitSet lhs, int rhs) {
        List<OpenBitSet> foundLhs = new ArrayList<>();
        OpenBitSet currentLhs = new OpenBitSet();
        int nextLhsAttr = lhs.nextSetBit(0);
        this.getFdAndGeneralizations(lhs, rhs, nextLhsAttr, currentLhs, foundLhs);
        return foundLhs;
    }

    public List<FDTreeElementLhsPair> getLevel(int level) {
        List<FDTreeElementLhsPair> result = new ArrayList<>();
        OpenBitSet currentLhs = new OpenBitSet();
        int currentLevel = 0;
        this.getLevel(level, currentLevel, currentLhs, result);
        return result;
    }

    public void filterGeneralizations() {
        OpenBitSet currentLhs = new OpenBitSet(this.numAttributes);
        this.filterGeneralizations(currentLhs, this);
    }

    public void filterGeneralizations(OpenBitSet lhs, int rhs) {
        OpenBitSet currentLhs = new OpenBitSet(this.numAttributes);
        int nextLhsAttr = lhs.nextSetBit(0);
        this.filterGeneralizations(lhs, rhs, nextLhsAttr, currentLhs);
    }

    public void removeFunctionalDependency(OpenBitSet lhs, int rhs) {
        int currentLhsAttr = lhs.nextSetBit(0);
        this.removeRecursive(lhs, rhs, currentLhsAttr);
    }

    public boolean containsFunctionalDependency(OpenBitSet lhs, int rhs) {
        FDTreeElement currentNode = this;
        for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
            if ((currentNode.getChildren() == null) || (currentNode.getChildren()[i] == null))
                return false;
            currentNode = currentNode.getChildren()[i];
        }
        return currentNode.isFd(rhs);
    }

    public boolean isEmpty() {
        return (this.rhsAttributes.cardinality() == 0);
    }

    public class FD {

        public OpenBitSet lhs;

        public int rhs;

        public PositionListIndex pli;

        public FD(OpenBitSet lhs, int rhs, PositionListIndex pli) {
            this.lhs = lhs;
            this.rhs = rhs;
            this.pli = pli;
        }
    }

    public void addPrunedElements() {
        int numAttributes = this.numAttributes;
        OpenBitSet currentLhs = new OpenBitSet(numAttributes);
        if (this.getChildren() == null)
            return;
        for (int attr = 0; attr < this.numAttributes; attr++) {
            if (this.getChildren()[attr] != null) {
                currentLhs.set(attr);
                this.getChildren()[attr].addPrunedElements(currentLhs, attr, this);
                currentLhs.clear(attr);
            }
        }
    }

    public void growNegative(List<PositionListIndex> plis, int[][] invertedPlis, int numRecords) {
        int numAttributes = plis.size();
        OpenBitSet currentLhs = new OpenBitSet(numAttributes);
        for (int rhs = 0; rhs < numAttributes; rhs++) {
            if (this.isFd(rhs))
                continue;
            if (plis.get(rhs).isConstant(numRecords))
                continue;
            this.markFd(rhs);
            for (int attr = 0; attr < numAttributes; attr++) {
                if (attr == rhs)
                    continue;
                if ((this.getChildren() != null) && (this.getChildren()[attr] != null) && this.getChildren()[attr].hasRhsAttribute(rhs))
                    continue;
                if (!plis.get(attr).refines(invertedPlis[rhs])) {
                    if (this.getChildren() == null)
                        this.setChildren(new FDTreeElement[this.numAttributes]);
                    if (this.getChildren()[attr] == null)
                        this.getChildren()[attr] = new FDTreeElement(this.numAttributes);
                    this.getChildren()[attr].addRhsAttribute(rhs);
                    this.getChildren()[attr].markFd(rhs);
                }
            }
        }
        if (this.getChildren() == null)
            return;
        for (int attr = 0; attr < numAttributes; attr++) {
            if (this.getChildren()[attr] != null) {
                currentLhs.set(attr);
                this.getChildren()[attr].growNegative(plis.get(attr), currentLhs, attr, plis, invertedPlis, this);
                currentLhs.clear(attr);
            }
        }
    }

    public void maximizeNegative(List<PositionListIndex> plis, int[][] invertedPlis, int numRecords) {
        int numAttributes = plis.size();
        OpenBitSet currentLhs = new OpenBitSet(numAttributes);
        if (this.getChildren() != null) {
            for (int attr = 0; attr < numAttributes; attr++) {
                if (this.getChildren()[attr] != null) {
                    currentLhs.set(attr);
                    this.getChildren()[attr].maximizeNegativeRecursive(plis.get(attr), currentLhs, numAttributes, invertedPlis, this);
                    currentLhs.clear(attr);
                }
            }
        }
        this.addInvalidRootFDs(plis, numRecords);
        for (int rhs = this.rhsFds.nextSetBit(0); rhs >= 0; rhs = this.rhsFds.nextSetBit(rhs + 1)) {
            OpenBitSet extensions = currentLhs.clone();
            extensions.flip(0, numAttributes);
            extensions.clear(rhs);
            for (int extensionAttr = extensions.nextSetBit(0); extensionAttr >= 0; extensionAttr = extensions.nextSetBit(extensionAttr + 1)) {
                currentLhs.set(extensionAttr);
                if (this.containsFdOrSpecialization(currentLhs, rhs) || !plis.get(extensionAttr).refines(invertedPlis[rhs])) {
                    this.rhsFds.clear(rhs);
                    FDTreeElement newElement = this.addFunctionalDependency(currentLhs, rhs);
                    newElement.maximizeNegativeRecursive(plis.get(extensionAttr), currentLhs, numAttributes, invertedPlis, this);
                }
                currentLhs.clear(extensionAttr);
            }
        }
    }

    private void addInvalidRootFDs(List<PositionListIndex> plis, int numRecords) {
        for (int rhs = 0; rhs < this.numAttributes; rhs++) if (!plis.get(rhs).isConstant(numRecords))
            this.markFd(rhs);
    }

    public void generalize() {
        int maxLevel = this.numAttributes;
        Int2ObjectOpenHashMap<ArrayList<ElementLhsPair>> level2elements = new Int2ObjectOpenHashMap<>(maxLevel);
        for (int level = 0; level < maxLevel; level++) level2elements.put(level, new ArrayList<>());
        this.addToIndex(level2elements, 0, new OpenBitSet(this.numAttributes));
        for (int level = maxLevel - 1; level >= 0; level--) {
            for (ElementLhsPair pair : level2elements.get(level)) {
                pair.element.removeAllFds();
                for (int lhsAttr = pair.lhs.nextSetBit(0); lhsAttr >= 0; lhsAttr = pair.lhs.nextSetBit(lhsAttr + 1)) {
                    pair.lhs.clear(lhsAttr);
                    FDTreeElement generalization = this.addGeneralization(pair.lhs, pair.element.getRhsAttributes());
                    if (generalization != null)
                        level2elements.get(level - 1).add(new ElementLhsPair(generalization, pair.lhs.clone()));
                    pair.lhs.set(lhsAttr);
                }
            }
        }
    }

    public void grow() {
        this.grow(new OpenBitSet(this.numAttributes), this);
    }

    public void minimize() {
        this.minimize(new OpenBitSet(this.numAttributes), this);
    }
}
