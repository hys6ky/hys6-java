package hyren.serv6.hadoop.commons.algorithms.helper;

import hyren.serv6.commons.hadoop.algorithms.helper.ColumnCombination;
import hyren.serv6.commons.hadoop.algorithms.helper.ColumnIdentifier;
import hyren.serv6.commons.hadoop.algorithms.helper.FunctionalDependency;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.lucene.util.OpenBitSet;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FDTreeElement implements Serializable {

    private static final long serialVersionUID = 1L;

    protected FDTreeElement[] children;

    protected OpenBitSet rhsAttributes;

    protected OpenBitSet rhsFds;

    protected int numAttributes;

    public FDTreeElement(int numAttributes) {
        this.rhsAttributes = new OpenBitSet(numAttributes);
        this.rhsFds = new OpenBitSet(numAttributes);
        this.numAttributes = numAttributes;
    }

    public int getNumAttributes() {
        return this.numAttributes;
    }

    public FDTreeElement[] getChildren() {
        return this.children;
    }

    public void setChildren(FDTreeElement[] children) {
        this.children = children;
    }

    public OpenBitSet getRhsAttributes() {
        return this.rhsAttributes;
    }

    public void addRhsAttribute(int i) {
        this.rhsAttributes.set(i);
    }

    public void addRhsAttributes(OpenBitSet other) {
        this.rhsAttributes.or(other);
    }

    public void removeRhsAttribute(int i) {
        this.rhsAttributes.clear(i);
    }

    public boolean hasRhsAttribute(int i) {
        return this.rhsAttributes.get(i);
    }

    public OpenBitSet getFds() {
        return this.rhsFds;
    }

    public void markFd(int i) {
        this.rhsFds.set(i);
    }

    public void markFds(OpenBitSet other) {
        this.rhsFds.or(other);
    }

    public void removeFd(int i) {
        this.rhsFds.clear(i);
    }

    public void retainFds(OpenBitSet other) {
        this.rhsFds.and(other);
    }

    public void setFds(OpenBitSet other) {
        this.rhsFds = other;
    }

    public void removeAllFds() {
        this.rhsFds.clear(0, this.numAttributes);
    }

    public boolean isFd(int i) {
        return this.rhsFds.get(i);
    }

    protected void trimRecursive(int currentDepth, int newDepth) {
        if (currentDepth == newDepth) {
            this.children = null;
            this.rhsAttributes.and(this.rhsFds);
            return;
        }
        if (this.children != null)
            for (FDTreeElement child : this.children) if (child != null)
                child.trimRecursive(currentDepth + 1, newDepth);
    }

    protected void filterGeneralizations(OpenBitSet currentLhs, FDTree tree) {
        if (this.children != null) {
            for (int attr = 0; attr < this.numAttributes; attr++) {
                if (this.children[attr] != null) {
                    currentLhs.set(attr);
                    this.children[attr].filterGeneralizations(currentLhs, tree);
                    currentLhs.clear(attr);
                }
            }
        }
        for (int rhs = this.rhsFds.nextSetBit(0); rhs >= 0; rhs = this.rhsFds.nextSetBit(rhs + 1)) tree.filterGeneralizations(currentLhs, rhs);
    }

    protected void filterGeneralizations(OpenBitSet lhs, int rhs, int currentLhsAttr, OpenBitSet currentLhs) {
        if (currentLhs.equals(lhs))
            return;
        this.rhsFds.clear(rhs);
        if (currentLhsAttr < 0)
            return;
        if (this.children != null) {
            for (int nextLhsAttr = lhs.nextSetBit(currentLhsAttr); nextLhsAttr >= 0; nextLhsAttr = lhs.nextSetBit(nextLhsAttr + 1)) {
                if ((this.children[nextLhsAttr] != null) && (this.children[nextLhsAttr].hasRhsAttribute(rhs))) {
                    currentLhs.set(nextLhsAttr);
                    this.children[nextLhsAttr].filterGeneralizations(lhs, rhs, lhs.nextSetBit(nextLhsAttr + 1), currentLhs);
                    currentLhs.clear(nextLhsAttr);
                }
            }
        }
    }

    protected boolean containsFdOrGeneralization(OpenBitSet lhs, int rhs, int currentLhsAttr) {
        if (this.isFd(rhs))
            return true;
        if (currentLhsAttr < 0)
            return false;
        int nextLhsAttr = lhs.nextSetBit(currentLhsAttr + 1);
        if ((this.children != null) && (this.children[currentLhsAttr] != null) && (this.children[currentLhsAttr].hasRhsAttribute(rhs)))
            if (this.children[currentLhsAttr].containsFdOrGeneralization(lhs, rhs, nextLhsAttr))
                return true;
        return this.containsFdOrGeneralization(lhs, rhs, nextLhsAttr);
    }

    protected boolean getFdOrGeneralization(OpenBitSet lhs, int rhs, int currentLhsAttr, OpenBitSet foundLhs) {
        if (this.isFd(rhs))
            return true;
        if (currentLhsAttr < 0)
            return false;
        int nextLhsAttr = lhs.nextSetBit(currentLhsAttr + 1);
        if ((this.children != null) && (this.children[currentLhsAttr] != null) && (this.children[currentLhsAttr].hasRhsAttribute(rhs))) {
            if (this.children[currentLhsAttr].getFdOrGeneralization(lhs, rhs, nextLhsAttr, foundLhs)) {
                foundLhs.set(currentLhsAttr);
                return true;
            }
        }
        return this.getFdOrGeneralization(lhs, rhs, nextLhsAttr, foundLhs);
    }

    protected void getFdAndGeneralizations(OpenBitSet lhs, int rhs, int currentLhsAttr, OpenBitSet currentLhs, List<OpenBitSet> foundLhs) {
        if (this.isFd(rhs))
            foundLhs.add(currentLhs.clone());
        if (this.children == null)
            return;
        while (currentLhsAttr >= 0) {
            int nextLhsAttr = lhs.nextSetBit(currentLhsAttr + 1);
            if ((this.children[currentLhsAttr] != null) && (this.children[currentLhsAttr].hasRhsAttribute(rhs))) {
                currentLhs.set(currentLhsAttr);
                this.children[currentLhsAttr].getFdAndGeneralizations(lhs, rhs, nextLhsAttr, currentLhs, foundLhs);
                currentLhs.clear(currentLhsAttr);
            }
            currentLhsAttr = nextLhsAttr;
        }
    }

    public void getLevel(int level, int currentLevel, OpenBitSet currentLhs, List<FDTreeElementLhsPair> result) {
        if (level == currentLevel) {
            result.add(new FDTreeElementLhsPair(this, currentLhs.clone()));
        } else {
            currentLevel++;
            if (this.children == null)
                return;
            for (int child = 0; child < this.numAttributes; child++) {
                if (this.children[child] == null)
                    continue;
                currentLhs.set(child);
                this.children[child].getLevel(level, currentLevel, currentLhs, result);
                currentLhs.clear(child);
            }
        }
    }

    public boolean containsFdOrSpecialization(OpenBitSet lhs, int rhs) {
        int currentLhsAttr = lhs.nextSetBit(0);
        return this.containsFdOrSpecialization(lhs, rhs, currentLhsAttr);
    }

    protected boolean containsFdOrSpecialization(OpenBitSet lhs, int rhs, int currentLhsAttr) {
        if (!this.hasRhsAttribute(rhs))
            return false;
        if (currentLhsAttr < 0)
            return true;
        if (this.children == null)
            return false;
        for (int child = 0; child < this.numAttributes; child++) {
            if (this.children[child] == null)
                continue;
            if (child == currentLhsAttr) {
                if (this.children[child].containsFdOrSpecialization(lhs, rhs, lhs.nextSetBit(currentLhsAttr + 1)))
                    return true;
            } else {
                if (this.children[child].containsFdOrSpecialization(lhs, rhs, currentLhsAttr))
                    return true;
            }
        }
        return false;
    }

    protected boolean removeRecursive(OpenBitSet lhs, int rhs, int currentLhsAttr) {
        if (currentLhsAttr < 0) {
            this.removeFd(rhs);
            this.removeRhsAttribute(rhs);
            return true;
        }
        if ((this.children != null) && (this.children[currentLhsAttr] != null)) {
            if (!this.children[currentLhsAttr].removeRecursive(lhs, rhs, lhs.nextSetBit(currentLhsAttr + 1)))
                return false;
            if (this.children[currentLhsAttr].getRhsAttributes().cardinality() == 0)
                this.children[currentLhsAttr] = null;
        }
        if (this.isLastNodeOf(rhs)) {
            this.removeRhsAttribute(rhs);
            return true;
        }
        return false;
    }

    protected boolean isLastNodeOf(int rhs) {
        if (this.children == null)
            return true;
        for (FDTreeElement child : this.children) if ((child != null) && child.hasRhsAttribute(rhs))
            return false;
        return true;
    }

    protected void addOneSmallerGeneralizations(OpenBitSet currentLhs, int maxCurrentLhsAttribute, int rhs, FDTree tree) {
        for (int lhsAttribute = currentLhs.nextSetBit(0); lhsAttribute != maxCurrentLhsAttribute; lhsAttribute = currentLhs.nextSetBit(lhsAttribute + 1)) {
            currentLhs.clear(lhsAttribute);
            tree.addGeneralization(currentLhs, rhs);
            currentLhs.set(lhsAttribute);
        }
    }

    protected void addOneSmallerGeneralizations(OpenBitSet currentLhs, int maxCurrentLhsAttribute, OpenBitSet rhs, FDTree tree) {
        for (int lhsAttribute = currentLhs.nextSetBit(0); lhsAttribute != maxCurrentLhsAttribute; lhsAttribute = currentLhs.nextSetBit(lhsAttribute + 1)) {
            currentLhs.clear(lhsAttribute);
            tree.addGeneralization(currentLhs, rhs);
            currentLhs.set(lhsAttribute);
        }
    }

    public void addPrunedElements(OpenBitSet currentLhs, int maxCurrentLhsAttribute, FDTree tree) {
        this.addOneSmallerGeneralizations(currentLhs, maxCurrentLhsAttribute, this.rhsAttributes, tree);
        if (this.children == null)
            return;
        for (int attr = 0; attr < this.numAttributes; attr++) {
            if (this.children[attr] != null) {
                currentLhs.set(attr);
                this.children[attr].addPrunedElements(currentLhs, attr, tree);
                currentLhs.clear(attr);
            }
        }
    }

    public void growNegative(PositionListIndex currentPli, OpenBitSet currentLhs, int maxCurrentLhsAttribute, List<PositionListIndex> plis, int[][] rhsPlis, FDTree invalidFds) {
        int numAttributes = plis.size();
        PositionListIndex[] childPlis = new PositionListIndex[numAttributes];
        for (int rhs = this.rhsAttributes.nextSetBit(0); rhs >= 0; rhs = this.rhsAttributes.nextSetBit(rhs + 1)) {
            for (int attr = maxCurrentLhsAttribute + 1; attr < numAttributes; attr++) {
                if (attr == rhs)
                    continue;
                if ((this.children != null) && (this.children[attr] != null) && this.children[attr].hasRhsAttribute(rhs))
                    continue;
                if (childPlis[attr] == null)
                    childPlis[attr] = currentPli.intersect(rhsPlis[attr]);
                if (!childPlis[attr].refines(rhsPlis[rhs])) {
                    if (this.children == null)
                        this.children = new FDTreeElement[this.numAttributes];
                    if (this.children[attr] == null)
                        this.children[attr] = new FDTreeElement(this.numAttributes);
                    this.children[attr].addRhsAttribute(rhs);
                    this.children[attr].markFd(rhs);
                    currentLhs.set(attr);
                    this.addOneSmallerGeneralizations(currentLhs, attr, rhs, invalidFds);
                    currentLhs.clear(attr);
                }
            }
        }
        if (this.children != null) {
            for (int i = 0; i < numAttributes; i++) if (this.children[i] == null)
                childPlis[i] = null;
            for (int attr = 0; attr < numAttributes; attr++) {
                if (this.children[attr] != null) {
                    if (childPlis[attr] == null)
                        childPlis[attr] = currentPli.intersect(rhsPlis[attr]);
                    currentLhs.set(attr);
                    this.children[attr].growNegative(childPlis[attr], currentLhs, attr, plis, rhsPlis, invalidFds);
                    currentLhs.clear(attr);
                    childPlis[attr] = null;
                }
            }
        }
    }

    protected void maximizeNegativeRecursive(PositionListIndex currentPli, OpenBitSet currentLhs, int numAttributes, int[][] rhsPlis, FDTree invalidFds) {
        PositionListIndex[] childPlis = new PositionListIndex[numAttributes];
        if (this.children != null) {
            for (int attr = 0; attr < numAttributes; attr++) {
                if (this.children[attr] != null) {
                    childPlis[attr] = currentPli.intersect(rhsPlis[attr]);
                    currentLhs.set(attr);
                    this.children[attr].maximizeNegativeRecursive(childPlis[attr], currentLhs, numAttributes, rhsPlis, invalidFds);
                    currentLhs.clear(attr);
                }
            }
        }
        for (int rhs = this.rhsFds.nextSetBit(0); rhs >= 0; rhs = this.rhsFds.nextSetBit(rhs + 1)) {
            OpenBitSet extensions = currentLhs.clone();
            extensions.flip(0, numAttributes);
            extensions.clear(rhs);
            for (int extensionAttr = extensions.nextSetBit(0); extensionAttr >= 0; extensionAttr = extensions.nextSetBit(extensionAttr + 1)) {
                currentLhs.set(extensionAttr);
                if (childPlis[extensionAttr] == null)
                    childPlis[extensionAttr] = currentPli.intersect(rhsPlis[extensionAttr]);
                if (!childPlis[extensionAttr].refines(rhsPlis[rhs])) {
                    this.rhsFds.clear(rhs);
                    FDTreeElement newElement = invalidFds.addFunctionalDependency(currentLhs, rhs);
                    newElement.maximizeNegativeRecursive(childPlis[extensionAttr], currentLhs, numAttributes, rhsPlis, invalidFds);
                }
                currentLhs.clear(extensionAttr);
            }
        }
    }

    public void addFunctionalDependenciesInto(List<FunctionalDependency> functionalDependencies, OpenBitSet lhs, ObjectArrayList<ColumnIdentifier> columnIdentifiers, List<PositionListIndex> plis) {
        for (int rhs = this.rhsFds.nextSetBit(0); rhs >= 0; rhs = this.rhsFds.nextSetBit(rhs + 1)) {
            ColumnIdentifier[] columns = new ColumnIdentifier[(int) lhs.cardinality()];
            int j = 0;
            for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
                int columnId = plis.get(i).getAttribute();
                columns[j++] = columnIdentifiers.get(columnId);
            }
            ColumnCombination colCombination = new ColumnCombination(columns);
            int rhsId = plis.get(rhs).getAttribute();
            FunctionalDependency fdResult = new FunctionalDependency(colCombination, columnIdentifiers.get(rhsId));
            functionalDependencies.add(fdResult);
        }
        if (this.getChildren() == null)
            return;
        for (int childAttr = 0; childAttr < this.numAttributes; childAttr++) {
            FDTreeElement element = this.getChildren()[childAttr];
            if (element != null) {
                lhs.set(childAttr);
                element.addFunctionalDependenciesInto(functionalDependencies, lhs, columnIdentifiers, plis);
                lhs.clear(childAttr);
            }
        }
    }

    public int writeFunctionalDependencies(OutputStreamWriter writer, OpenBitSet lhs, ObjectArrayList<ColumnIdentifier> columnIdentifiers, boolean writeTableNamePrefix) throws IOException {
        int numFDs = (int) this.rhsFds.cardinality();
        if (numFDs != 0) {
            List<String> lhsIdentifier = new ArrayList<>();
            for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
                int columnId = i;
                if (writeTableNamePrefix)
                    lhsIdentifier.add(columnIdentifiers.get(columnId).toString());
                else
                    lhsIdentifier.add(columnIdentifiers.get(columnId).getColumnIdentifier());
            }
            Collections.sort(lhsIdentifier);
            String lhsString = "[" + CollectionUtils.concat(lhsIdentifier, ",") + "]";
            List<String> rhsIdentifier = new ArrayList<>();
            for (int i = this.rhsFds.nextSetBit(0); i >= 0; i = this.rhsFds.nextSetBit(i + 1)) {
                int columnId = i;
                if (writeTableNamePrefix)
                    rhsIdentifier.add(columnIdentifiers.get(columnId).toString());
                else
                    rhsIdentifier.add(columnIdentifiers.get(columnId).getColumnIdentifier());
            }
            Collections.sort(rhsIdentifier);
            String rhsString = CollectionUtils.concat(rhsIdentifier, ",");
            writer.write(lhsString + ":" + rhsString + "\n");
        }
        if (this.getChildren() == null)
            return numFDs;
        for (int childAttr = 0; childAttr < this.numAttributes; childAttr++) {
            FDTreeElement element = this.getChildren()[childAttr];
            if (element != null) {
                lhs.set(childAttr);
                numFDs += element.writeFunctionalDependencies(writer, lhs, columnIdentifiers, writeTableNamePrefix);
                lhs.clear(childAttr);
            }
        }
        return numFDs;
    }

    public boolean filterDeadElements() {
        boolean allChildrenFiltered = true;
        if (this.children != null) {
            for (int childAttr = 0; childAttr < this.numAttributes; childAttr++) {
                FDTreeElement element = this.children[childAttr];
                if (element != null) {
                    if (element.filterDeadElements())
                        this.children[childAttr] = null;
                    else
                        allChildrenFiltered = false;
                }
            }
        }
        return allChildrenFiltered && (this.rhsFds.nextSetBit(0) < 0);
    }

    protected class ElementLhsPair {

        public FDTreeElement element = null;

        public OpenBitSet lhs = null;

        public ElementLhsPair(FDTreeElement element, OpenBitSet lhs) {
            this.element = element;
            this.lhs = lhs;
        }
    }

    protected void addToIndex(Int2ObjectOpenHashMap<ArrayList<ElementLhsPair>> level2elements, int level, OpenBitSet lhs) {
        level2elements.get(level).add(new ElementLhsPair(this, lhs.clone()));
        if (this.children != null) {
            for (int childAttr = 0; childAttr < this.numAttributes; childAttr++) {
                FDTreeElement element = this.children[childAttr];
                if (element != null) {
                    lhs.set(childAttr);
                    element.addToIndex(level2elements, level + 1, lhs);
                    lhs.clear(childAttr);
                }
            }
        }
    }

    public void grow(OpenBitSet lhs, FDTree fdTree) {
        OpenBitSet rhs = this.rhsAttributes;
        OpenBitSet invalidRhs = rhs.clone();
        invalidRhs.remove(this.rhsFds);
        if (invalidRhs.cardinality() > 0) {
            for (int extensionAttr = 0; extensionAttr < this.numAttributes; extensionAttr++) {
                if (lhs.get(extensionAttr) || rhs.get(extensionAttr))
                    continue;
                lhs.set(extensionAttr);
                fdTree.addFunctionalDependencyIfNotInvalid(lhs, invalidRhs);
                lhs.clear(extensionAttr);
            }
        }
        if (this.children != null) {
            for (int childAttr = 0; childAttr < this.numAttributes; childAttr++) {
                FDTreeElement element = this.children[childAttr];
                if (element != null) {
                    lhs.set(childAttr);
                    element.grow(lhs, fdTree);
                    lhs.clear(childAttr);
                }
            }
        }
    }

    protected void minimize(OpenBitSet lhs, FDTree fdTree) {
        if (this.children != null) {
            for (int childAttr = 0; childAttr < this.numAttributes; childAttr++) {
                FDTreeElement element = this.children[childAttr];
                if (element != null) {
                    lhs.set(childAttr);
                    element.minimize(lhs, fdTree);
                    lhs.clear(childAttr);
                }
            }
        }
        for (int rhs = this.rhsFds.nextSetBit(0); rhs >= 0; rhs = this.rhsFds.nextSetBit(rhs + 1)) {
            this.rhsFds.clear(rhs);
            if (!fdTree.containsFdOrGeneralization(lhs, rhs))
                this.rhsFds.set(rhs);
        }
    }
}
