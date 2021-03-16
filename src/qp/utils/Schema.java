/**
 * schema of the table/result, and is attached to every operator
 **/

package qp.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Schema implements Serializable {
    private final List<Attribute> attributes;  // The attributes belong to this schema
    private int tupleSize;                // Number of bytes required for this tuple (size of record)

    public Schema(List<Attribute> colset) {
        attributes = new ArrayList<>();
        for (Object o : colset) {
            attributes.add((Attribute) o);
        }
    }

    public void setTupleSize(int size) {
        tupleSize = size;
    }

    public int getTupleSize() {
        return tupleSize;
    }

    public int getNumCols() {
        return attributes.size();
    }

    public void add(Attribute attr) {
        attributes.add(attr);
    }

    public List<Attribute> getAttList() {
        return attributes;
    }

    public Attribute getAttribute(int i) {
        return attributes.get(i);
    }

    public int indexOf(Attribute tarattr) {
        for (int i = 0; i < attributes.size(); ++i) {
            Attribute attr = attributes.get(i);
            if (attr.equals(tarattr)) {
                return i;
            }
        }
        return -1;
    }

    public int typeOf(Attribute tarattr) {
        for (int i = 0; i < attributes.size(); ++i) {
            Attribute attr = attributes.get(i);
            if (attr.equals(tarattr)) {
                return attr.getType();
            }
        }
        return -1;
    }

    public int typeOf(int attrAt) {
        Attribute attr = attributes.get(attrAt);
        return attr.getType();
    }

    /** Checks whether given attribute is present in this Schema or not **/
    public boolean contains(Attribute tarattr) {
        for (int i = 0; i < attributes.size(); ++i) {
            Attribute attr = attributes.get(i);
            if (attr.equals(tarattr)) {
                return true;
            }
        }
        return false;
    }

    /** The schema of resultant join operation
     Not considered the elimination of duplicate column **/
    public Schema joinWith(Schema right) {
        ArrayList<Attribute> newVector = new ArrayList<>(this.attributes);
        newVector.addAll(right.getAttList());
        int newTupleSize = this.getTupleSize() + right.getTupleSize();
        Schema newSchema = new Schema(newVector);
        newSchema.setTupleSize(newTupleSize);
        return newSchema;
    }

    /** To get schema due to result of project operation
     attrlist is the attirbuted that are projected **/
    public Schema subSchema(List<Attribute> attrlist) {
        ArrayList<Attribute> newVector = new ArrayList<>();
        int newTupleSize = 0;
        for (int i = 0; i < attrlist.size(); ++i) {
            Attribute resAttr = attrlist.get(i);
            int baseIndex = this.indexOf(resAttr.getBaseAttribute());
            Attribute baseAttr = (Attribute) this.getAttribute(baseIndex).clone();
            baseAttr.setAggType(resAttr.getAggType());
            newVector.add(baseAttr);
            if (baseAttr.getAggType() == Attribute.NONE) {
                newTupleSize = newTupleSize + baseAttr.getAttrSize();
            } else {
                newTupleSize = newTupleSize + 4;
            }
        }
        Schema newSchema = new Schema(newVector);
        newSchema.setTupleSize(newTupleSize);
        return newSchema;
    }

    /** Check compatibility for set operations **/
    public boolean checkCompat(Schema right) {
        List<Attribute> rightattrlist = right.getAttList();
        if (attributes.size() != rightattrlist.size()) {
            return false;
        }
        for (int i = 0; i < attributes.size(); ++i) {
            if (attributes.get(i).getProjectedType() != rightattrlist.get(i).getProjectedType()) {
                return false;
            }
        }
        return true;
    }

    public Object clone() {
        ArrayList<Attribute> newVector = new ArrayList<>();
        for (int i = 0; i < attributes.size(); ++i) {
            Attribute newAttribute = (Attribute) (attributes.get(i)).clone();
            newVector.add(newAttribute);
        }
        Schema newSchema = new Schema(newVector);
        newSchema.setTupleSize(tupleSize);
        return newSchema;
    }

}
