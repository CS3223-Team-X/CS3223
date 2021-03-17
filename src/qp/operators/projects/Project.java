package qp.operators.projects;

import qp.operators.Operator;
import qp.operators.OperatorType;
import qp.operators.projects.aggregate.*;
import qp.utils.*;

import java.util.ArrayList;
import java.util.List;

/**
 * The projector of required attributes.
 */
public class Project extends Operator {
    private Operator base;                 // Base table to project
    private final int numTuplesPerPage;

    private final List<Attribute> projectedAttributes;
    private final int[] projectedIndices;

    private boolean requiresAggregation;
    private final List<Object> aggregators;

    private Batch inputPage;

    public Project(Operator base, List<Attribute> projectedAttributes) {
        super(OperatorType.PROJECT);
        this.base = base;

        numTuplesPerPage = Batch.getPageSize() / schema.getTupleSize();

        this.projectedAttributes = projectedAttributes;
        this.projectedIndices = computeProjectedIndices(this.projectedAttributes);

        requiresAggregation = false;
        aggregators = initAggregators();
    }

    private int[] computeProjectedIndices(List<Attribute> projectedAttributes) {
        int[] projectedIndices = new int[projectedAttributes.size()];
        for (int i = 0; i < projectedAttributes.size(); i++) {
            Attribute projectedAttribute = projectedAttributes.get(i);
            if (projectedAttribute.getAggType() == Attribute.NONE) {
                projectedIndices[i] = base.getSchema().indexOf(projectedAttribute.getBaseAttribute());
            } else { //TODO do more thorough check
                requiresAggregation = true;
                projectedIndices[i] = base.getSchema().indexOf(projectedAttribute);
            }
            projectedIndices[i] = base.getSchema().indexOf(projectedAttributes.get(i));
        }
        return projectedIndices;
    }

    private List<Object> initAggregators() {
        return List.of(
                new MaxAggregator(),
                new MinAggregator(),
                new SumAggregator(),
                new CountAggregator(),
                new AvgAggregator()
        );
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public List<Attribute> getProjAttr() {
        return projectedAttributes;
    }

    @Override
    public boolean open() {
        return base.open();
    }

    /**
     * Read next tuple from operator
     */
    @Override
    public Batch next() {
        Batch outputPage = new Batch(numTuplesPerPage);
        if (requiresAggregation) {
            while ((inputPage = base.next()) != null) {
                for (Tuple record : inputPage.getRecords()) {
                    for (int i = 0; i < projectedAttributes.size(); i++) {
                        int aggregateType = projectedAttributes.get(i).getAggType();
                        Object projectedDatum = record.getData(projectedIndices[i]);
                        switch (aggregateType) {
                            case Attribute.MAX:
                                if (!TypeChecker.isType(projectedDatum, Integer.class, Float.class)) {
                                    throw new RuntimeException();
                                }
                                MaxAggregator maxAggregator = (MaxAggregator) aggregators.get(aggregateType - 1);
                                maxAggregator.include((float) projectedDatum);
                                break;
                            case Attribute.MIN:
                                if (!TypeChecker.isType(projectedDatum, Integer.class, Float.class)) {
                                    throw new RuntimeException();
                                }
                                MinAggregator minAggregator = (MinAggregator) aggregators.get(aggregateType - 1);
                                minAggregator.include((float) projectedDatum);
                                break;
                            case Attribute.SUM:
                                if (!TypeChecker.isType(projectedDatum, Integer.class, Float.class)) {
                                    throw new RuntimeException();
                                }
                                SumAggregator sumAggregator = (SumAggregator) aggregators.get(aggregateType - 1);
                                sumAggregator.include((float) projectedDatum);
                                break;
                            case Attribute.COUNT:
                                CountAggregator countAggregator = (CountAggregator) aggregators.get(aggregateType - 1);
                                countAggregator.include(CountAggregator.DONT_CARE);
                                break;
                            case Attribute.AVG:
                                if (!TypeChecker.isType(projectedDatum, Integer.class, Float.class)) {
                                    throw new RuntimeException();
                                }
                                AvgAggregator avgAggregator = (AvgAggregator) aggregators.get(aggregateType - 1);
                                avgAggregator.include((float) projectedDatum);
                                break;
                            default:
                                throw new RuntimeException();
                        }

                    }
                }
            }

            List<Object> aggregates = new ArrayList<>();
            for (int i = 0; i < projectedIndices.length; i++) {
                int aggregateType = projectedAttributes.get(i).getAggType();
                switch (aggregateType) {
                    case Attribute.MAX:
                        aggregates.add(((MaxAggregator) aggregators.get(aggregateType - 1)).get());
                        break;
                    case Attribute.MIN:
                        aggregates.add(((MinAggregator) aggregators.get(aggregateType - 1)).get());
                        break;
                    case Attribute.SUM:
                        aggregates.add(((SumAggregator) aggregators.get(aggregateType - 1)).get());
                        break;
                    case Attribute.COUNT:
                        aggregates.add(((CountAggregator) aggregators.get(aggregateType - 1)).get());
                        break;
                    case Attribute.AVG:
                        aggregates.add(((AvgAggregator) aggregators.get(aggregateType - 1)).get());
                        break;
                    default:
                        throw new RuntimeException();
                }
            }

            Tuple projectedRecord = new Tuple(aggregates);
            outputPage.addRecord(projectedRecord);

        } else {
            inputPage = base.next();
            if (inputPage == null) {
                return null;
            }

            for (int i = 0; i < inputPage.size(); i++) {
                Tuple record = inputPage.getRecord(i);
                List<Object> projectedData = new ArrayList<>();
                for (int j = 0; j < projectedAttributes.size(); j++) {
                    Object projectedDatum = record.getData(projectedIndices[j]);
                    projectedData.add(projectedDatum);
                }
                Tuple projectedRecord = new Tuple(projectedData);
                outputPage.addRecord(projectedRecord);
            }
        }

        return outputPage;
    }

    @Override
    public boolean close() {
        inputPage = null;
        base.close();
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < projectedAttributes.size(); ++i)
            newattr.add((Attribute) projectedAttributes.get(i).clone());
        Project newproj = new Project(newbase, newattr);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newproj.setSchema(newSchema);
        return newproj;
    }

}
