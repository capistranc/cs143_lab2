package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.lang.Integer;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op agop;

    private HashMap<Field,Integer> groups;
    private HashMap<Field,Integer> aggVal;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.agop = what;

        this.groups = new HashMap<Field, Integer>();
        this.aggVal = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) 
    {
        
        Field gfield = (this.gbfield == Aggregator.NO_GROUPING) ? null : tup.getField(this.gbfield);

        if (!aggVal.containsKey(gfield)) 
        {
            groups.put(gfield, 0);
            
            if (this.agop == Op.MAX)
                aggVal.put(gfield, Integer.MIN_VALUE);
            else if (this.agop == Op.MIN)
                aggVal.put(gfield, Integer.MAX_VALUE);
            else
               aggVal.put(gfield, 0);
        }

        Integer tupVal = ((IntField)tup.getField(this.afield)).getValue();

        Integer currentCount = groups.get(gfield);
        Integer currentVal = aggVal.get(gfield);

        Integer newVal = currentVal;
        
        switch (this.agop) 
        {
            case COUNT:
                 newVal = currentVal + 1;
                 break;

            case MAX:
                if (tupVal > currentVal)
                    newVal = tupVal;
                break;
            case MIN:
                if (tupVal< currentVal)
                    newVal = tupVal;
                break;
            case SUM:
                newVal = currentVal + tupVal;
                break;
            case AVG:
                groups.put(gfield, currentCount + 1);
                newVal = currentVal + tupVal;
                break;
            default:
                break;
        }

        aggVal.put(gfield, newVal);
        
    }

//Creates a new tupleDesc for the grouped by tuples.
    public TupleDesc getTupleDesc()
    {
        String[] groupName;
        Type[] groupType;

        if (this.gbfield == Aggregator.NO_GROUPING) 
        {
            groupName = new String[] {"aggrregateVal"};
            groupType = new Type[] {Type.INT_TYPE};
        } else
        {
            groupName = new String[] {"groupVal", "aggregateVal"};
            groupType = new Type[] {this.gbfieldtype, Type.INT_TYPE};
        }

        return new TupleDesc(groupType, groupName);
    }



    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator()
    {
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        TupleDesc grouptd = getTupleDesc();
        Integer val;
        Tuple tup;

        for (Field groupKey : aggVal.keySet()) 
        {
            tup = new Tuple(grouptd);
            val = aggVal.get(groupKey);

            val = (this.agop == Op.AVG) ? (val / groups.get(groupKey)) : val;

            if (this.gbfield == Aggregator.NO_GROUPING) {
                tup.setField(0, new IntField(val));
            } else {
                tup.setField(0, groupKey);
                tup.setField(1, new IntField(val));
            }

            tuples.add(tup);
            }


        return new TupleIterator(grouptd, tuples);
    
    }

}
