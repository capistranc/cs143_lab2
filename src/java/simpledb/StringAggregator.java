package simpledb;

import java.util.ArrayList;
import java.util.HashMap;


/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op agop;

    private HashMap<Field,Integer> groups;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {

        //System.out.println("String aggregate initialized");
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.agop = what;

        this.groups = new HashMap<Field,Integer>();

        if (this.agop != Op.COUNT)
            throw new IllegalArgumentException("incompatible aggregate operator");
    }   

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        
        Field gfield = (this.gbfield == Aggregator.NO_GROUPING) ? null : tup.getField(this.gbfield);

        if (!groups.containsKey(gfield))
            groups.put(gfield, 0);

        Integer count = groups.get(gfield) + 1;
        groups.put(gfield, count);
    }

//Creates a new tupleDesc for the grouped by tuples.
    public TupleDesc getTupleDesc()
    {
        String[] groupName;
        Type[] groupType;

        if (this.gbfield == Aggregator.NO_GROUPING) 
        {
            groupName = new String[] {"aggregateVal"};
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
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        TupleDesc grouptd = getTupleDesc();
        Integer count;
        Tuple tup;

        for (Field groupVal : groups.keySet()) 
        {
            count = groups.get(groupVal);
            tup = new Tuple(grouptd);

            if (this.gbfield == Aggregator.NO_GROUPING) {
                tup.setField(0, new IntField(count));
            } else {
                tup.setField(0, groupVal);
                tup.setField(1, new IntField(count));
            }

            tuples.add(tup);
        }

        return new TupleIterator(grouptd, tuples);
    }

}
