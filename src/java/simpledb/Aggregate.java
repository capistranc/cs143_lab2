package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator iter;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;

    private Aggregator agg;
    private DbIterator aggIter;
    
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int agfield, int gbfield, Aggregator.Op agop) {
        this.iter = child;
        this.afield = agfield;
        this.gfield = (gbfield == -1) ? Aggregator.NO_GROUPING : gbfield;
        this.aop = agop;

        Type gType = (this.gfield == -1) ? null : iter.getTupleDesc().getFieldType(gfield);
        Type aggType = iter.getTupleDesc().getFieldType(afield);

        if (aggType == Type.INT_TYPE)
            agg = new IntegerAggregator(this.gfield, gType, afield, aop);
        else if (aggType == Type.STRING_TYPE)
            agg = new StringAggregator(this.gfield, gType, afield, aop);
        else assert(false);


    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	   return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
	   if (this.gfield == Aggregator.NO_GROUPING)
            return null;
       return this.iter.getTupleDesc().getFieldName(this.gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
	return this.iter.getTupleDesc().getFieldName(this.afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
            super.open();
            this.iter.open();
            
            Tuple tup;
            while (this.iter.hasNext()) 
            {
                tup = this.iter.next();
                System.out.println(tup.toString());
                agg.mergeTupleIntoGroup(tup);
            }

            this.aggIter = this.agg.iterator(); //Need to fill up the tuple array list before opening the iterator
            this.aggIter.open();

    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	   if (this.aggIter.hasNext()) {
            Tuple tup = this.aggIter.next();
            //System.out.println(tup.toString());
            return tup;
       }
	return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.iter.rewind();
        this.aggIter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc()
    {
        String[] groupName;
        Type[] groupType;
        Type gType = (this.gfield == -1) ? null : iter.getTupleDesc().getFieldType(gfield);

        if (this.gfield == Aggregator.NO_GROUPING) 
        {
            groupName = new String[] {this.aggregateFieldName()};
            groupType = new Type[] {Type.INT_TYPE};
        } else
        {
            groupName = new String[] {"groupVal", iter.getTupleDesc().getFieldName(afield)};
            groupType = new Type[] {gType, Type.INT_TYPE};
        }

        return new TupleDesc(groupType, groupName);
    }
    

    public void close() {
    super.close();
	this.iter.close();
    this.aggIter.close();
    
    }

    @Override
    public DbIterator[] getChildren() {
	DbIterator[] child = new DbIterator[1];
    child[0] = this.aggIter;
	return child;
    }

    @Override
    public void setChildren(DbIterator[] children) {
	this.aggIter = children[0];
    }
    
}
