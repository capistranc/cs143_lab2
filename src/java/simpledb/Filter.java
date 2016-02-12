package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private Predicate pred;
    private DbIterator iter;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.pred = p;
        this.iter = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.pred;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.iter.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        this.iter.open();
    }

    public void close() {
        super.close();
        this.iter.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.iter.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {

        Tuple temp = null;
        while (this.iter.hasNext())
        {
            temp = this.iter.next();
            if (this.pred.filter(temp))
                return temp;
        }

        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator[] children = new DbIterator[1];
        children[1] = this.iter;
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) 
    {
        if (children.length > 0)
            this.iter = children[0];
    }

}
