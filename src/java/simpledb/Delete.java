package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    TransactionId tid;
    DbIterator iter;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.tid = t;
        this.iter = child;
    }

    public TupleDesc getTupleDesc() {
        String [] fieldName = {"Deleted Tuples"};
        Type [] fieldType = {Type.INT_TYPE};

        return new TupleDesc(fieldType, fieldName);
    }

    public void open() throws DbException, TransactionAbortedException {
        this.iter.open();
        super.open();
    }

    public void close() {
        this.iter.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.iter.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator child[] = new DbIterator[1];
        child[0] = this.iter;
        return child;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.iter = children[0];
    }

}
