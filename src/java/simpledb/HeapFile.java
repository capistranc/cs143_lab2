package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    File file;
    TupleDesc tupleDesc;
    int pageSize;

    public class HeapFileIterator implements DbFileIterator {
        public HeapFileIterator(TransactionId transactionId, HeapFile heapFile) {
            this.transactionId = transactionId;
            this.heapFile = heapFile;
            this.currentPageNum = 0;
            this.tupleIterator = null;
        }

        public void open()
            throws DbException, TransactionAbortedException {
            // note the static method for getting one buffer pool
            BufferPool bufferPool = Database.getBufferPool();
            HeapPageId pageId = new HeapPageId(this.heapFile.getId(), this.currentPageNum);

            try {
                HeapPage heapPage = (HeapPage) bufferPool.getPage(transactionId, pageId, Permissions.READ_ONLY);
                this.tupleIterator = heapPage.iterator();
            } catch (ClassCastException e) {
                // in case the indicated pageId does not correspond with a heap page
            }
        }

        /** @return true if there are more tuples available. */
        public boolean hasNext()
            throws DbException, TransactionAbortedException {
            if (this.tupleIterator == null) {
                return false;
            }
            if (this.tupleIterator.hasNext()) {
                return true;
            } else {
                this.currentPageNum++;
                BufferPool bufferPool = Database.getBufferPool();
                
                // hasNext, a peek function, would cause bufferPool to load; in case that the next page is empty
                while (this.currentPageNum < this.heapFile.numPages()) {
                    HeapPageId pageId = new HeapPageId(this.heapFile.getId(), this.currentPageNum);
                    try {
                        HeapPage heapPage = (HeapPage) bufferPool.getPage(transactionId, pageId, Permissions.READ_ONLY);
                        if (heapPage.iterator().hasNext()) {
                            // Check: if setting iterator here would cause issues: iterator can be thought of as being the pseudohead of a linked list?
                            this.tupleIterator = heapPage.iterator();
                            return true;
                        } else {
                            this.currentPageNum++;
                        }
                    } catch (ClassCastException e) {
                        // in case the indicated pageId does not correspond with a heap page
                    }
                }
                return false;
            }
        }

        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        public Tuple next()
            throws DbException, TransactionAbortedException, NoSuchElementException {
            if (hasNext()) {
                return this.tupleIterator.next();
            } else {
                throw new NoSuchElementException();
            }
        }

        /**
         * Resets the iterator to the start.
         * @throws DbException When rewind is unsupported.
         */
        public void rewind() throws DbException, TransactionAbortedException {
            // TODO: rewind function not tested; rewind spec: should open() be called after rewind, or does rewind contain open?
            if (this.tupleIterator != null) {
                this.close();
            }
            this.open();
        }

        /**
         * Closes the iterator.
         */
        public void close() {
            this.tupleIterator = null;
            this.currentPageNum = 0;
        }

        private Iterator<Tuple> tupleIterator;
        private int currentPageNum;
        private TransactionId transactionId;
        private HeapFile heapFile;
    }

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
        this.pageSize = BufferPool.getPageSize();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    // Check: for now process FileNotFoundException and IOException are caught here, probably don't want them thrown to calling function
    public Page readPage(PageId pid) {
        int pageNo = pid.pageNumber();
        try {
            RandomAccessFile raf = new RandomAccessFile(this.file, "r");
            if (pageNo < numPages()) {
                byte[] pageData = new byte[this.pageSize];
                // Check: the offset param in read's not doing what I supposed it would do; added seek call instead
                raf.seek(pageNo * this.pageSize);
                raf.read(pageData, 0, this.pageSize);
                // A cast exception would be thrown if pid cannot be converted to HeapPageId
                HeapPage heapPage = new HeapPage((HeapPageId)pid, pageData);
                return heapPage;
            } else {
                throw new IllegalArgumentException();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(this.file, "rw");

        int offset = this.pageSize * page.getId().pageNumber(); 
        raf.seek(offset);

        byte[] newData = page.getPageData();

        raf.write(newData, 0, this.pageSize);
        
        page.markDirty(false, null);

        raf.close();

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (this.file.length() / (long)this.pageSize);
    }

    public HeapPage getFreePage(TransactionId tid) 
    {


        return null;
    }


    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException 
    {
        HeapPage hpage = null;
        ArrayList<Page> ret = new ArrayList<Page>();

        int len = this.numPages();

        for (int i = 0; i < len; i++) 
        {
            PageId pid = new HeapPageId(this.getId(), i);
            hpage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);

            if (hpage.getNumEmptySlots() > 0)
                break;
        }

        if (hpage != null & hpage.getNumEmptySlots() == 0 )
        {
            HeapPageId hid = new HeapPageId(this.getId(), this.numPages());
            hpage = new HeapPage(hid, HeapPage.createEmptyPageData());            
        }

        hpage.insertTuple(t);
        ret.add(hpage);
        hpage.markDirty(true, tid);
        
        this.writePage(hpage);

        return ret;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> ret = new ArrayList<Page>();

        RecordId rid = t.getRecordId();
        HeapPage hpage = (HeapPage) Database.getBufferPool().getPage(tid, rid.getPageId(), Permissions.READ_WRITE);
        hpage.deleteTuple(t);

        hpage.markDirty(true, tid);
        ret.add(hpage);

        return ret;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }


}