package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;

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
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        HeapPage heapPage = null;
        int pageSize = BufferPool.getPageSize();
        byte[] buf = new byte[pageSize];

        // try with resource, close is automatic
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            // set the file pointer
            raf.seek(pid.getPageNumber() * pageSize);
            if (raf.read(buf) == -1) {
                throw new IllegalArgumentException("PageId: " + pid + " doea not exist in the disk.");
            }
            heapPage = new HeapPage((HeapPageId) pid, buf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return heapPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, Permissions.READ_ONLY);
    }

    public class HeapFileIterator extends AbstractDbFileIterator {
        private TransactionId tid;
        private Permissions permissions;
        private int nextPage;
        private Iterator<Tuple> iterator;

        public HeapFileIterator(TransactionId tid, Permissions permissions) {
            this.tid = tid;
            this.permissions = permissions;
            this.nextPage = 0;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            nextPage = 0;
            loadNextPageIterator();
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if (iterator == null) {
                return null;
            }

            if (iterator.hasNext()) {
                return iterator.next();
            } else {
                while (nextPage < numPages()) {
                    loadNextPageIterator();
                    if (iterator.hasNext()) {
                        return iterator.next();
                    }
                }
            }

            return null;
        }

        private void loadNextPageIterator() throws TransactionAbortedException, DbException {
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), nextPage), permissions);
            this.iterator = heapPage.iterator();
            nextPage++;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            super.close();
            iterator = null;
        }
    }

}

