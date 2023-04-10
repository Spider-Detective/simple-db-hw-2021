package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

import static simpledb.execution.Aggregator.NO_GROUPING;

public class AggregateIter implements OpIterator {

    private Map<Field, List<Field>> groupMap;
    private int gbfield;
    private Aggregator.Op op;
    private TupleDesc tupleDesc;
    private List<Tuple> resultList;
    private Iterator<Tuple> tupleIter;

    public AggregateIter(Map<Field, List<Field>> groupMap, int gbfield, Type gbfieldtype, Aggregator.Op op) {
        this.groupMap = groupMap;
        this.gbfield = gbfield;
        this.op = op;
        if (gbfield == NO_GROUPING) {
            this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            this.tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.resultList = new ArrayList<>();
        switch (op) {
            case COUNT:
                for (Field f : groupMap.keySet()) {
                    Tuple tuple = new Tuple(this.tupleDesc);
                    updateTupleField(tuple, f, new IntField(groupMap.get(f).size()));
                    resultList.add(tuple);
                }
                break;
            case MIN:
                for (Field f : groupMap.keySet()) {
                    int min = Integer.MAX_VALUE;
                    Tuple tuple = new Tuple(this.tupleDesc);
                    for (Field value : groupMap.get(f)) {
                        if (value instanceof IntField) {
                            min = Math.min(min, ((IntField) value).getValue());
                        }
                    }
                    updateTupleField(tuple, f, new IntField(min));
                    resultList.add(tuple);
                }
                break;
            case MAX:
                for (Field f : groupMap.keySet()) {
                    int max = Integer.MIN_VALUE;
                    Tuple tuple = new Tuple(this.tupleDesc);
                    for (Field value : groupMap.get(f)) {
                        if (value instanceof IntField) {
                            max = Math.max(max, ((IntField) value).getValue());
                        }
                    }
                    updateTupleField(tuple, f, new IntField(max));
                    resultList.add(tuple);
                }
                break;
            case AVG:
                for (Field f : groupMap.keySet()) {
                    int avg = 0;
                    Tuple tuple = new Tuple(this.tupleDesc);
                    for (Field value : groupMap.get(f)) {
                        if (value instanceof IntField) {
                            avg += ((IntField) value).getValue();
                        }
                    }
                    updateTupleField(tuple, f, new IntField(avg / groupMap.get(f).size()));
                    resultList.add(tuple);
                }
                break;
            case SUM:
                for (Field f : groupMap.keySet()) {
                    int sum = 0;
                    Tuple tuple = new Tuple(this.tupleDesc);
                    for (Field value : groupMap.get(f)) {
                        if (value instanceof IntField) {
                            sum += ((IntField) value).getValue();
                        }
                    }
                    updateTupleField(tuple, f, new IntField(sum));
                    resultList.add(tuple);
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown Op: " + op);
        }
        tupleIter = resultList.iterator();
    }

    private void updateTupleField(Tuple tuple, Field gfield, IntField afield) {
        if (gbfield == NO_GROUPING) {
            tuple.setField(0, afield);
        } else {
            tuple.setField(0, gfield);
            tuple.setField(1, afield);
        }
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return tupleIter.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        return tupleIter.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    @Override
    public void close() {
        tupleIter = null;
    }
}
