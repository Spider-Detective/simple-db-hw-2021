package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static simpledb.common.Permissions.READ_ONLY;
import static simpledb.common.Permissions.READ_WRITE;

public class LockManager {
    Map<PageId, List<Lock>> lockTable;

    public LockManager() {
        lockTable = new ConcurrentHashMap<>();
    }

    public synchronized boolean lock(TransactionId transactionId, PageId pageId, Permissions permissions) {
        Lock lock = new Lock(transactionId, permissions);
        List<Lock> lockList = lockTable.get(pageId);
        if (lockList == null || lockList.isEmpty()) {
            lockList = new ArrayList<>();
            lockList.add(lock);
            lockTable.put(pageId, lockList);
            return true;
        }

        if (lockList.size() == 1) {
            Lock currLock = lockList.get(0);
            if (currLock.getTransactionId().equals(lock.getTransactionId())) {
                // upgrade lock
                if (currLock.getPermissions().equals(READ_ONLY) && lock.getPermissions().equals(READ_WRITE)) {
                    currLock.setPermissions(READ_WRITE);
                }
                return true;
            } else {
                if (currLock.getPermissions().equals(READ_ONLY) && lock.getPermissions().equals(READ_ONLY)) {
                    lockList.add(lock);
                    return true;
                } else {
                    return false;
                }
            }
        } else {
            // reject exclusive lock
            if (lock.getPermissions().equals(READ_WRITE)) {
                return false;
            }
            // add lock if not exist
            for (Lock l : lockList) {
                if (l.getTransactionId().equals(lock.getTransactionId())) {
                    return true;
                }
            }
            lockList.add(lock);
            return true;
        }
    }

    public synchronized boolean unlock(TransactionId transactionId, PageId pageId) {
        List<Lock> lockList = lockTable.get(pageId);
        for (Lock l : lockList) {
            if (l.getTransactionId().equals(transactionId)) {
                lockList.remove(l);
                if (lockList.isEmpty()) {
                    lockTable.remove(pageId);
                }
                return true;
            }
        }
        return false;
    }

    public synchronized void releaseAllLocks(TransactionId transactionId) {
        for (PageId pageId : lockTable.keySet()) {
            List<Lock> lockList = lockTable.get(pageId);
            Lock toRemove = null;
            for (Lock l : lockList) {
                if (l.getTransactionId().equals(transactionId)) {
                    toRemove = l;
                }
            }
            if (toRemove != null) {
                lockList.remove(toRemove);
                if (lockList.isEmpty()) {
                    lockTable.remove(pageId);
                }
            }
        }
    }

    public synchronized boolean holdsLock(TransactionId transactionId, PageId pageId) {
        for (Lock l : lockTable.get(pageId)) {
            if (l.getTransactionId().equals(transactionId)) {
                return true;
            }
        }
        return false;
    }
}
