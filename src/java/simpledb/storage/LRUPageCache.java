package simpledb.storage;

import simpledb.common.DbException;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LRUPageCache<K> {
    private class DLinkNode {
        private final K key;
        private Page val;
        private DLinkNode prev;
        private DLinkNode next;

        public DLinkNode(K key, Page val) {
            this.key = key;
            this.val = val;
        }

        public String toString() {
            return "DLinkNode{" + key + "," + val + "}";
        }
    }

    Map<K, DLinkNode> map;
    DLinkNode head;
    DLinkNode tail;
    int capacity;
    int size;

    public LRUPageCache(int capacity) {
        map = new ConcurrentHashMap<>();
        head = new DLinkNode(null, null);
        tail = new DLinkNode(null, null);
        head.next = tail;
        tail.prev = head;
        this.capacity = capacity;
        this.size = 0;
    }

    public int size() {
        return size;
    }

    public boolean contains(K key) {
        return map.containsKey(key);
    }

    public Set<K> keySet() {
        return map.keySet();
    }

    public synchronized Page get(K key) {
        Page res = null;
        if (map.containsKey(key)) {
            DLinkNode node = map.get(key);
            moveToHead(node);
            res = node.val;
        }
        return res;
    }

    public synchronized void put(K key, Page value) throws DbException {
        if (map.containsKey(key)) {
            map.get(key).val = value;
            moveToHead(map.get(key));
        } else {
            if (size == capacity) {
                evictCleanPage();
            }

            DLinkNode node = new DLinkNode(key, value);
            addToHead(node);
            map.put(key, node);
            size++;
        }
    }

    // remove the last node
    public synchronized Map.Entry<K, Page> evictCleanPage() throws DbException {
        if (tail.prev == head) {
            return null;
        }
        DLinkNode curr = tail;
        while (curr != head) {
            if (curr.val != null && curr.val.isDirty() == null) {
                return discard(curr.key);
            }
            curr = curr.prev;
        }
        throw new DbException("No clean page for eviction");
    }

    public synchronized Map.Entry<K, Page> discard(K key) {
        if (!map.containsKey(key)) {
            return null;
        }
        DLinkNode node = map.get(key);
        removeNode(node);
        map.remove(key);
        size--;
        return new AbstractMap.SimpleEntry<>(node.key, node.val);
    }

    private void moveToHead(DLinkNode node) {
        removeNode(node);
        addToHead(node);
    }

    private void addToHead(DLinkNode node) {
        node.next = head.next;
        node.prev = head;
        head.next.prev = node;
        head.next = node;
    }

    private void removeNode(DLinkNode node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }
}
