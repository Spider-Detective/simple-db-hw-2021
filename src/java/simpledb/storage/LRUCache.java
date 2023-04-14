package simpledb.storage;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LRUCache<K, V> {
    private class DLinkNode {
        private final K key;
        private V val;
        private DLinkNode prev;
        private DLinkNode next;

        public DLinkNode(K key, V val) {
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

    public LRUCache(int capacity) {
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

    public synchronized V get(K key) {
        V res = null;
        if (map.containsKey(key)) {
            DLinkNode node = map.get(key);
            moveToHead(node);
            res = node.val;
        }
        return res;
    }

    public synchronized void put(K key, V value) {
        if (map.containsKey(key)) {
            map.get(key).val = value;
            moveToHead(map.get(key));
        } else {
            if (size == capacity) {
                evict();
            }

            DLinkNode node = new DLinkNode(key, value);
            addToHead(node);
            map.put(key, node);
            size++;
        }
    }

    // remove the last node
    public synchronized Map.Entry<K, V> evict() {
        if (tail.prev == head) {
            return null;
        }
        return discard(tail.prev.key);
    }

    public synchronized Map.Entry<K, V> discard(K key) {
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
