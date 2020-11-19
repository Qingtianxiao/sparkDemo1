package LRU;

/**
 * Created by ligc on 2020/6/21 17:57
 * LRU使用双向链表实现
 * 定义最起初的Node数据结构
 */
public class Node {
    Object key;
    Object value;
    Node pre;
    Node next;

    public Node(Object key, Object value){
        this.key = key;
        this.value = value;
    }
}
