package LRU;

import java.util.HashMap;

/**
 * Created by ligc on 2020/6/21 18:08
 * 定义一个LRU类，定义它的大小、容量、头结点、尾节点等部分，然后定义一个基本的构造方法
 */
public class LRU<K, V> {
    private int currentSize;//当前的大小
    private int capacity;//总容量
    private HashMap<K, Node> caches; //所有的node节点
    private Node head;
    private Node tail;

    public LRU(int size){
        currentSize = 0;
        this.capacity = size;
        caches = new HashMap<K, Node>(size);
    }

    //添加元素
    //首先判断是不是新的元素
    //如果是新的元素，判断当前的大小是不是大于总容量了，如果大于直接抛弃最后一个节点，然后再以新传入的key/value值创建新的节点
    //如果是已经存在的元素，直接覆盖旧值，再将该元素移动到头部，然后保存在map中
    public void put(K key, V value){
        Node node = caches.get(key);
        if(node == null){
            //如果超出元素容量
            if(caches.size() == this.capacity){
                caches.remove(tail.key);
                removeLast();
            }
            //创建新节点
            node = new Node(key, value);
            caches.put(key, node);
            currentSize ++;
        }else{
            node.value = value;
        }
        //把元素移到首部
        moveToHead(node);
    }

    private void moveToHead(Node node) {

    }

    public void removeLast(){
        if(tail != null){
            tail = tail.pre;
            if(tail == null){
                head = null;
            }else{
                tail.next = null;
            }
        }
    }
}
