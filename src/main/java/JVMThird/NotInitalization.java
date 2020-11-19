package JVMThird;

/**
 * Created by ligc on 2020/8/17 21:15
 * 被动实用类字段演示二：
 * 通过数组定义来引用类，不会触发此类的初始化
 */
public class NotInitalization {
    public static void main(String[] args) {
        SuperClass[] sca = new SuperClass[10];
    }
}
