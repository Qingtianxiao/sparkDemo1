package JVMThird;

/**
 * 被动类字段使用演示一
 * 通过子类引用父类的静态字段，不会导致子类初始化
 * Created by ligc on 2020/8/17 21:08
 */
public class SuperClass {
    static{
        System.out.println("SuperClass init");
    }

    public static int value = 1;
}
