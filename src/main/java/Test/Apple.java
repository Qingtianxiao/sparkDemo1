package Test;

/**
 * Created by ligc on 2020/6/23 20:47
 */
public class Apple<T> {
    //使用T类型定义变量
    private T info;
    //定义构造器的时候不要加菱形语法
    public Apple(){

    }
    public Apple(T info){
        this.info = info;
    }

    public T getInfo() {
        return info;
    }

    public void setInfo(T info) {
        this.info = info;
    }

    public static void main(String[] args) {
        //使用构造器的时候要加菱形语法
        Apple<String> a1 = new Apple<>("苹果");
        System.out.println(a1.getInfo());
        Apple<Double> a2 = new Apple<>(5.45);
        System.out.println(a2.getInfo());
    }
}
