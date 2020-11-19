package Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ligc on 2020/6/23 20:51
 */
public class MyApple extends Apple<Integer>{


    public static void main(String[] args) {

        ArrayList  list;
        list = new ArrayList(123);
        list.add(1);
        System.out.println(list.get(0));

    }
}
