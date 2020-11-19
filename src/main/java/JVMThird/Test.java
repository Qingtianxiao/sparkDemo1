package JVMThird;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ligc on 2020/8/17 21:10
 */
public class Test {
    public static void main(String[] args) {
//        System.out.println(SubClass.value);
        int[] a = new int[]{3,2,4};
        HashMap<Integer, Integer> hashMap = new HashMap<>(10);
        int[] ret = twoSum(a, 6);
        System.out.println(ret[0] + "," +  ret[1]);
    }


    public static int[] twoSum(int[] nums, int target) {
        int length = nums.length;
        int[] ret = new int[2];
        Map<Integer, Integer> hashMap = new HashMap<>(length);
        for(int i = 0; i < length; i ++){
            hashMap.put(nums[i], i);
        }

        for(int i = 0; i < length; i ++){
            if(hashMap.containsKey(nums[i])){
                if(hashMap.containsKey(target - nums[i])){
                    ret[0] = hashMap.get(nums[i]);
                    ret[1] = hashMap.get(target - nums[i]);
                    break;
                }
            }
        }

        return ret;
    }
}
