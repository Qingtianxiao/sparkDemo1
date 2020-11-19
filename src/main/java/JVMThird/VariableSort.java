package JVMThird;

/**
 * Created by ligc on 2020/9/2 10:50
 * 变量槽复用带来的负面影响
 * 影响系统的GC
 */
public class VariableSort {
    public static void main(String[] args) {
        {
            byte[] placeholder = new byte[64 * 1024 * 1024];

        }
//        int a = 0;
        System.gc();
    }
}
