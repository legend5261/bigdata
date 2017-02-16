package test;

/**
 * Created by pc on 2016/9/28.
 */
public class TestStatic {
    static {
        System.out.println("first scheduler");
    }

    public static void testPrint() {
        System.out.println("scheduler");
    }
}
