/**
 * Created by pc on 2016/6/27.
 */
public class TestCommon {
    public static void main(String[] args) {
        String str = "aA汉";
        byte[] bytes = str.getBytes();

        char c = '汉';
        System.out.println(c);
        byte b = 34;
        char d = (char)b;
        System.out.println(d);
        //System.out.printf(String.format("%.1f%%",22.465432));
    }
}
