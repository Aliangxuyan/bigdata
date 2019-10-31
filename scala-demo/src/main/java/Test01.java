/**
 * @author lxy
 * @date 2019-06-16
 */
public class Test01 {
    public static void main(String[] args) throws InterruptedException {
        Thread t1 = new Thread();
        Thread t2 = new Thread();

        t1.sleep(1000); //正在运行的线程（main 线程）休眠
        t2.wait();              // t2 线程等待
    }
}
