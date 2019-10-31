import com.atguigu.hbase.HbaseAPI;
import org.junit.Test;

import java.io.IOException;

/**
 * @author lxy
 * @date 2019-05-29
 */
public class HbaseAPITest {
    @Test
    public void isTableExist() {
        try {
            boolean exist = HbaseAPI.isTableExist("student2");
            System.out.println(exist);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getRow() {
        try {
            HbaseAPI.getRow("student", "1001");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getRowQualifier(){
        try {
            HbaseAPI.getRowQualifier("student","1001","info","sex");
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
    
    @Test
    public void deleteData(){
        try {
            HbaseAPI.deleteData("student","1001","info","sex");
        } catch (Exception e) {
            e.printStackTrace();
        }

        
    }
}
