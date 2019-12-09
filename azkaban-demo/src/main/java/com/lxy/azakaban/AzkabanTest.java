package com.lxy.azakaban;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author lxy
 * @date 2019-12-04
 */
public class AzkabanTest {
    public void run() {
        FileOutputStream fos = null;
// 根据需求编写具体代码
        try {
            fos = new
                    FileOutputStream("/Users/lxy/Documents/Idea_workspace/bigdata/azkaban-demo/output.txt");
            fos.write("this is a java progress".getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fos != null){
                try {
                    fos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        AzkabanTest azkabanTest = new AzkabanTest();
        azkabanTest.run();
    }
}
