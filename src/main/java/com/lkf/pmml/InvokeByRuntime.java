package com.lkf.pmml;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * todo 一句话描述该类的用途
 *
 * @author 刘凯峰
 * @date 2019-07-24 09-07
 */
public class InvokeByRuntime {
    /**
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main( String[] args ) throws IOException, InterruptedException {
        String exe = "python";
        String command = "G:\\workspace\\python3\\hello.py";
        String num1 = "1";
        String num2 = "2";
        String[] cmdArr = new String[]{exe, command, num1, num2};
        Process process = Runtime.getRuntime().exec(cmdArr);
        InputStream is = process.getInputStream();
        DataInputStream dis = new DataInputStream(is);
        String str = dis.readLine();
        process.waitFor();
        System.out.println(str);
    }
}
