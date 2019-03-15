package com.spark;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @package: com.spark
 * @project-name: spark-learning
 * @description: todo 一句话描述该类的用途
 * @author: Created by 刘凯峰
 * @create-datetime: 2018-05-14 17-28
 */
public class TestDemo {
    private static final int COUNT_BITS = Integer.SIZE - 3;
    private static final int CAPACITY = (1 << COUNT_BITS) - 1;

    // runState is stored in the high-order bits
    private static final int RUNNING = -1 << COUNT_BITS;
    private static final int SHUTDOWN = 0 << COUNT_BITS;
    private static final int STOP = 1 << COUNT_BITS;
    private static final int TIDYING = 2 << COUNT_BITS;
    private static final int TERMINATED = 3 << COUNT_BITS;

    // Packing and unpacking ctl
    private static int runStateOf( int c ) {
        return c & ~CAPACITY;
    }

    private static int workerCountOf( int c ) {
        return c & CAPACITY;
    }

    private static int ctlOf( int rs, int wc ) {
        return rs | wc;
    }

    public static void main( String[] args ) {

        System.out.println(9^3);
        System.out.println(9^4);
        System.out.println(9^0);
//        System.out.println("COUNT_BITS = Integer.SIZE - 3值:" + COUNT_BITS);
//        System.out.println("CAPACITY = (1 << COUNT_BITS) - 1值:" + CAPACITY);
//        System.out.println("RUNNING = -1 << COUNT_BITS:" + RUNNING);
//        System.out.println("SHUTDOWN = 0 << COUNT_BITS值 :" + SHUTDOWN);
//        System.out.println("STOP = 1 << COUNT_BITS值 :" + STOP);
//        System.out.println("TIDYING = 2 << COUNT_BITS值 :" + TIDYING);
//        System.out.println("TERMINATED = 3 << COUNT_BITS :" + TERMINATED);

//        String key="MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCZ5awIIe+3e1y1Q5l1MZUT5HcawcbO9XcenEcwDKCp0K6bEyTks0bUxKYKvyxCTld++yaTywtZ8zRr9ICph0yQDiNya9dIzQuuDSvOF4gWvJLqfGba48+ODebxTkM5TfrBfDyj1NfH1xc/GFzYV5+wETSfLPLFfzj/Ff8n16nR/QIDAQAB";
//
//        String data="XH1mt+A4LGc0LmOlxnOVdT5zTBFSBOtGGi48s4bmvCc0DtAkOj6x2taDxIQeQIZGcY4pVH7Pp01Gi6pIS8Wiyfo+NT5wuWxgk5WpNR7kzehdxJb5CKtaQaUaIbThpN8Lre+za04l8QV1Cjw/DXMGkzlD+YIc4hrJsXG3zHkReHo=";
//        try {
//            Map<String, Object> dataMap;
//            data = RSAUtils.decryptByPublicKey(data, key);
//            dataMap = (Map) JsonUtils.getInstance().getDTO(data, Map.class);
//            System.out.println(dataMap);
//        } catch (NoSuchAlgorithmException e) {
//            e.printStackTrace();
//        } catch (InvalidKeySpecException e) {
//            e.printStackTrace();
//        } catch (NoSuchPaddingException e) {
//            e.printStackTrace();
//        } catch (InvalidKeyException e) {
//            e.printStackTrace();
//        } catch (IllegalBlockSizeException e) {
//            e.printStackTrace();
//        } catch (BadPaddingException e) {
//            e.printStackTrace();
//        } catch (UnsupportedEncodingException e) {
//            e.printStackTrace();
//        }

    }
}
