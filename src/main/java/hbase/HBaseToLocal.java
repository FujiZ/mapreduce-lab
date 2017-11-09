package hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.FileOutputStream;
import java.io.IOException;


public class HBaseToLocal {
    public static void main(String[] args) {
        try {
            FileOutputStream fo = new FileOutputStream(args[0]);
            HBaseClient client = new HBaseClient("Wuxia");
            ResultScanner rs = client.getScanner();
            for (Result res : rs) {
                fo.write(res.getRow());
                fo.write('\t');
                fo.write(res.getValue("avgcount".getBytes(), "avgcount".getBytes()));
                fo.write('\n');
            }
            client.close();
            fo.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
