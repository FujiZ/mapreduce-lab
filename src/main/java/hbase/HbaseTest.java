package hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.util.Scanner;


public class HbaseTest {
    private static Configuration conf = null;
    private static String tableName = "Wuxia";
    static {
        conf = HBaseConfiguration.create();
        //conf.set("hbase.zookeeper.quorum","node1");
        //conf.set("hbase.zookeeper.property.client","2181");
    }

    public void addData(String Name,int avgcount)
    {
        try{
            HTable table = new HTable(conf,tableName);
            Put put = new Put(Bytes.toBytes(Name));
            put.add(Bytes.toBytes("avgcount"),Bytes.toBytes("avgcount"),Bytes.toBytes(avgcount+""));
            table.put(put);
            System.out.println("sucess");
        }
        catch (IOException e) {
            e.printStackTrace();
            System.out.println("error");
        }
    }
    public ResultScanner getScanner(Scan scanner) {

        try {
            HTable table = new HTable(conf, tableName);

            ResultScanner rscanner = table.getScanner(scanner);
            System.out.println("sucess");
            return rscanner;
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("error");
            return  null;
        }
    }
}
