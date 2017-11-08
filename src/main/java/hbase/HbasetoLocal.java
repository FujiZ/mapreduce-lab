package hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class HbasetoLocal {
    public static void main(String[] args){
        File file = new File(args[0]);
        try {
            FileOutputStream fo = new FileOutputStream(file);
            HbaseTest hbase = new HbaseTest();
            Scan scanner = new Scan();
            ResultScanner rs = hbase.getScanner(scanner);
            try {
                for (Result res : rs) {
                    fo.write(res.getRow());
                    fo.write(' ');
                    fo.write(res.getValue("avgcount".getBytes(),"avgcount".getBytes()));
                    fo.write('\n');
                }
            }catch (IOException e)
            {
                System.out.println("cannot wirte");
            }
        }catch (FileNotFoundException fe)
        {
            System.out.println("File not found!");
        }
    }
}
