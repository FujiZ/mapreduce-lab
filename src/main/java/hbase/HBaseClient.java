package hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;


import java.io.IOException;


public class HBaseClient {
    private String tableName;
    private Configuration config;
    private HTable table;

    public HBaseClient(String tableName) throws IOException {
        this.tableName = tableName;
        config = HBaseConfiguration.create();
        config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
        table = new HTable(config, tableName);
    }

    public void putData(String key, String family,
                        String qualifier, String val) throws IOException {
        Put put = new Put(key.getBytes("UTF-8"));
        put.add(family.getBytes("UTF-8"),
                qualifier.getBytes("UTF-8"),
                val.getBytes("UTF-8"));
        table.put(put);
    }

    public void close() throws IOException {
        table.close();
    }

    public ResultScanner getScanner() throws IOException {
        HTable table = new HTable(config, tableName);
        return table.getScanner(new Scan());
    }
}
