package DemoUtils.hbaseUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

public class HBaseSink extends RichSinkFunction<List<Put>> {
    private static Logger log = Logger.getLogger(HBaseSink.class);
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Admin admin = HBaseConnPool.getInstance().getConnection().getAdmin();
        if (!admin.tableExists(TableName.valueOf("lx_hbase:tableName"))) {
            log.info("create hbase table: tableName");
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("lx_hbase:tableName"));
            tableDescriptor.addFamily(new HColumnDescriptor("cf"));
            admin.createTable(tableDescriptor);
        }
        admin.close();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(List<Put> putList, Context context) throws Exception{
        try (Table table = HBaseConnPool.getInstance().getConnection().getTable(TableName.valueOf("lx_hbase:tableName"))) {
            table.put(putList);
        } catch (IOException e) {
            log.error("put error", e);
        }
    }
}

