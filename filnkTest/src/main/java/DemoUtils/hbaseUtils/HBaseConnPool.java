package DemoUtils.hbaseUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

public class HBaseConnPool implements Serializable {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(HBaseConnPool.class);
    private static Configuration conf = null;

    //HBase中Connection已经实现了对连接的管理功能,是线程安全的，一个JVM中共用一个Connection对象即可，
    // 而Table和Admin则不是线程安全的，不同的线程中使用单独的Table和Admin对象。
    private static Connection conn = null;

    private static final HBaseConnPool instance = new HBaseConnPool();

    private HBaseConnPool() {
        log.info("init HBaseConnPool");
    }

    /**
     * 获取pool实例
     *
     * @return HBaseConnPool
     */
    public static HBaseConnPool getInstance() {
        log.info("execute getInstance");
        if (instance == null) {
            log.error("getInstance error, instance is null.");
        }
        return instance;
    }

    public synchronized Connection getConnection() throws Exception {
        log.info("get connection");
        if (conn == null || conn.isClosed()) {
            log.info("recreate HBase connection");
            closeConnectionPool();
            createConnectionPool();
        }
        if (conn == null) {
            log.error("getConnection error, conn is null");
        }
        return conn;
    }

    private void createConnectionPool() throws IOException {
        log.info("create connection pool");
        loadConfiguration();
        conn = ConnectionFactory.createConnection(conf);
    }

    private void closeConnectionPool() throws IOException {
        log.info("close connection pool");
        if (conn == null) {
            log.info("conn is null, end close");
            return;
        }
        conn.close();
        log.info("end close");
    }

    /**
     * 加载配置文件
     */
    private void loadConfiguration() {

        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "172.16.122.54");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.security.authentication.tbds.secureid", "dPRH5zhmsETx2Qpl7YMWIkQ8mgwXY9mTMtLw");
        conf.set("hbase.security.authentication.tbds.securekey", "GvGv1VJ3QdqKUUJGw1Ghxeq8gMltafNI");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");

        //获取路径:classes/
//        String filePath = this.getClass().getClassLoader().getResource("").getPath();
//        String hbaseFilePath = filePath + "hbase" + File.separator;
        //读取hbase客户端配置文件
//        conf.addResource(new Path(hbaseFilePath + "core-site.xml"));
//        conf.addResource(new Path(hbaseFilePath + "hbase-site.xml"));
    }

    public static void main(String[] args) throws Exception {
        Admin admin = HBaseConnPool.getInstance().getConnection().getAdmin();
        BufferedMutatorParams params;
        BufferedMutator mutator = null;
        String table = "lx_hbase:tableName";
        TableName tableName = TableName.valueOf(table);
        // 设置缓存
        params = new BufferedMutatorParams(tableName);
        params.writeBufferSize(1024);
        mutator = conn.getBufferedMutator(params);
        String value = "101,sad,23";
        String[] array = value.split(",");
        Put put= new Put(Bytes.toBytes(array[0]));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes(array[1]));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(array[2]));
        mutator.mutate(put);
        mutator.flush();
        mutator.close();
//        if (!admin.tableExists(TableName.valueOf("lx_hbase:tableName"))) {
//            log.info("create hbase table: tableName");
//            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("lx_hbase:tableName"));
//            tableDescriptor.addFamily(new HColumnDescriptor("cf"));
//            admin.createTable(tableDescriptor);
//        }else {
//            log.info("hbase table: tableName is exists");
//        }
        admin.close();
    }

}
