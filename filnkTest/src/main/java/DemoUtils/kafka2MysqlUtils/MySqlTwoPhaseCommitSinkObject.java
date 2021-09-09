package DemoUtils.kafka2MysqlUtils;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MySqlTwoPhaseCommitSinkObject extends TwoPhaseCommitSinkFunction<ObjectNode, MySqlTwoPhaseCommitSinkObject.ConnectionState, Void> {
    private static final Logger log = LoggerFactory.getLogger(MySqlTwoPhaseCommitSinkObject.class);

    public MySqlTwoPhaseCommitSinkObject() {
        super(new KryoSerializer<>(MySqlTwoPhaseCommitSinkObject.ConnectionState.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    @Override
    protected void invoke(ConnectionState connectionState, ObjectNode objectNode, Context context) throws Exception {
        Connection connection = connectionState.connection;
        log.info("start invoke...");
        String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        log.info("===>date:" + date + " " + objectNode);
        log.info("===>date:{} --{}",date,objectNode);
        String value = objectNode.get("value").toString();
        log.info("objectNode-value:" + value);
        JSONObject valueJson = JSONObject.parseObject(value);
        String value_str = valueJson.getString("value");
        PreparedStatement pstm = connection.prepareStatement("insert into `mysqlExactlyOnce_test` (`value`,`insert_time`) values (?,?)");
        pstm.setString(1,value_str);
        Timestamp value_time = new Timestamp(System.currentTimeMillis());
        pstm.setTimestamp(2,value_time);
        log.info("要插入的数据:{}--{}",value_str,value_time);
        pstm.executeUpdate();
        pstm.close();
    }

    @Override
    protected MySqlTwoPhaseCommitSinkObject.ConnectionState beginTransaction() throws Exception {

        System.out.println("=====> beginTransaction... ");
        //Class.forName("com.mysql.jdbc.Driver");
        //Connection conn = DriverManager.getConnection("jdbc:mysql://172.16.200.101:3306/bigdata?characterEncoding=UTF-8", "root", "123456");
        Connection connection = DruidConnectionPool.getConnection();
        connection.setAutoCommit(false);
        return new ConnectionState(connection);

    }


    @Override
    protected void preCommit(MySqlTwoPhaseCommitSinkObject.ConnectionState connectionState) throws Exception {
        System.out.println("=====> preCommit... " + connectionState);
    }

    @Override
    protected void commit(MySqlTwoPhaseCommitSinkObject.ConnectionState connectionState) {
        System.out.println("=====> commit... ");
        Connection connection = connectionState.connection;
        try {
            connection.commit();
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException("提交事物异常");
        }
    }

    @Override
    protected void abort(MySqlTwoPhaseCommitSinkObject.ConnectionState connectionState) {
        System.out.println("=====> abort... ");
        Connection connection = connectionState.connection;
        try {
            connection.rollback();
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException("回滚事物异常");
        }
    }

    static class ConnectionState {

        private final transient Connection connection;

        ConnectionState(Connection connection) {
            this.connection = connection;
        }

    }


}