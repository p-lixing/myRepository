package DemoUtils.flinkTest;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MysqlSink extends
        RichSinkFunction<Tuple3<Integer, String, Integer>> {

    private Connection connection;
    private PreparedStatement preparedStatement;
    String username = "root";
    String password = "portal@Tbds.com";
    String drivername = "com.mysql.cj.jdbc.Driver"; //配置改成自己的配置
    String dburl = "jdbc:mysql://172.16.122.48:3306/lx_test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true&allowPublicKeyRetrieval=true";

    public void invoke(Tuple3<Integer, String, Integer> value) throws Exception {
        Class.forName(drivername);
        connection = DriverManager.getConnection(dburl, username, password);
        String sql = "replace into flink2mysql(id,num,price) values(?,?,?)"; //假设mysql 有3列 id,num,price
        System.out.println("====values======"+value);
        preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setInt(1, value.f0);
        preparedStatement.setString(2, value.f1);
        preparedStatement.setInt(3, value.f2);
        preparedStatement.executeUpdate();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
