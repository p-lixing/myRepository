package DemoUtils.kafka2MysqlUtils;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class DruidConnectionPool {

    private transient static DataSource dataSource = null;

    private transient static Properties props = new Properties();

    static {

        props.put("driverClassName", "com.mysql.cj.jdbc.Driver");
//        props.put("url", "jdbc:mysql://172.16.200.101:3306/bigdata?characterEncoding=UTF-8");
        props.put("url", "jdbc:mysql://172.16.122.48:3306/lx_test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true&allowPublicKeyRetrieval=true");
        props.put("username", "root");
        props.put("password", "portal@Tbds.com");
        try {
            dataSource = DruidDataSourceFactory.createDataSource(props);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private DruidConnectionPool() {
    }

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }
}
