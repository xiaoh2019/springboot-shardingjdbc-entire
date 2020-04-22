package notspring;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.shardingsphere.api.config.sharding.KeyGeneratorConfiguration;
import org.apache.shardingsphere.api.config.sharding.ShardingRuleConfiguration;
import org.apache.shardingsphere.api.config.sharding.TableRuleConfiguration;
import org.apache.shardingsphere.api.config.sharding.strategy.InlineShardingStrategyConfiguration;
import org.apache.shardingsphere.shardingjdbc.api.ShardingDataSourceFactory;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;

/**
 * @description:
 * @author: xh
 * @create: 2020-04-20 21:12
 */
public class Test1 {
    
    @Test
    public void hello(){


    }

    /**
     * 水平分库
     */
    @Test
    public void test4() throws Exception{

        HashMap<String, DataSource> map = new HashMap<>();
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setUsername("root");
        dataSource.setPassword("123qwe");
        dataSource.setJdbcUrl("jdbc:mysql://localhost:3306/sharding_jdbc_0?useSSL=false&serverTimezone=UTC");
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        map.put("ds0", dataSource);
        HikariDataSource dataSource1 = new HikariDataSource();
        dataSource1.setUsername("root");
        dataSource1.setPassword("123qwe");
        dataSource1.setJdbcUrl("jdbc:mysql://localhost:3306/sharding_jdbc_1?useSSL=false&serverTimezone=UTC");
        dataSource1.setDriverClassName("com.mysql.cj.jdbc.Driver");
        map.put("ds1", dataSource1);
        // 2.配置Order表规则
        TableRuleConfiguration orderTableRuleConfig = new TableRuleConfiguration("t_order","ds${0..1}.t_order0");


        // 3.配置分表策略
        orderTableRuleConfig.setDatabaseShardingStrategyConfig(new InlineShardingStrategyConfiguration("order_id", "ds${order_id % 2}"));
        //orderTableRuleConfig.setTableShardingStrategyConfig(new InlineShardingStrategyConfiguration("order_id", "t_order${order_id % 2}"));
        // 4.配置分片键的生成
        KeyGeneratorConfiguration keyGeneratorConfiguration = new KeyGeneratorConfiguration("SNOWFLAKE","order_id");
        orderTableRuleConfig.setKeyGeneratorConfig(keyGeneratorConfiguration);
        // 5.配置分片规则
        ShardingRuleConfiguration shardingRuleConfig = new ShardingRuleConfiguration();
        shardingRuleConfig.getTableRuleConfigs().add(orderTableRuleConfig);

        // 6.获取数据源对象
        DataSource dataSourceJ = ShardingDataSourceFactory.createDataSource(map, shardingRuleConfig, new Properties());

        Connection connection = dataSourceJ.getConnection();
        String sql = "insert into t_order(price,pro_name,status)value(12, '笔记本', 1)";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        for (int i = 0; i < 20; i++) {
            boolean execute = preparedStatement.execute();
            System.out.println(execute);
        }
    }

    /**
     * 水平分表查询
     */
    @Test
    public void test3()throws Exception{
        HashMap<String, DataSource> map = new HashMap<>();
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setUsername("root");
        dataSource.setPassword("123qwe");
        dataSource.setJdbcUrl("jdbc:mysql://localhost:3306/sharding_jdbc_0?useSSL=false&serverTimezone=UTC");
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        map.put("ds0", dataSource);
        HikariDataSource dataSource1 = new HikariDataSource();
        dataSource1.setUsername("root");
        dataSource1.setPassword("123qwe");
        dataSource1.setJdbcUrl("jdbc:mysql://localhost:3306/sharding_jdbc_0?useSSL=false&serverTimezone=UTC");
        dataSource1.setDriverClassName("com.mysql.cj.jdbc.Driver");
        // map.put("ds1", dataSource1);
        // 2.配置Order表规则
        TableRuleConfiguration orderTableRuleConfig = new TableRuleConfiguration("t_order","ds0.t_order${0..1}");

        // 3.配置分表策略
        //orderTableRuleConfig.setDatabaseShardingStrategyConfig(new InlineShardingStrategyConfiguration("user_id", "ds${user_id % 2}"));
        orderTableRuleConfig.setTableShardingStrategyConfig(new InlineShardingStrategyConfiguration("order_id", "t_order${order_id % 2}"));
        // 4.配置分片键的生成
        KeyGeneratorConfiguration keyGeneratorConfiguration = new KeyGeneratorConfiguration("SNOWFLAKE","order_id");
        orderTableRuleConfig.setKeyGeneratorConfig(keyGeneratorConfiguration);
        // 5.配置分片规则
        ShardingRuleConfiguration shardingRuleConfig = new ShardingRuleConfiguration();
        shardingRuleConfig.getTableRuleConfigs().add(orderTableRuleConfig);

        // 6.获取数据源对象
        DataSource dataSourceJ = ShardingDataSourceFactory.createDataSource(map, shardingRuleConfig, new Properties());

        Connection connection = dataSourceJ.getConnection();
        String sql = "select * from t_order where order_id = 459355832023252993";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()){
            long order_id = resultSet.getLong("order_id");
            String price = resultSet.getString("price");
            String status = resultSet.getString("status");
            String pro_name = resultSet.getString("pro_name");
            System.out.println("----------");
        }
    }

    /**
     * 水平分表
     * @throws SQLException
     */
    @Test
    public void test2() throws SQLException {
        //1.
        HashMap<String, DataSource> map = new HashMap<>();
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setUsername("root");
        dataSource.setPassword("123qwe");
        dataSource.setJdbcUrl("jdbc:mysql://localhost:3306/sharding_jdbc_0?useSSL=false&serverTimezone=UTC");
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        map.put("ds0", dataSource);
        HikariDataSource dataSource1 = new HikariDataSource();
        dataSource1.setUsername("root");
        dataSource1.setPassword("123qwe");
        dataSource1.setJdbcUrl("jdbc:mysql://localhost:3306/sharding_jdbc_0?useSSL=false&serverTimezone=UTC");
        dataSource1.setDriverClassName("com.mysql.cj.jdbc.Driver");
       // map.put("ds1", dataSource1);
        // 2.配置Order表规则
        TableRuleConfiguration orderTableRuleConfig = new TableRuleConfiguration("t_order","ds0.t_order${0..1}");

        // 3.配置分表策略
        //orderTableRuleConfig.setDatabaseShardingStrategyConfig(new InlineShardingStrategyConfiguration("user_id", "ds${user_id % 2}"));
        orderTableRuleConfig.setTableShardingStrategyConfig(new InlineShardingStrategyConfiguration("order_id", "t_order${order_id % 2}"));
        // 4.配置分片键的生成
        KeyGeneratorConfiguration keyGeneratorConfiguration = new KeyGeneratorConfiguration("SNOWFLAKE","order_id");
        orderTableRuleConfig.setKeyGeneratorConfig(keyGeneratorConfiguration);
        // 5.配置分片规则
        ShardingRuleConfiguration shardingRuleConfig = new ShardingRuleConfiguration();
        shardingRuleConfig.getTableRuleConfigs().add(orderTableRuleConfig);

        // 6.获取数据源对象
        DataSource dataSourceJ = ShardingDataSourceFactory.createDataSource(map, shardingRuleConfig, new Properties());

        Connection connection = dataSourceJ.getConnection();
        String sql = "insert into t_order(price,pro_name)value(12, '笔记本')";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        for (int i = 0; i < 20; i++) {
            boolean execute = preparedStatement.execute();
            System.out.println(execute);
        }
    }
}
