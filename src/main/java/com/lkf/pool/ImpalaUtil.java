package com.lkf.pool;

import com.google.common.collect.Lists;

import java.lang.reflect.Field;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * todo 一句话描述该类的用途
 *
 * @author 刘凯峰
 * @date 2018-12-25 17-45
 */
public class ImpalaUtil {
    /**
     * sql values 模板
     */
    private static final String SQL_MERGE_VALUES_TMPL = "(%s, '%s', '%s', %s, %s, '%s')";

    /**
     * 门店与openid查询语句
     */
    private static final String SQL_SELECT_WX_OPEN_ID = "SELECT storeid,openid,totalprice,ordernum FROM wx.wx_store_openid";
    /**
     * 将门店与openid信息批量添加到ignite缓存的sql语句
     */
    private static final String SQL_MERGE_WX_OPEN_ID = "MERGE INTO WX_STORE_OPENID (ID, STOREID, OPENID, TOTALPRICE, ORDERNUM, CREATE_TIME) VALUES %s";
    /**
     * 门店与openid信息对象全名
     */
    private static final String WX_STORE_OPENID_BEAN = "com.lkf.pool.WxStoreOpenId";
    /**
     * id 自增对象
     */
    private static AtomicInteger atomicLong = new AtomicInteger(1);

    private static String impalaDriver = "cn.com.tcsl.ds.connector.jdbc.impala.HiveDriver";
    private static String impalaUrl = "jdbc:hive2://192.168.12.203:21050/;auth=noSasl";
    private static String igniteDriver = "org.apache.ignite.IgniteJdbcDriver";
    private static String igniteUrl = "jdbc:ignite:thin://192.168.12.203,192.168.12.204";

    public static void main( String[] args ) throws SQLException, ClassNotFoundException {
        try {
            System.out.println("数据加载开始");
            String sql = loadWxFromImpala2Ignite();
            System.out.println("sql：" + sql);
            insert(sql);
        } catch (IllegalAccessException | InstantiationException | NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取连接对象
     *
     * @param driver   连接驱动
     * @param url      连接地址
     * @param username 用户名
     * @param password 密码
     * @return java.sql.Connection
     * @author 刘凯峰
     * @date 2019/6/26 17:37
     */
    public static Connection getConnection( String driver, String url, String username, String password ) throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        return DriverManager.getConnection(url, username, password);
    }

    /**
     * 获取连接对象
     *
     * @param driver 连接驱动
     * @param url    连接地址
     * @return java.sql.Connection
     * @author 刘凯峰
     * @date 2019/6/26 17:37
     */
    public static Connection getConnection( String driver, String url ) throws SQLException, ClassNotFoundException {
        return getConnection(driver, url, "", "");
    }

    public static String loadWxFromImpala2Ignite() throws SQLException, ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchFieldException {
        Connection conn = null;
        try {
            conn = getConnection(impalaDriver, impalaUrl);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

        //准备执行语句对象
        PreparedStatement ps = null;
        try {
            if (conn != null) {
                ps = conn.prepareStatement(SQL_SELECT_WX_OPEN_ID);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //结果集
        ResultSet rs = null;
        try {
            if (ps != null) {
                rs = ps.executeQuery();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //结果集元数据对象
        ResultSetMetaData meta = null;
        try {
            if (rs != null) {
                meta = rs.getMetaData();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //获得结果集元数据
        ResultSetMetaData md = null;
        if (rs != null) {
            md = rs.getMetaData();
        }
        //获得列数
        int columnCount = md.getColumnCount();
        Class<?> clazz = Class.forName(WX_STORE_OPENID_BEAN);
        List<String> sqlValuesList = Lists.newArrayList();
        while (rs.next()) {
            Object obj = clazz.newInstance();
            for (int i = 1; i <= columnCount; i++) {
                Field field = clazz.getDeclaredField(meta.getColumnName(i));
                field.setAccessible(true);
                field.set(obj, rs.getObject(i));
            }
            sqlValuesList.add(generateSqlValues((WxStoreOpenId) obj));
        }
        return generateMergeSql(sqlValuesList);
    }

    private static void insert( String insertSql ) throws SQLException, ClassNotFoundException {
        Connection conn = getConnection(igniteDriver, igniteUrl);
        try (PreparedStatement stmt = conn.prepareStatement(insertSql)) {
            long begin = System.currentTimeMillis();
            stmt.executeUpdate();
            long end = System.currentTimeMillis();
            System.out.println("Populated data：" + (end - begin));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 拼接插入的values值
     *
     * @param wxStoreOpenId 门店与openid关系对象
     * @return java.lang.String
     * @author 刘凯峰
     * @date 2019/6/26 16:23
     */
    private static String generateSqlValues( WxStoreOpenId wxStoreOpenId ) {
        return String.format(SQL_MERGE_VALUES_TMPL, atomicLong.getAndIncrement(), wxStoreOpenId.getStoreid(), wxStoreOpenId.getOpenid(), wxStoreOpenId.getTotalprice(), wxStoreOpenId.getOrdernum(), LocalDateTime.now());
    }

    /**
     * 生产添加数据sql
     *
     * @param valueList 添加值集合
     * @return java.lang.String
     * @author 刘凯峰
     * @date 2019/6/26 16:19
     */
    private static String generateMergeSql( List<String> valueList ) {
        return String.format(SQL_MERGE_WX_OPEN_ID, valueList.stream().collect(Collectors.joining(",")));
    }
}
