package com.lkf.pool;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;

import java.sql.*;
import java.util.List;

/**
 * todo 一句话描述该类的用途
 *
 * @author 刘凯峰
 * @date 2018-12-25 17-45
 */
public class ImpalaUtil {
    public static void main( String[] args ) throws SQLException, ClassNotFoundException {
        select();
    }


    public static Connection getConnection() throws ClassNotFoundException, SQLException {
        String driver = "org.apache.hive.jdbc.HiveDriver";
        String url = "jdbc:impala://192.168.12.204:21050/;auth=noSasl";
        String username = "";
        String password = "";
        Connection conn = null;
        Class.forName(driver);
        conn = (Connection) DriverManager.getConnection(url, username, password);
        return conn;
    }

    public static void select() throws ClassNotFoundException, SQLException {
        Connection conn = getConnection();
//        SELECT * FROM e000.dw_trade_bill_detail_fact_p_group LIMIT 10
        String sql = "DESCRIBE cy7_2.dw_trade_bill_detail_fact_p_group_9759 ";
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        //执行结果返回对象
        SqlExecuteResultSet sqlExecuteResult = new SqlExecuteResultSet();
        SchemaBean schemaBean = new SchemaBean();
        //值集合
        List<List<String>> valueLists = Lists.newArrayList();
        //字段信息
        List<FieldBean> fieldBeans = Lists.newArrayList();
        //列数量
        int col = rs.getMetaData().getColumnCount();
        //从结果集解析列名及数据类型
        for (int i = 1; i <= col; i++) {
            FieldBean fieldBean = new FieldBean();
            fieldBean.setName(rs.getMetaData().getColumnName(i));
            fieldBean.setType(rs.getMetaData().getColumnTypeName(i));
            fieldBeans.add(fieldBean);
        }
        //遍历获取每一条数据
        while (rs.next()) {
            List<String> list = Lists.newArrayList();
            //获取每一列的值
            for (int i = 1; i <= col; i++) {
                list.add(rs.getString(i));
            }
            valueLists.add(list);
        }
        sqlExecuteResult.setDetail(valueLists);
        schemaBean.setFields(fieldBeans);
        sqlExecuteResult.setSchema(schemaBean);
        System.out.println(JSONObject.toJSONString(sqlExecuteResult));
    }
}
