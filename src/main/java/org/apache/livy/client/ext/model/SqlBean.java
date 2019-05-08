package org.apache.livy.client.ext.model;

import lombok.Data;

import java.util.List;

/**
 * todo 一句话描述该类的用途
 *
 * @author 刘凯峰
 * @date 2019-03-11 15-19
 */
@Data
public class SqlBean {
    /**
     * 查询项
     */
    private List<SelectOptionDTO> selectList;
    /**
     * 过滤筛选项
     */
    private List<FilterConditionBean> filterList;
    /**
     * 数据表全名，包含数据库名
     */
    private String fullTableName;
    /**
     * 最终查询项
     */
    private List<String> selectSqlList;
    /**
     * 同环比子查询SQL，带on条件
     */
    private List<String> selectQoqSqlList;

    /**
     * where 条件
     */
    private List<String> whereSqlList;
    /**
     * 分组条件
     */
    private List<String> groupBySqlList;
}
