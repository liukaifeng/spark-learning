package com.lkf.pool;

import lombok.Data;

import java.util.List;

/**
 * sql 执行结果对象
 *
 * @author 刘凯峰
 * @date 2018-12-28 13-17
 */
@Data
public class SqlExecuteResultSet {
    /**
     * 数据结构
     */
    private SchemaBean schema;
    /**
     * 结果数据
     */
    private Object detail;
    /**
     * 跟踪ID
     */
    private String trcid;
    /**
     * commonid (异步执行会返回这个id)
     */
    private String commonId;
    /**
     * commonid (异步执行会返回这个id)
     */
    private String sessionId;
    /**
     * 总条数
     */
    private Long total;
}
