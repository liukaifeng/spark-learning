package org.apache.livy.client.ext.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * 查询项实体对象
 *
 * @author 刘凯峰
 * @date 2019-01-21 10-13
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class SelectOptionDTO extends BaseConditionBean {
    /**
     * 聚合代码，eg：SUM
     */
    private String aggregator;
    /**
     * 同环比标识
     */
    private Boolean qoqFlag;

    /**
     * 查询类型
     * 0-默认值（普通查询）
     * 1-筛选项数据查询
     */
    private Integer queryType;

    public SelectOptionDTO() {
        this.qoqFlag = false;
    }
}
