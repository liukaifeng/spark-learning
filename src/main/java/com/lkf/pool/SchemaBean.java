package com.lkf.pool;

import lombok.Data;

import java.util.List;

/**
 * 结果集元数据结构
 *
 * @author 刘凯峰
 * @date 2018-12-28 14-02
 */
@Data
public class SchemaBean {
    private List<FieldBean> fields;
}
