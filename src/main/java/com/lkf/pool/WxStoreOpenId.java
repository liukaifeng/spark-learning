package com.lkf.pool;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;

/**
 * todo 一句话描述该类的用途
 *
 * @author 刘凯峰
 * @date 2019-06-26 14-53
 */
@Data
public class WxStoreOpenId {
    private Long id;
    private String storeid;
    private String openid;
    private BigDecimal totalprice;
    private Long ordernum;
    private Date createTime;
}
