/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.livy.client.ext.model.v1;

import lombok.Data;

/**
 * @package: cn.com.tcsl.loongboss.livy.api.model.report
 * @project-name: tcsl-loongboss-parent
 * @description: Livy报表查询入参
 * @author: Created by 刘凯峰
 * @create-datetime: 2018-03-27 14-22
 */
@Data
public class LivyReportInDTO {
    /**
     * 报表编号
     */
    private String reportCode;
    /**
     * 用户令牌
     */
    private String acessToken;
    /**
     * SQL语句
     */
    private String sqlStatement;


}
