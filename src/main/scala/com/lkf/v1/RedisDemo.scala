package com.lkf.v1


object RedisDemo {
  def main(args: Array[String]): Unit = {
    import redis.clients.jedis.Jedis
    val jedis = new Jedis("192.168.12.117", 30078)
    //    jedis.auth("lbredis123")
    //    jedis.del("180828190008000140","180828190008000134","180828190008000137","180828190008000138","180828190008000135","180828190008000136","180828190008000141","180828190008000132","180828190008000139","180828190008000133","180828190632000142","180828190650000143","180828191240000144","180828191333000145","180828191933000146","180828192005000147","180828192656000148","180828192708000149","180828204617000153","181210190306000369","181211095107000370","181211095107000371","181211095107000372","181211095107000373","181211095107000374","181211095108000375","181211095108000376","181211095108000377","181211095108000378","181211095108000379","181211095137000380","181211095138000381","181211095201000382","181211095205000383","181211095232000384","181211095233000385","181211163207000388","181213104509000389")

    //    val str="180828190008000140,180828190008000134,180828190008000137,180828190008000138,180828190008000135,180828190008000136,180828190008000141,180828190008000132,180828190008000139,180828190008000133,180828190632000142,180828190650000143,180828191240000144,180828191333000145,180828191933000146,180828192005000147,180828192656000148,180828192708000149,180828204617000153,181210190306000369,181211095107000370,181211095107000371,181211095107000372,181211095107000373,181211095107000374,181211095108000375,181211095108000376,181211095108000377,181211095108000378,181211095108000379,181211095137000380,181211095138000381,181211095201000382,181211095205000383,181211095232000384,181211095233000385,181211163207000388,181213104509000389"
    //    var result:StringBuilder=new StringBuilder
    //    str.split(",").foreach(s=>result.append(String.format("\"%s\",",s)))
    //    println(result.toString())
    jedis.select(5)
    //    val result = jedis.get("2018120700001")
    //    println(jedis.get("2018120700001"))
//    val value = jedis.get("181224082238000")
    val value="\"{\\\"result\\\":{\\\"msg\\\":\\\"成功\\\",\\\"xaxis\\\":[{\\\"data\\\":[\\\"保定店\\\",\\\"兰州店\\\",\\\"刘一手总店\\\",\\\"北京店\\\",\\\"南开店\\\",\\\"和平店\\\",\\\"天津店\\\",\\\"广西店\\\",\\\"武汉店\\\",\\\"河北店\\\"],\\\"name\\\":\\\"门店名称\\\"}],\\\"total\\\":16,\\\"yaxis\\\":[{\\\"data\\\":[\\\"1\\\",\\\"1\\\",\\\"2\\\",\\\"2\\\",\\\"1\\\",\\\"1\\\",\\\"2\\\",\\\"1\\\",\\\"1\\\",\\\"1\\\"],\\\"name\\\":\\\"应收金额(计数)\\\"},{\\\"data\\\":[\\\"56055300.744000\\\",\\\"5604.567000\\\",\\\"80.022220\\\",\\\"801.200000\\\",\\\"400000000.000000\\\",\\\"100000044\\\",\\\"100.888000\\\",\\\"4030.123000\\\",\\\"4012300.654000\\\",\\\"-50060.000000\\\"],\\\"name\\\":\\\"数值(求和)\\\"}]},\\\"reportCode\\\":\\\"181225134947000640\\\",\\\"info\\\":{\\\"reportId\\\":\\\"181225134947000640\\\",\\\"reportName\\\":\\\"date1\\\",\\\"indexCondition\\\":[{\\\"uniqId\\\":\\\"1545716982000\\\",\\\"fieldId\\\":\\\"181221111320001790\\\"},{\\\"uniqId\\\":\\\"1545974194000\\\",\\\"fieldId\\\":\\\"181228131422003315\\\"}],\\\"reportParam\\\":{\\\"sortCondition\\\":[],\\\"tbName\\\":\\\"riqishaixuantubiaomy_sheet1_000118\\\",\\\"indexCondition\\\":[{\\\"aliasName\\\":\\\"应收金额\\\",\\\"udfType\\\":0,\\\"qoqType\\\":0,\\\"dataType\\\":\\\"double\\\",\\\"aggregator\\\":\\\"count\\\",\\\"uniqId\\\":\\\"1545716982000\\\",\\\"aggregatorName\\\":\\\"计数\\\",\\\"fieldDescription\\\":\\\"应收金额\\\",\\\"fieldGroup\\\":1,\\\"isBuildAggregated\\\":0,\\\"fieldId\\\":\\\"181221111320001790\\\",\\\"numDisplayed\\\":{\\\"state\\\":{\\\"commas\\\":true,\\\"unit\\\":\\\"\\\",\\\"dec\\\":\\\"2\\\"},\\\"type\\\":\\\"dec\\\"}},{\\\"aliasName\\\":\\\"数值\\\",\\\"qoqType\\\":0,\\\"dataType\\\":\\\"int\\\",\\\"aggregator\\\":\\\"sum\\\",\\\"fieldDescription\\\":\\\"数值\\\",\\\"fieldGroup\\\":5,\\\"isBuildAggregated\\\":2,\\\"numDisplayed\\\":{\\\"state\\\":{\\\"commas\\\":true,\\\"unit\\\":\\\"\\\",\\\"dec\\\":\\\"2\\\"},\\\"type\\\":\\\"dec\\\"},\\\"udfType\\\":0,\\\"fieldFormula\\\":\\\"CAST(  di11lie AS DOUBLE)\\\",\\\"uniqId\\\":\\\"1545974194000\\\",\\\"aggregatorName\\\":\\\"求和\\\",\\\"fieldId\\\":\\\"181228131422003315\\\"}],\\\"longitudeAndLatitude\\\":[],\\\"dbName\\\":\\\"impala::e000118\\\",\\\"queryPoint\\\":1,\\\"sessionGroup\\\":\\\"group_report\\\",\\\"queryType\\\":0,\\\"filterCondition\\\":[],\\\"limit\\\":10,\\\"dimensionCondition\\\":[{\\\"aliasName\\\":\\\"门店名称\\\",\\\"udfType\\\":0,\\\"dataType\\\":\\\"str\\\",\\\"uniqId\\\":\\\"1545717269000\\\",\\\"fieldDescription\\\":\\\"门店名称\\\",\\\"fieldGroup\\\":2,\\\"isBuildAggregated\\\":0,\\\"fieldId\\\":\\\"181221111320001787\\\"}],\\\"synSubmit\\\":true,\\\"page\\\":0,\\\"compareCondition\\\":[],\\\"enterTime\\\":1545716987864,\\\"dataSourceType\\\":0,\\\"tbId\\\":\\\"181221111320000154\\\"},\\\"reportTypeCode\\\":\\\"graph_crosstab\\\",\\\"sort\\\":[],\\\"reportConf\\\":[{\\\"isNeedQuery\\\":true,\\\"type\\\":\\\"chartDisplay\\\",\\\"title\\\":\\\"图表显示\\\",\\\"value\\\":{\\\"chartDataNum\\\":10,\\\"partSelect\\\":2,\\\"aroundSelect\\\":1}},{\\\"keyOfSubtotalColumnUniqId\\\":[],\\\"totalColumn\\\":false,\\\"mergeTheLineKey\\\":true,\\\"keyOfSubtotalLine\\\":[],\\\"isNeedQuery\\\":false,\\\"totalLine\\\":false,\\\"type\\\":\\\"tableTotal\\\",\\\"title\\\":\\\"\\\",\\\"keyOfSubtotalColumn\\\":[],\\\"value\\\":{\\\"sortType\\\":1,\\\"sortFlag\\\":\\\"\\\",\\\"fieldId\\\":\\\"\\\"}}],\\\"drillThroughPath\\\":[],\\\"filterCondition\\\":[],\\\"menuCode\\\":\\\"181225134356000119\\\",\\\"reportCode\\\":\\\"181225134947000640\\\",\\\"dimensionCondition\\\":[{\\\"uniqId\\\":\\\"1545717269000\\\",\\\"fieldId\\\":\\\"181221111320001787\\\"}],\\\"reportStyle\\\":{\\\"w\\\":2,\\\"h\\\":2,\\\"x\\\":0,\\\"i\\\":5,\\\"y\\\":4},\\\"compareCondition\\\":[]}}\""
    var key = ""
    for (a <- 1 to 100) {
      if (a < 10) {
        key = "201812070000".concat(a.toString)
      }
      if (a >= 10 && a < 100) {
        key = "20181207000".concat(a.toString)
      }
      if (a >= 100) {
        key = "2018120700".concat(a.toString)
      }

      if (key.nonEmpty) {
        jedis.set(key.concat("_").concat("181128130728000232"), value)
        println(key)
      }
    }
  }
}
