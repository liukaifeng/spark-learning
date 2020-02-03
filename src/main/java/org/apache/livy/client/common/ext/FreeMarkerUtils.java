package org.apache.livy.client.common.ext;

import com.google.common.collect.Maps;
import freemarker.cache.NullCacheStorage;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Map;

/**
 * freemarker 模板工具类
 *
 * @author 刘凯峰
 * @date 2019-07-16 16-34
 */
public class FreeMarkerUtils {
    private static Logger logger = LoggerFactory.getLogger(FreeMarkerUtils.class.getName());

    private FreeMarkerUtils() {
    }

    private static final Configuration CONFIGURATION = new Configuration(Configuration.VERSION_2_3_28);

    static {
        //本地模版加载
//        ClassTemplateLoader classTemplateLoader = new ClassTemplateLoader(FreeMarkerUtils.class, "/templates");
//        CONFIGURATION.setTemplateLoader(classTemplateLoader);
        CONFIGURATION.setDefaultEncoding("UTF-8");
        CONFIGURATION.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
        CONFIGURATION.setCacheStorage(NullCacheStorage.INSTANCE);
    }

    /**
     * 获取模板对象
     *
     * @param templateName 模板名称
     * @return freemarker.template.Template
     * @author 刘凯峰
     * @date 2019/7/17 9:59
     */
    private static Template getTemplate(String templateName) throws IOException {
        return CONFIGURATION.getTemplate(templateName);
    }

    /**
     * 解析ftl文件内容
     *
     * @param content 原ftl模板文件内容
     * @param model   模板文件数据
     * @return 解析后的html文件内容
     */
    public static String parseFtlContent(String content, Map<String, Object> model) {
        // 获取配置
        StringWriter out = new StringWriter();
        try {
            new Template("template", new StringReader(content), CONFIGURATION).process(model, out);
        } catch (TemplateException | IOException e) {
            logger.error("parseFtlContent", e);
            return "";
        }
        content = out.toString();
        try {
            out.close();
        } catch (IOException e) {
            logger.error("parseFtlContent", e);
            return "";
        }
        return content;
    }

    public static void main(String[] args) {
        String ftl = "{'city': '${city}', 'group_code': '${group_code}', 'store_name': '${store_name}', 'history_date': '${history_date}','predict_days':'${predict_days}'}";
        Map<String, Object> tmpl = Maps.newHashMap();
        tmpl.put("city", "天津");
        tmpl.put("group_code", "3297");
        tmpl.put("store_name", "九田家天津武清区体育中心店,九田家天津和平区吉利商厦店,九田家天津红桥区欧亚达,九田家天津南开区熙悦汇店,九田家天津武清区保利金街店,九田家天津滨海新区八角楼店");
        tmpl.put("history_date", "2018-07-01,2018-12-10");
        tmpl.put("predict_days", "15");
        System.out.println(parseFtlContent(ftl, tmpl));
    }

}

