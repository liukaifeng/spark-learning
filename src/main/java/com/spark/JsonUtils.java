package com.spark;



import net.sf.ezmorph.object.DateMorpher;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JsonConfig;
import net.sf.json.util.JSONUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;




/**********************************************************
 * 项目名称：tcslWebService <br>
 * 包名称：com.tcsl.service.utils <br>
 * 类名称：JsonUtils <br>
 * 类描述： <br>
 * 创建人：夏鸿鹏 <br>
 * 创建时间：2013-5-23 下午03:11:44 <br>
 * 修改备注：
 **********************************************************/

public class JsonUtils {

	private JsonUtils() {
	}

	private static final JsonUtils jsonUtils = new JsonUtils();

	synchronized public static JsonUtils getInstance() {
		return jsonUtils;
	}

	private void setDataFormat2JAVA() {
		// 设定日期转换格式
		JSONUtils.getMorpherRegistry().registerMorpher(
				new DateMorpher(new String[] {"yyyy-MM-dd HH:mm:ss"}));
	}

	public String Object2Json(Object object)  {
		String jsonString = null;
		// 日期值处理器
		JsonConfig jsonConfig = new JsonConfig();
		jsonConfig.registerJsonValueProcessor(java.util.Date.class,
				new JsonDateValueProcessor());
		if (object != null) {
			if (object instanceof Collection || object instanceof Object[]) {
				jsonString = JSONArray.fromObject(object, jsonConfig)
						.toString();
			} else {
				jsonString = JSONObject.fromObject(object, jsonConfig)
						.toString();
			}
		}
		return jsonString == null ? "{}" : jsonString;
	}
	
	public String Object2Json(Object object, String[] configStr) throws Exception {
		String jsonString = null;
		// 日期值处理器
		JsonConfig jsonConfig = new JsonConfig();
		jsonConfig.setExcludes(configStr);	
		jsonConfig.registerJsonValueProcessor(java.util.Date.class,
				new JsonDateValueProcessor());
		if (object != null) {
			if (object instanceof Collection || object instanceof Object[]) {
				jsonString = JSONArray.fromObject(object, jsonConfig)
						.toString();
			} else {
				jsonString = JSONObject.fromObject(object, jsonConfig)
						.toString();
			}
		}
		return jsonString == null ? "{}" : jsonString;
	}

	@SuppressWarnings("unchecked")
    public <T> T getDTO(String jsonString, Class<T> clazz) {
		JSONObject jsonObject = null;
		try {
			setDataFormat2JAVA();
			jsonObject = JSONObject.fromObject(jsonString);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return (T) JSONObject.toBean(jsonObject, clazz);
	}
	
	@SuppressWarnings("rawtypes")
	public Object getDTO(String jsonString, Class clazz, Map map) {
		JSONObject jsonObject = null;
		try {
			setDataFormat2JAVA();
			jsonObject = JSONObject.fromObject(jsonString);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return JSONObject.toBean(jsonObject, clazz, map);
	}
	
	
	@SuppressWarnings("rawtypes")
	public String  getArrayDTO(String jsonString, String getKey) {
		JSONArray jSONArray = null;
		String value = "";
		try {
			setDataFormat2JAVA();
			jSONArray = JSONArray.fromObject(jsonString);
			for (int i = 0; i < jSONArray.size(); i++) {
				Map o = (Map) jSONArray.get(i);
				if (o.get(getKey) != null) {
					value = o.get(getKey).toString();
				}			
			}
		} catch (Exception e) {
			//e.printStackTrace();
			return value;
		}
		return value;
	}
	
	
	@SuppressWarnings({ "rawtypes", "deprecation", "static-access" })
	public List getArrayDTO(String jsonString, Class clazz) { 
		JSONArray jSONArray = null;
		try {
			setDataFormat2JAVA();
			jSONArray = JSONArray.fromObject(jsonString);
			return jSONArray.toList(jSONArray, clazz);		
//			return jSONArray.toArray();
		} catch (Exception e) {
			return null;
		}
	}
	
	@SuppressWarnings({ "rawtypes", "deprecation", "static-access" })
	public List getArrayDTO(String jsonString, Class clazz, Map<String, Class> classMap) {
		JSONArray jSONArray = null;
		try {
			setDataFormat2JAVA();
			jSONArray = JSONArray.fromObject(jsonString);			
			return jSONArray.toList(jSONArray, clazz, classMap);		
//			return jSONArray.toArray();
		} catch (Exception e) {
			return null;
		}
	}

	
	public static void main(String[] args) {
		
		
//		String a = "[{\"orderId\":100,\"shopId\":144,\"dishes\":[{\"dishId\":60001,\"count\":1}],},{\"orderId\":200,\"shopId\":103}]";
//		
//		
//		Map<String, Class> classMap = new HashMap<String, Class>();  
//        classMap.put("dishes", CDTO.class);
//		
//		List<TDTO> b = JsonUtils.getInstance().getArrayDTO(a, TDTO.class, classMap);
//		
//		
//		
//		for (int i=0; i<b.size(); i++) {		
//			List<CDTO>  cc = b.get(i).getDishes();
//			
//			if (cc != null) {
//				for (int j=0; j<cc.size(); j++) {
//					System.out.println(cc.get(j).getDishId());
//				}
//			}
//		}
		
//		List list = new ArrayList();
//		LoginInfo loginInfo = new LoginInfo(true, "login ok", "12345678",
//				"2ED4A98309293EA232EB32CD3232ED4A98309293EA232EB32CD323D4A9830929");
//		list.add(loginInfo);
//		try {
//			System.out.println(JsonUtils.getInstance().Object2Json(loginInfo));
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		
		
	
		
	}
	
}
