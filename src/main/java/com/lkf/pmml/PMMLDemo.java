package com.lkf.pmml;

import jdk.internal.org.xml.sax.SAXException;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.*;

import javax.xml.bind.JAXBException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * todo 一句话描述该类的用途
 *
 * @author 刘凯峰
 * @date 2019-07-22 15-42
 */
public class PMMLDemo {
    private Evaluator loadPmml() throws SAXException {
        PMML pmml = new PMML();
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream("G:\\pmml\\xgb.pmml");
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (inputStream == null) {
            return null;
        }
        InputStream is = inputStream;
        try {
            pmml = org.jpmml.model.PMMLUtil.unmarshal(is);
        } catch (JAXBException e1) {
            e1.printStackTrace();
        } catch (org.xml.sax.SAXException e) {
            e.printStackTrace();
        } finally {
            //关闭输入流
            try {
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();
        Evaluator evaluator = modelEvaluatorFactory.newModelEvaluator(pmml);
        pmml = null;
        return evaluator;
    }

    private int predict( Evaluator evaluator, String a, String b ) {
        Map<String, String> data = new HashMap<String, String>();

        data.put("day_of_week", a);
        data.put("month", b);
        List<InputField> inputFields = evaluator.getInputFields();
        //过模型的原始特征，从画像中获取数据，作为模型输入
        Map<FieldName, FieldValue> arguments = new LinkedHashMap<FieldName, FieldValue>();
        for (InputField inputField : inputFields) {
            FieldName inputFieldName = inputField.getName();
            Object rawValue = data.get(inputFieldName.getValue());
            FieldValue inputFieldValue = inputField.prepare(rawValue);
            arguments.put(inputFieldName, inputFieldValue);
        }

        Map<FieldName, ?> results = evaluator.evaluate(arguments);
        List<TargetField> targetFields = evaluator.getTargetFields();

        TargetField targetField = targetFields.get(0);
        FieldName targetFieldName = targetField.getName();

        Object targetFieldValue = results.get(targetFieldName);
        System.out.println("target: " + targetFieldName.getValue() + " value: " + targetFieldValue);
        int primitiveValue = -1;
        if (targetFieldValue instanceof Computable) {
            Computable computable = (Computable) targetFieldValue;
            primitiveValue = (Integer) computable.getResult();
        }
        System.out.println(a + " " + b + ":" + primitiveValue);
        return primitiveValue;
    }

    public static void main( String args[] ) throws SAXException {
        PMMLDemo demo = new PMMLDemo();
        Evaluator model = demo.loadPmml();
        demo.predict(model, "2","7");
    }
}
