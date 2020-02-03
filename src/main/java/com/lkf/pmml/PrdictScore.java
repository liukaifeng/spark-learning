package com.lkf.pmml;

import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.*;
import org.jpmml.model.PMMLUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by sjmei on 2017/1/19.
 */
public class PrdictScore {
    public static void main(String[] args) throws Exception {
        PMML pmml = readPMML(new File("data/pmmlmodel/rf.pmml"));
        ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();
//        System.out.println(pmml.getModels().get(0));
        Evaluator evaluator = modelEvaluatorFactory.newModelEvaluator(pmml);
//        ModelEvaluator evaluator = new MiningModelEvaluator(pmml);
        evaluator.verify();
        List<InputField> inputFields = evaluator.getInputFields();
        InputStream is = new FileInputStream(new File("data/train.txt"));
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String line;
        int diffDelta = 0;
        int sameDelta = 0;
        while((line = br.readLine()) != null) {
            String[] splits = line.split("\t",-1);
            double targetMs = transToDouble(splits[14]);
            double risk_value = transToDouble(splits[2]);
            double label = 0.0;
            if(targetMs==1.0 && risk_value >5.0d){
                label = 1.0;
            }
            LinkedHashMap<FieldName, FieldValue> arguments = readArgumentsFromLine(splits, inputFields);
            Map<FieldName, ?> results = evaluator.evaluate(arguments);
            List<TargetField> targetFields = evaluator.getTargetFields();
            for(TargetField targetField : targetFields){
                FieldName targetFieldName = targetField.getName();
                Object targetFieldValue = results.get(targetFieldName);
                ProbabilityDistribution nodeMap = (ProbabilityDistribution)targetFieldValue;
                Object result = nodeMap.getResult();
                if(label == Double.valueOf(result.toString())){
                    sameDelta +=1;
                }else{
                    diffDelta +=1;
                }
            }
        }
        System.out.println("acc count:"+sameDelta);
        System.out.println("error count:"+diffDelta);
        System.out.println("acc rate:"+(sameDelta*1.0d/(sameDelta+diffDelta)));
    }
    /**
     * 从文件中读取pmml模型文件
     * @param file
     * @return
     * @throws Exception
     */
    public static PMML readPMML(File file) throws Exception {
        InputStream is = new FileInputStream(file);
        return PMMLUtil.unmarshal(is);
    }
    /**
     * 构造模型输入特征字段
     * @param splits
     * @param inputFields
     * @return
     */
    public static LinkedHashMap<FieldName, FieldValue> readArgumentsFromLine(String[] splits, List<InputField> inputFields) {
        List<Double> lists = new ArrayList<Double>();
        lists.add(Double.valueOf(splits[3]));
        lists.add(Double.valueOf(splits[4]));
        lists.add(Double.valueOf(splits[5]));
        lists.add(Double.valueOf(splits[7]));
        lists.add(Double.valueOf(splits[8]));
        lists.add(Double.valueOf(splits[9]));
        lists.add(Double.valueOf(splits[10]));
        lists.add(Double.valueOf(splits[11]));
        LinkedHashMap<FieldName, FieldValue> arguments = new LinkedHashMap<FieldName, FieldValue>();
        int i = 0;
        for(InputField inputField : inputFields){
            FieldName inputFieldName = inputField.getName();
            // The raw (ie. user-supplied) value could be any Java primitive value
            Object rawValue = lists.get(i);
            // The raw value is passed through: 1) outlier treatment, 2) missing value treatment, 3) invalid value treatment and 4) type conversion
            FieldValue inputFieldValue = inputField.prepare(rawValue);
            arguments.put(inputFieldName, inputFieldValue);
            i+=1;
        }
        return arguments;
    }
    public static Double transToDouble(String label) {
        try {
            return Double.valueOf(label);
        }catch (Exception e){
            return Double.valueOf(0);
        }
    }
}