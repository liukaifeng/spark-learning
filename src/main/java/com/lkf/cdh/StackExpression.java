package com.lkf.cdh;

import java.util.Stack;

/**
 * todo 一句话描述该类的用途
 *
 * @author 刘凯峰
 * @date 2019-03-18 16-37
 */
public class StackExpression {

    /**
     * 表达式解析
     *
     * @param expression 表达式
     * @return int
     * @author 刘凯峰
     * @date 2019/3/18 17:21
     */
    public static int parserExpression( String expression ) {
        //存放操作数的栈
        Stack<Integer> operandStack = new Stack<>();
        //存放运算符的栈
        Stack<Character> operatorStack = new Stack<>();
        //将字符串分割成单个字符
        String[] charArr = expression.split("");
        for (String aCharArr : charArr) {
            //如果字符串为空，则跳过此次循环
            if (!"".equals(aCharArr.trim())) {
                if ("+".equals(aCharArr.trim()) || "-".equals(aCharArr.trim())) {
                    //如果字符串为"+"或者"-"，则执行栈中已存数据的加减计算
                    while (!operatorStack.isEmpty() && (operatorStack.peek() == '+' || operatorStack.peek() == '-')) {
                        processOneOperator(operandStack, operatorStack);
                    }
                    //将操作符压入操作符栈中
                    operatorStack.push(aCharArr.charAt(0));
                } else if ("*".equals(aCharArr.trim()) || "/".equals(aCharArr.trim())) {
                    //如果字符串为"*"或者"/"，则执行栈中已存数据的乘除计算
                    while (!operatorStack.isEmpty() && (operatorStack.peek() == '*' || operatorStack.peek() == '/')) {
                        processOneOperator(operandStack, operatorStack);
                    }
                    operatorStack.push(aCharArr.charAt(0));
                } else if ("(".equals(aCharArr.trim())) {
                    //如果遇到左括号，则将左括号压入操作符栈中
                    operatorStack.push('(');
                } else if (")".equals(aCharArr.trim())) {
                    //如果遇到右括号，则计算栈中的数据，直到遇到左括号为止
                    while (operatorStack.peek() != '(') {
                        processOneOperator(operandStack, operatorStack);
                    }
                    operatorStack.pop();//将进行过计算的左括号弹出
                } else {
                    //如果遇到的是操作数，则将操作数直接压入操作数栈中
                    operandStack.push(Integer.parseInt(aCharArr));
                }
            }
        }
        //对栈中数据进行计算，知道栈为空为止
        while (!operatorStack.isEmpty()) {
            processOneOperator(operandStack, operatorStack);
        }
        //此时操作数栈中的栈顶元素也就是计算结果
        return operandStack.pop();
    }

    /**
     * 根据栈顶的运算符，计算位于栈顶的操作数
     *
     * @param operandStack  运算符栈
     * @param operatorStack 操作数栈
     */
    private static void processOneOperator( Stack<Integer> operandStack, Stack<Character> operatorStack ) {
        //取栈顶操作符
        char operator = operatorStack.pop();
        //取栈顶操作数
        int pop1 = operandStack.pop();
        //取栈顶操作数
        int pop2 = operandStack.pop();
        //根据操作符计算两个操作数，并将计算结果压入栈顶
        if (operator == '+') {
            operandStack.push(pop2 + pop1);
        } else if (operator == '-') {
            operandStack.push(pop2 - pop1);
        } else if (operator == '*') {
            operandStack.push(pop2 * pop1);
        } else if (operator == '/') {
            operandStack.push(pop2 / pop1);
        }
    }

    public static void main( String[] args ) {
        System.out.println("3+(8-3)*6/3 = " + parserExpression("3+(8-3)*6/3"));

    }
}
