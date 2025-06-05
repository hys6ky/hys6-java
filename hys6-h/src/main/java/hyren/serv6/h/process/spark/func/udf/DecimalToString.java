package hyren.serv6.h.process.spark.func.udf;

import org.apache.spark.sql.api.java.UDF1;

public class DecimalToString implements UDF1<Object, String> {

    private static final long serialVersionUID = -4984843644982746038L;

    @Override
    public String call(Object num) {
        String string = num.toString();
        boolean flg = false;
        if (string.contains("-")) {
            flg = true;
            string = string.replaceAll("-", "");
        }
        String result;
        StringBuilder strb = new StringBuilder();
        String sub1;
        String sub2;
        strb.delete(0, strb.length());
        String[] split = string.split("E");
        String num1 = split[0];
        int num2 = Integer.parseInt(split[1]);
        if (num2 > 0) {
            if (num1.contains(".")) {
                String[] split2 = num1.split("\\.");
                if (split2[1].length() > num2) {
                    sub1 = split2[1].substring(0, num2);
                    sub2 = split2[1].substring(num2);
                    strb.append(split2[0]).append(sub1).append(".").append(sub2);
                } else {
                    strb.append(split2[0]).append(split2[1]).append(".");
                    for (int i = 0; i < num2 - split2.length; i++) {
                        strb.append("0");
                    }
                }
            } else {
                strb.append(split[0]);
                for (int i = 0; i < num2; i++) {
                    strb.append("0");
                }
            }
        } else if (0 == num2) {
            strb.append(split[0]);
        } else {
            num2 = -num2;
            strb.append("0");
            if (split[0].contains(".")) {
                String[] split2 = split[0].split("\\.");
                for (int i = 0; i < num2 - 1; i++) {
                    strb.append("0");
                }
                strb.append(split[0]);
            }
        }
        result = strb.toString();
        if (flg) {
            result = "-" + result;
        }
        return result;
    }
}
