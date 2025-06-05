package hyren.serv6.base.utils.jsch;

import fd.ng.core.utils.NumberUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.regular.RegexConstant;
import net.sourceforge.pinyin4j.PinyinHelper;
import net.sourceforge.pinyin4j.format.HanyuPinyinCaseType;
import net.sourceforge.pinyin4j.format.HanyuPinyinOutputFormat;
import net.sourceforge.pinyin4j.format.HanyuPinyinToneType;
import net.sourceforge.pinyin4j.format.HanyuPinyinVCharType;
import net.sourceforge.pinyin4j.format.exception.BadHanyuPinyinOutputFormatCombination;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ChineseUtil {

    static final int GB_SP_DIFF = 160;

    static final int[] secPosValueList = { 1601, 1637, 1833, 2078, 2274, 2302, 2433, 2594, 2787, 3106, 3212, 3472, 3635, 3722, 3730, 3858, 4027, 4086, 4390, 4558, 4684, 4925, 5249, 5600 };

    static final char[] firstLetter = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'w', 'x', 'y', 'z' };

    public static boolean isChinese(String str) {
        Pattern p = Pattern.compile(RegexConstant.IS_CHINESE);
        Matcher m;
        boolean flag = false;
        for (int i = 0; i < str.length(); i++) {
            String temp;
            if (i != str.length() - 1) {
                temp = str.substring(i, i + 1);
            } else {
                temp = str.substring(i);
            }
            m = p.matcher(temp);
            if (m.matches()) {
                flag = true;
            } else {
                flag = false;
                break;
            }
        }
        return flag;
    }

    public static String getFirstLetter(String oriStr) {
        String str = oriStr.toLowerCase();
        StringBuilder buffer = new StringBuilder();
        char ch;
        char[] temp;
        for (int i = 0; i < str.length(); i++) {
            ch = str.charAt(i);
            temp = new char[] { ch };
            byte[] uniCode = new String(temp).getBytes();
            if (uniCode[0] > 0) {
                buffer.append(temp);
            } else {
                buffer.append(convert(uniCode));
            }
        }
        return buffer.toString();
    }

    public static char convert(byte[] bytes) {
        char result = '-';
        int secPosValue;
        int i;
        for (i = 0; i < bytes.length; i++) {
            bytes[i] -= GB_SP_DIFF;
        }
        secPosValue = bytes[0] * 100 + bytes[1];
        for (i = 0; i < 23; i++) {
            if (secPosValue >= secPosValueList[i] && secPosValue < secPosValueList[i + 1]) {
                result = firstLetter[i];
                break;
            }
        }
        return result;
    }

    public static String[] getAlphaFromString(String str) {
        String[] arrays = str.split(RegexConstant.NO_ALPHABET);
        int count = 0;
        String index;
        String[] returnString = new String[2];
        for (String string : arrays) {
            if (isAlpha(string)) {
                count++;
                index = str.indexOf(string) + "";
                returnString[0] = string;
                returnString[1] = index;
            }
            if (count == 1) {
                break;
            }
        }
        if (StringUtil.isEmpty(returnString[0])) {
            returnString[0] = "";
            returnString[1] = "-1";
        }
        return returnString;
    }

    public static boolean isAlpha(String str) {
        Pattern pattern = Pattern.compile(RegexConstant.IS_ALPHABET);
        Matcher isAlpha = pattern.matcher(str);
        return isAlpha.matches();
    }

    public static String[] getNumFromString(String str) {
        String[] arrays = str.split(RegexConstant.NO_NUMBER);
        int count = 0;
        String index;
        String[] returnString = new String[2];
        for (String string : arrays) {
            if (NumberUtil.isNumberic(string)) {
                count++;
                index = str.indexOf(string) + "";
                returnString[0] = string;
                returnString[1] = index;
            }
            if (count == 1) {
                break;
            }
        }
        if (StringUtil.isEmpty(returnString[0])) {
            returnString[0] = "";
            returnString[1] = "-1";
        }
        return returnString;
    }

    public static String getPingYin(String inputString) {
        HanyuPinyinOutputFormat format = new HanyuPinyinOutputFormat();
        format.setCaseType(HanyuPinyinCaseType.LOWERCASE);
        format.setToneType(HanyuPinyinToneType.WITHOUT_TONE);
        format.setVCharType(HanyuPinyinVCharType.WITH_V);
        char[] input = inputString.trim().toCharArray();
        StringBuilder output = new StringBuilder();
        try {
            for (char c : input) {
                if (Character.toString(c).matches("[\\u4E00-\\u9FA5]+")) {
                    String[] temp = PinyinHelper.toHanyuPinyinStringArray(c, format);
                    output.append(temp[0]);
                } else {
                    output.append(StringUtil.replace(Character.toString(c), " ", ""));
                }
            }
        } catch (BadHanyuPinyinOutputFormatCombination e) {
            throw new BusinessException("转换失败");
        }
        return output.toString();
    }

    public static String getCnASCII(String str) {
        StringBuilder strBuf = new StringBuilder();
        byte[] byteGBK = str.getBytes();
        for (byte b : byteGBK) {
            strBuf.append(Integer.toHexString(b & 0xff));
        }
        return strBuf.toString();
    }
}
