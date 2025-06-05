package hyren.serv6.base.utils;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import net.sourceforge.pinyin4j.PinyinHelper;
import net.sourceforge.pinyin4j.format.HanyuPinyinCaseType;
import net.sourceforge.pinyin4j.format.HanyuPinyinOutputFormat;
import net.sourceforge.pinyin4j.format.HanyuPinyinToneType;
import net.sourceforge.pinyin4j.format.exception.BadHanyuPinyinOutputFormatCombination;

@DocClass(desc = "", author = "博彦科技", createdate = "2020/4/8 0008 下午 04:51")
public class PinyinUtil {

    private HanyuPinyinOutputFormat hanyuPinyinOutputFormat;

    public enum Type {

        UPPERCASE(HanyuPinyinCaseType.UPPERCASE), LOWERCASE(HanyuPinyinCaseType.LOWERCASE);

        private HanyuPinyinCaseType caseType;

        Type(HanyuPinyinCaseType caseType) {
            this.caseType = caseType;
        }

        protected HanyuPinyinCaseType getType() {
            if (null == caseType) {
                caseType = HanyuPinyinCaseType.UPPERCASE;
            }
            return caseType;
        }
    }

    public PinyinUtil() {
        hanyuPinyinOutputFormat = new HanyuPinyinOutputFormat();
        hanyuPinyinOutputFormat.setCaseType(HanyuPinyinCaseType.UPPERCASE);
        hanyuPinyinOutputFormat.setToneType(HanyuPinyinToneType.WITHOUT_TONE);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "chinese", desc = "", range = "")
    @Return(desc = "", range = "")
    public String toPinYin(String chinese) throws BadHanyuPinyinOutputFormatCombination {
        return toPinYin(chinese, Type.UPPERCASE);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "chinese", desc = "", range = "")
    @Param(name = "type", desc = "", range = "")
    @Return(desc = "", range = "")
    public String toPinYin(String chinese, Type type) throws BadHanyuPinyinOutputFormatCombination {
        if (null == chinese || chinese.trim().length() == 0)
            return "";
        hanyuPinyinOutputFormat.setCaseType(type.getType());
        StringBuilder py = new StringBuilder();
        String[] t;
        for (int i = 0; i < chinese.length(); i++) {
            char c = chinese.charAt(i);
            if ((int) c <= 128)
                py.append(c);
            else {
                t = PinyinHelper.toHanyuPinyinStringArray(c, hanyuPinyinOutputFormat);
                if (t == null)
                    py.append(c);
                else {
                    py.append(t[0]);
                }
            }
        }
        return py.toString().replaceAll("\\W", "").trim();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "chinese", desc = "", range = "")
    @Return(desc = "", range = "")
    public String toFixPinYin(String chinese) {
        return toFixPinYin(chinese, 1, Type.UPPERCASE);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "chinese", desc = "", range = "")
    @Param(name = "number", desc = "", range = "")
    @Return(desc = "", range = "")
    public String toFixPinYin(String chinese, int number) {
        return toFixPinYin(chinese, number, Type.UPPERCASE);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "chinese", desc = "", range = "")
    @Param(name = "number", desc = "", range = "")
    @Param(name = "type", desc = "", range = "")
    @Return(desc = "", range = "")
    public String toFixPinYin(String chinese, int number, Type type) {
        if (null == chinese || chinese.trim().length() == 0 || 0 == number) {
            return "";
        }
        hanyuPinyinOutputFormat.setCaseType(type.getType());
        StringBuilder pybf = new StringBuilder();
        char[] arr = chinese.toCharArray();
        for (char c : arr) {
            if (c > 128) {
                try {
                    String[] temp = PinyinHelper.toHanyuPinyinStringArray(c, hanyuPinyinOutputFormat);
                    if (temp != null) {
                        for (int j = 0; j < number; j++) {
                            if (temp[0].length() <= j) {
                                continue;
                            }
                            pybf.append(temp[0].charAt(j));
                        }
                    }
                } catch (BadHanyuPinyinOutputFormatCombination e) {
                    e.printStackTrace();
                }
            } else {
                pybf.append(c);
            }
        }
        return pybf.toString().replaceAll("\\W", "").trim();
    }
}
