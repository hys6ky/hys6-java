package hyren.serv6.base.utils;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.NumberFormat;

@DocClass(desc = "", author = "dhw", createdate = "2022-05-27 15:30:28")
public class NumberFormatUtil {

    @Method(desc = "", logicStep = "")
    @Param(name = "needFormatNum", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String formatNumber(String needFormatNum) {
        String unit = "";
        String formatNumStr = "";
        StringBuilder sb = new StringBuilder();
        BigDecimal bigDecimal = new BigDecimal("10000");
        BigDecimal bigDecimal2 = new BigDecimal("100000000");
        BigDecimal needNum = new BigDecimal(needFormatNum);
        if ((needNum.compareTo(bigDecimal) == 0 || needNum.compareTo(bigDecimal) > 0) && needNum.compareTo(bigDecimal2) < 0) {
            formatNumStr = needNum.divide(bigDecimal).setScale(3, RoundingMode.HALF_UP).toString();
            unit = "万";
        } else if (needNum.compareTo(bigDecimal2) >= 0) {
            formatNumStr = needNum.divide(bigDecimal2).setScale(3, RoundingMode.HALF_UP).toString();
            unit = "亿";
        } else {
            sb.append(needNum);
        }
        if (StringUtil.isNotBlank(formatNumStr)) {
            NumberFormat nf = NumberFormat.getInstance();
            nf.setGroupingUsed(false);
            double number = new BigDecimal(formatNumStr).doubleValue();
            sb.append(nf.format(number)).append(unit);
        }
        return sb.toString();
    }
}
