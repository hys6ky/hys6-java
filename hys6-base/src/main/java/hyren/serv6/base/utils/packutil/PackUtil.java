package hyren.serv6.base.utils.packutil;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.AppSystemException;
import java.util.HashMap;
import java.util.Map;

@DocClass(desc = "", author = "WangZhengcheng")
public class PackUtil {

    @Method(desc = "", logicStep = "")
    @Param(name = "jsonData", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String packMsg(String jsonData) {
        try {
            int valueSize = jsonData.getBytes().length;
            StringBuilder sbData = new StringBuilder();
            if (valueSize >= 300) {
                sbData.append(IsFlag.Shi.getCode());
                sbData.append(ZipUtils.gzip(jsonData));
            } else {
                sbData.append(IsFlag.Fou.getCode());
                sbData.append(jsonData);
            }
            return sbData.toString();
        } catch (Exception var7) {
            throw new AppSystemException("SENDERR:数据打包失败!", var7);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "data", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Map<String, String> unpackMsg(String data) {
        String compressed = data.substring(0, 1);
        IsFlag compressedFlag = IsFlag.ofEnumByCode(compressed);
        Map<String, String> map = new HashMap<>();
        map.put("isCompress", compressed);
        String msg = data.substring(1);
        if (compressedFlag == IsFlag.Shi) {
            map.put("msg", StringUtil.isBlank(msg) ? "" : ZipUtils.gunzip(msg));
        } else {
            map.put("msg", StringUtil.isBlank(msg) ? "" : msg);
        }
        return map;
    }
}
