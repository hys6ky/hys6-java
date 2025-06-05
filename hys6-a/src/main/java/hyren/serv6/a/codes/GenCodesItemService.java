package hyren.serv6.a.codes;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.db.resultset.Result;
import hyren.serv6.base.codes.fdCode.WebCodesItem;
import org.springframework.stereotype.Service;
import java.util.Map;

@Service
public class GenCodesItemService {

    @Method(desc = "", logicStep = "")
    @Param(name = "category", desc = "", range = "")
    @Param(name = "code", desc = "", range = "")
    @Return(desc = "", range = "")
    public String getValue(String category, String code) {
        return WebCodesItem.getValue(category, code);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "category", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getCategoryItems(String category) {
        return WebCodesItem.getCategoryItems(category);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "category", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, String> getCodeItems(String category) {
        return WebCodesItem.getCodeItems(category);
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getAllCodeItems() {
        return WebCodesItem.getAllCodeItems();
    }
}
