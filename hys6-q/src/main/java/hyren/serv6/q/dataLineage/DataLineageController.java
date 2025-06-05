package hyren.serv6.q.dataLineage;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.Validator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.BusinessException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/dataLineage")
@Slf4j
public class DataLineageController {

    @Autowired
    private DataLineageServiceImpl dataLineageService;

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @PostMapping("/fuzzySearchTableName")
    public List<String> fuzzySearchTableName() {
        return dataLineageService.fuzzySearchTableName();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "table_name", desc = "", range = "")
    @Param(name = "search_type", desc = "", range = "")
    @Param(name = "search_relationship", desc = "", range = "")
    @Return(desc = "", range = "")
    @PostMapping("/getTableBloodRelationship")
    public Map<String, Object> getTableBloodRelationship(String table_name, String search_type, String search_relationship) {
        Validator.notBlank(table_name, "搜索表名称为空!");
        Validator.notBlank(search_type, "搜索类型为空!");
        Validator.notBlank(search_relationship, "搜索关系为空!");
        IsFlag is_sr = IsFlag.ofEnumByCode(search_relationship);
        Map<String, Object> tableBloodRelationshipMap;
        if (is_sr == IsFlag.Fou) {
            tableBloodRelationshipMap = dataLineageService.influencesDataInfo(Dbo.db(), table_name, search_type);
        } else if (is_sr == IsFlag.Shi) {
            tableBloodRelationshipMap = dataLineageService.bloodlineDateInfo(Dbo.db(), table_name, search_type);
        } else {
            throw new BusinessException("搜索类型不匹配! search_type=" + search_relationship);
        }
        return tableBloodRelationshipMap;
    }
}
