package hyren.serv6.b.realtimecollection.realtimeCollectManagement.userpermission;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.FlowApplyStatus;
import hyren.serv6.base.entity.SdmUserPermission;
import hyren.serv6.commons.utils.DboExecute;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "yec", createdate = "2021-4-6")
@Service
@Slf4j
public class UserPermissionService {

    @Method(desc = "", logicStep = "")
    @Param(name = "app_id", desc = "", range = "")
    public void userApplicationPass(long app_id) {
        DboExecute.updatesOrThrow("用户申请通过失败!", "update " + SdmUserPermission.TableName + " set application_status = ? where app_id = ?", FlowApplyStatus.ShenQingTongGuo.getCode(), app_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "app_id", desc = "", range = "")
    public void userApplicationNoPass(long app_id) {
        DboExecute.updatesOrThrow("用户申请不通过失败!", "update " + SdmUserPermission.TableName + " set application_status = ? where app_id = ?", FlowApplyStatus.ShenQingBuTongGuo.getCode(), app_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> searchUserApplication(int currPage, int pageSize) {
        Map<String, Object> sdmUserPermissionMap = new HashMap<>();
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("SELECT t1.*,t2.sdm_receive_name,t3.sdm_top_name as topic_name from sdm_user_permission t1 left join sdm_receive_conf t2 " + " on t1.sdm_receive_id = t2.sdm_receive_id left join sdm_topic_info t3 on t1.topic_id = t3.topic_id ");
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> maps = Dbo.queryPagedList(page, asmSql.sql());
        for (Map<String, Object> map : maps) {
            map.put("application_status", FlowApplyStatus.ofValueByCode(map.get("application_status").toString()));
        }
        sdmUserPermissionMap.put("sdmUserPermissions", maps);
        sdmUserPermissionMap.put("totalSize", page.getTotalSize());
        return sdmUserPermissionMap;
    }
}
