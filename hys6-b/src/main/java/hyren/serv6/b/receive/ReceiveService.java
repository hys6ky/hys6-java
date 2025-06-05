package hyren.serv6.b.receive;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.JsonUtil;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.CollectCase;
import hyren.serv6.base.entity.DataStoreReg;
import hyren.serv6.base.entity.SourceFileAttribute;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.packutil.PackUtil;
import io.swagger.annotations.Api;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
@Api("海云服务接收端")
@DocClass(desc = "", author = "zxz", createdate = "2019/11/19 11:17")
public class ReceiveService {

    @Method(desc = "", logicStep = "")
    @Param(name = "addSql", desc = "", range = "")
    @Param(name = "addParamsPool", desc = "", range = "")
    public void batchAddSourceFileAttribute(String addSql, String addParamsPool) {
        addParamsPool = PackUtil.unpackMsg(addParamsPool).get("msg");
        List<Object[]> objects = parseListArray(addParamsPool);
        int[] adds = Dbo.executeBatch(addSql, objects);
        for (int i : adds) {
            if (i != 1) {
                throw new BusinessException("批量添加source_file_attribute表失败");
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "updateSql", desc = "", range = "")
    @Param(name = "updateParamsPool", desc = "", range = "")
    public void batchUpdateSourceFileAttribute(String updateSql, String updateParamsPool) {
        updateParamsPool = PackUtil.unpackMsg(updateParamsPool).get("msg");
        List<Object[]> objects = parseListArray(updateParamsPool);
        int[] updates = Dbo.executeBatch(updateSql, objects);
        for (int i : updates) {
            if (i != 1) {
                throw new BusinessException("批量更新source_file_attribute表失败");
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collect_case", desc = "", range = "")
    @Param(name = "msg", desc = "", range = "")
    public void saveCollectCase(String collect_case, String msg) {
        collect_case = PackUtil.unpackMsg(collect_case).get("msg");
        CollectCase collect = JsonUtil.toObject(collect_case, new TypeReference<CollectCase>() {
        });
        Result result = Dbo.queryResult("select * from collect_case where agent_id = ? and source_id = ?" + " and collect_set_id = ?  and job_type = ? and etl_date = ?  and task_classify = ? ", collect.getAgent_id(), collect.getSource_id(), collect.getCollect_set_id(), collect.getJob_type(), collect.getEtl_date(), collect.getTask_classify());
        if (result.isEmpty()) {
            String job_rs_id = UUID.randomUUID().toString();
            collect.setJob_rs_id(job_rs_id);
            collect.setCc_remark(msg);
            collect.add(Dbo.db());
        } else {
            String job_rs_id = result.getString(0, "job_rs_id");
            collect.setJob_rs_id(job_rs_id);
            collect.setCc_remark(msg);
            collect.update(Dbo.db());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "source_file_attribute", desc = "", range = "")
    public void addSourceFileAttribute(String source_file_attribute) {
        source_file_attribute = PackUtil.unpackMsg(source_file_attribute).get("msg");
        SourceFileAttribute attribute = JsonUtil.toObject(source_file_attribute, new TypeReference<SourceFileAttribute>() {
        });
        Result result = Dbo.queryResult("select * from source_file_attribute where lower(hbase_name) = lower(?)", attribute.getHbase_name());
        if (result.isEmpty()) {
            attribute.setFile_id(UUID.randomUUID().toString());
            attribute.add(Dbo.db());
        } else {
            attribute.setFile_id(result.getString(0, "file_id"));
            attribute.update(Dbo.db());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "data_store_reg", desc = "", range = "")
    public void addDataStoreReg(String data_store_reg) {
        data_store_reg = PackUtil.unpackMsg(data_store_reg).get("msg");
        DataStoreReg data_store = JsonUtil.toObject(data_store_reg, new TypeReference<DataStoreReg>() {
        });
        Result result = Dbo.queryResult("select * from data_store_reg where lower(hyren_name) = lower(?)", data_store.getHyren_name());
        if (result.isEmpty()) {
            data_store.setFile_id(UUID.randomUUID().toString());
            data_store.add(Dbo.db());
        } else {
            data_store.setFile_id(result.getString(0, "file_id"));
            data_store.update(Dbo.db());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "addSql", desc = "", range = "")
    @Param(name = "addParamsPool", desc = "", range = "")
    public void batchAddFtpTransfer(String addSql, String addParamsPool) {
        addParamsPool = PackUtil.unpackMsg(addParamsPool).get("msg");
        List<Object[]> objects = parseListArray(addParamsPool);
        int[] adds = Dbo.executeBatch(addSql, objects);
        for (int i : adds) {
            if (i != 1) {
                throw new BusinessException("批量添加ftp_transfered表失败");
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "paramPool", desc = "", range = "")
    private List<Object[]> parseListArray(String paramPool) {
        List<Object[]> arrayList = new ArrayList<>();
        List<Object> data = JsonUtil.toObject(paramPool, new TypeReference<List<Object>>() {
        });
        for (Object aaa : data) {
            List<Object> array1 = JsonUtil.toObject(JsonUtil.toJson(aaa), new TypeReference<List<Object>>() {
            });
            if (array1 != null && !array1.isEmpty()) {
                Object[] o = new Object[array1.size()];
                for (int i = 0; i < array1.size(); i++) {
                    o[i] = array1.get(i);
                }
                arrayList.add(o);
            }
        }
        return arrayList;
    }
}
