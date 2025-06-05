package hyren.serv6.a.codemaintenance;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.BeanUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.a.entity.OrigCodeInfoDto;
import hyren.serv6.base.entity.HyrenCodeInfo;
import hyren.serv6.base.entity.OrigCodeInfo;
import hyren.serv6.base.entity.OrigSysoInfo;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import java.util.List;
import java.util.Map;

@Service
public class CodeMaintenanceService {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result getCodeInfo() {
        return Dbo.queryResult("select * from " + HyrenCodeInfo.TableName + " where code_classify in( select " + "code_classify from " + HyrenCodeInfo.TableName + " GROUP BY code_classify)");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "hyren_code_infos", desc = "", range = "", isBean = true)
    public void saveCodeInfo(@RequestBody HyrenCodeInfo[] hyren_code_infos) {
        for (HyrenCodeInfo hyren_code_info : hyren_code_infos) {
            checkHyrenCodeInfoFields(hyren_code_info);
            if (Dbo.queryNumber("select count(*) from " + HyrenCodeInfo.TableName + " where code_classify= ? and code_value=?", hyren_code_info.getCode_classify(), hyren_code_info.getCode_value()).orElseThrow(() -> new BusinessException("sql查询错误")) > 0) {
                throw new BusinessException("当前编码分类已存在或者对应的编码类型值已存在，不能新增");
            }
            hyren_code_info.add(Dbo.db());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "hyren_code_info", desc = "", range = "")
    private void checkHyrenCodeInfoFields(HyrenCodeInfo hyren_code_info) {
        Validator.notBlank(hyren_code_info.getCode_classify(), "编码分类不能为空");
        Validator.notBlank(hyren_code_info.getCode_classify_name(), "编码分类名称不能为空");
        Validator.notBlank(hyren_code_info.getCode_type_name(), "编码名称不能为空");
        Validator.notBlank(hyren_code_info.getCode_value(), "编码类型值不能为空");
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result getOrigSysInfo() {
        return Dbo.queryResult("select * from " + OrigSysoInfo.TableName);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "orig_syso_info", desc = "", range = "", isBean = true)
    public void addOrigSysInfo(OrigSysoInfo orig_syso_info) {
        Validator.notBlank(orig_syso_info.getOrig_sys_code(), "码值系统编码不能为空");
        Validator.notBlank(orig_syso_info.getOrig_sys_name(), "码值系统名称不能为空");
        if (Dbo.queryNumber("select count(*) from " + OrigSysoInfo.TableName + " where orig_sys_code=?", orig_syso_info.getOrig_sys_code()).orElseThrow(() -> new BusinessException("sql查询错误")) > 0) {
            throw new BusinessException("源系统编号已存在，不能新增");
        }
        orig_syso_info.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "orig_sys_code", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getOrigCodeInfo(String orig_sys_code) {
        return Dbo.queryResult("select t1.*,t2.code_classify_name,t2.code_type_name from " + OrigCodeInfo.TableName + " t1," + HyrenCodeInfo.TableName + " t2" + " where t1.code_classify=t2.code_classify AND t1.code_value=t2.code_value" + " AND t1.orig_sys_code=? AND t2.code_classify in (" + " select code_classify from " + HyrenCodeInfo.TableName + " GROUP BY code_classify)", orig_sys_code);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "code_classify", desc = "", range = "")
    public void deleteCodeInfo(String code_classify) {
        if (Dbo.queryNumber("select count(*) from " + OrigCodeInfo.TableName + " where code_classify = ?", code_classify).orElseThrow(() -> new BusinessException("sql查询错误")) > 0) {
            throw new BusinessException("当前编码分类正在被使用，不能删除！");
        }
        Dbo.execute("delete from " + HyrenCodeInfo.TableName + " where code_classify=?", code_classify);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "code_classify", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getCodeInfoByCodeClassify(String code_classify) {
        return Dbo.queryList("select * from " + HyrenCodeInfo.TableName + " where code_classify=?", code_classify);
    }

    public List<OrigCodeInfoDto> stringToOrigCodeInfoDto(String origCodeInfoStr) {
        ObjectMapper objectMapper = new ObjectMapper();
        List<OrigCodeInfoDto> list = null;
        try {
            list = objectMapper.readValue(origCodeInfoStr, new TypeReference<List<OrigCodeInfoDto>>() {
            });
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return list;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "orig_code_infos", desc = "", range = "", isBean = true)
    @Param(name = "orig_sys_code", desc = "", range = "")
    public void addOrigCodeInfo(List<OrigCodeInfoDto> list, String orig_sys_code) {
        isOrigSysoInfoExist(orig_sys_code);
        for (OrigCodeInfoDto origCodeInfo : list) {
            OrigCodeInfo orig_code_info = new OrigCodeInfo();
            BeanUtil.copyProperties(origCodeInfo, orig_code_info);
            checkOrigCodeInfoFields(orig_code_info);
            orig_code_info.setOrig_sys_code(orig_sys_code);
            orig_code_info.setOrig_id(PrimayKeyGener.getNextId());
            orig_code_info.add(Dbo.db());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "orig_code_infos", desc = "", range = "", isBean = true)
    @Param(name = "orig_sys_code", desc = "", range = "")
    public void updateOrigCodeInfo(List<OrigCodeInfoDto> list, String orig_sys_code) {
        isOrigSysoInfoExist(orig_sys_code);
        for (OrigCodeInfoDto origCodeInfo : list) {
            OrigCodeInfo orig_code_info = new OrigCodeInfo();
            BeanUtil.copyProperties(origCodeInfo, orig_code_info);
            Validator.notNull(orig_code_info.getOrig_id(), "更新源系统编码信息时源系统编码主键不能为空");
            Validator.notBlank(orig_code_info.getCode_classify(), "编码分类不能为空");
            Validator.notBlank(orig_code_info.getOrig_value(), "源系统编码值不能为空");
            isHyrenCodeInfoExist(orig_code_info.getCode_classify());
            orig_code_info.setOrig_sys_code(orig_sys_code);
            orig_code_info.update(Dbo.db());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "orig_sys_code", desc = "", range = "")
    public void isOrigSysoInfoExist(String orig_sys_code) {
        if (Dbo.queryNumber("select count(*) from " + OrigSysoInfo.TableName + " where orig_sys_code=?", orig_sys_code).orElseThrow(() -> new BusinessException("sql查询错误")) == 0) {
            throw new BusinessException(orig_sys_code + "对应的源系统信息已不存在，请检查");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "orig_code_info", desc = "", range = "")
    public void checkOrigCodeInfoFields(OrigCodeInfo orig_code_info) {
        Validator.notBlank(orig_code_info.getCode_classify(), "编码分类不能为空");
        Validator.notBlank(orig_code_info.getCode_value(), "编码类型值不能为空");
        isHyrenCodeInfoExist(orig_code_info.getCode_classify());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "code_classify", desc = "", range = "")
    public void isHyrenCodeInfoExist(String code_classify) {
        if (Dbo.queryNumber("select count(*) from " + HyrenCodeInfo.TableName + " where code_classify=?", code_classify).orElseThrow(() -> new BusinessException("sql查询错误")) == 0) {
            throw new BusinessException(code_classify + "对应的统一编码信息已不存在，请检查");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "orig_sys_code", desc = "", range = "")
    @Param(name = "code_classify", desc = "", range = "")
    public void deleteOrigCodeInfo(String origSysCode, String codeClassify) {
        Dbo.execute("delete from " + OrigCodeInfo.TableName + " where code_classify = ? and orig_sys_code=?", codeClassify, origSysCode);
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<String> getAllCodeClassify() {
        return Dbo.queryOneColumnList("select code_classify from " + HyrenCodeInfo.TableName + " group by code_classify");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "orig_sys_code", desc = "", range = "")
    @Param(name = "code_classify", desc = "", range = "")
    @Return(desc = "", range = "")
    @PostMapping("/getOrigCodeInfoByCode")
    public Result getOrigCodeInfoByCode(String orig_sys_code, String code_classify) {
        isOrigSysoInfoExist(orig_sys_code);
        isHyrenCodeInfoExist(code_classify);
        return Dbo.queryResult("select t1.*,t2.code_classify_name,t2.code_type_name from " + OrigCodeInfo.TableName + " t1," + HyrenCodeInfo.TableName + " t2" + " where t1.code_classify=t2.code_classify AND t1.code_value=t2.code_value" + " AND t1.orig_sys_code=? AND t2.code_classify =?", orig_sys_code, code_classify);
    }
}
