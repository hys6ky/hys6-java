package hyren.serv6.b.realtimecollection.realtimeCollectManagement.sdmdatasource;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.AgentInfo;
import hyren.serv6.base.entity.DataSource;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.utils.regular.RegexConstant;
import hyren.serv6.commons.utils.DboExecute;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static hyren.serv6.base.user.UserUtil.getUserId;

@DocClass(desc = "", author = "yec", createdate = "2021-03-26")
@Service
@Slf4j
public class SdmDataSourceService {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result searchSdmDataSourceAndSdmAgentCount() {
        Result dsResult = Dbo.queryResult("SELECT source_id,datasource_name,datasource_number,source_remark " + " FROM " + DataSource.TableName + " WHERE create_user_id=?", getUserId());
        if (!dsResult.isEmpty()) {
            for (int i = 0; i < dsResult.getRowCount(); i++) {
                long number = Dbo.queryNumber("select count(*) from " + AgentInfo.TableName + " where source_id=?", dsResult.getLong(i, "source_id")).orElseThrow(() -> new BusinessException("sql查询错误！"));
                dsResult.setObject(i, "sumSdmAgent", number);
            }
        }
        return dsResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdmDataSource", desc = "", range = "", isBean = true)
    public void saveSdmDataSource(DataSource sdmDataSource) {
        fieldLegalityValidation(sdmDataSource.getDatasource_name(), sdmDataSource.getDatasource_number());
        isExistDataSourceNumber(sdmDataSource.getDatasource_number());
        isExistDataSourceName(sdmDataSource.getDatasource_name());
        sdmDataSource.setSource_id(PrimayKeyGener.getNextId());
        sdmDataSource.setCreate_user_id(getUserId());
        sdmDataSource.setCreate_date(DateUtil.getSysDate());
        sdmDataSource.setCreate_time(DateUtil.getSysTime());
        sdmDataSource.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_source_id", desc = "", range = "")
    public void deleteSdmDataSource(long sdm_source_id) {
        if (Dbo.queryNumber("SELECT count(1) FROM " + AgentInfo.TableName + " WHERE source_id=? and user_id=?", sdm_source_id, getUserId()).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
            throw new BusinessException("此流数据管理源下还有agent，不能删除！source_id=" + sdm_source_id);
        }
        DboExecute.deletesOrThrow("删除数据源信息表失败，source_id=" + sdm_source_id, "delete from " + DataSource.TableName + " where source_id=? and user_id=?", sdm_source_id, getUserId());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_source_id", desc = "", range = "")
    @Param(name = "sdm_source_des", desc = "", range = "", nullable = true)
    @Param(name = "sdm_source_name", desc = "", range = "")
    @Param(name = "sdm_source_number", desc = "", range = "")
    public void updateSdmDataSource(long sdm_source_id, String sdm_source_des, String sdm_source_name, String sdm_source_number) {
        fieldLegalityValidation(sdm_source_name, sdm_source_number);
        if (Dbo.queryNumber("select count(1) from " + DataSource.TableName + " where source_id=? and user_id=?", sdm_source_id, getUserId()).orElseThrow(() -> new BusinessException("sql查询错误！")) == 0) {
            throw new BusinessException("该用户对应数据源不存在！");
        }
        DataSource sdm_data_source = Dbo.queryOneObject(DataSource.class, "select * from " + DataSource.TableName + " where source_id = ?", sdm_source_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
        if (!sdm_source_number.equals(sdm_data_source.getDatasource_number())) {
            throw new BusinessException("编辑时数据源编号不能被修改");
        }
        if (sdm_data_source.getDatasource_name().equals(sdm_source_name)) {
            Dbo.execute("update " + DataSource.TableName + " set source_remark = ? where source_id = ?", sdm_source_des, sdm_source_id);
        } else {
            isExistDataSourceName(sdm_source_name);
            Dbo.execute("update " + DataSource.TableName + " set source_remark = ?,datasource_name = ? " + " where source_id=?", sdm_source_des, sdm_source_name, sdm_source_id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_source_name", desc = "", range = "")
    @Param(name = "sdm_source_number", desc = "", range = "")
    private void fieldLegalityValidation(String sdm_source_name, String sdm_source_number) {
        Validator.notBlank(sdm_source_name, "数据源名称不能为空以及不能为空格");
        Matcher matcher = Pattern.compile(RegexConstant.FRONT_LETTER).matcher(sdm_source_number);
        if (StringUtil.isBlank(sdm_source_number) || !matcher.matches()) {
            throw new BusinessException("数据源编号只能是以字母开头的数字、26个英文字母或者下划线组成的字符串:" + sdm_source_number);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_source_number", desc = "", range = "")
    private void isExistDataSourceNumber(String sdm_source_number) {
        if (Dbo.queryNumber("select count(1) from " + DataSource.TableName + " where datasource_number=? and user_id=?", sdm_source_number, getUserId()).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
            throw new BusinessException("数据源编号已存在:" + sdm_source_number);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_source_name", desc = "", range = "")
    private void isExistDataSourceName(String sdm_source_name) {
        if (Dbo.queryNumber("select count(1) from " + DataSource.TableName + " where datasource_name=? and user_id=?", sdm_source_name, getUserId()).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
            throw new BusinessException("数据源名称已存在:" + sdm_source_name);
        }
    }
}
