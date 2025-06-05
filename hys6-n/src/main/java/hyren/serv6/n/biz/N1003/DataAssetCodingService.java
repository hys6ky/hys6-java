package hyren.serv6.n.biz.N1003;

import fd.ng.core.utils.BeanUtil;
import fd.ng.core.utils.DateUtil;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.n.bean.DataAssetCodingVo;
import hyren.serv6.n.entity.DataAssetCoding;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.stereotype.Service;
import java.util.List;

@Service
@Slf4j
public class DataAssetCodingService {

    public void addOrUpdateCoding(DataAssetCodingVo dataAssetCodingVo) {
        if (ObjectUtils.isNotEmpty(dataAssetCodingVo.getCoding_id()) && 0 != dataAssetCodingVo.getCoding_id()) {
            updateCoding(dataAssetCodingVo);
        } else {
            addCoding(dataAssetCodingVo);
        }
    }

    public List<DataAssetCoding> queryCoding(long codingId, Page page) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance().addSql("SELECT * FROM " + DataAssetCoding.TableName);
        if (ObjectUtils.isNotEmpty(codingId) && 0 != codingId) {
            assembler.addSql(" WHERE coding_id = ?").addParam(codingId);
        }
        return Dbo.queryPagedList(DataAssetCoding.class, page, assembler.sql(), assembler.params());
    }

    public List<DataAssetCoding> queryCodingByDirId(long dirId) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance().addSql("SELECT * FROM " + DataAssetCoding.TableName);
        if (ObjectUtils.isNotEmpty(dirId) && 0 != dirId) {
            assembler.addSql(" WHERE dir_id = ?").addParam(dirId);
        }
        return Dbo.queryList(DataAssetCoding.class, assembler.sql(), assembler.params());
    }

    public void deleteCodingById(long codingId) {
        Dbo.execute("delete from " + DataAssetCoding.TableName + " where coding_id = ?", codingId);
    }

    public void addCoding(DataAssetCodingVo dataAssetCodingVo) {
        DataAssetCoding dataAssetCoding = new DataAssetCoding();
        BeanUtil.copyProperties(dataAssetCodingVo, dataAssetCoding);
        dataAssetCoding.setCoding_id(PrimayKeyGener.getNextId());
        dataAssetCoding.setCreate_by(String.valueOf(UserUtil.getUserId()));
        dataAssetCoding.setCreate_date(DateUtil.getSysDate());
        dataAssetCoding.setCreate_time(DateUtil.getSysTime());
        dataAssetCoding.add(Dbo.db());
    }

    public void updateCoding(DataAssetCodingVo dataAssetCodingVo) {
        if (ObjectUtils.isEmpty(dataAssetCodingVo) || ObjectUtils.isEmpty(dataAssetCodingVo.getCoding_id()) || 0 == dataAssetCodingVo.getCoding_id()) {
            throw new SystemBusinessException("无法修改，数据为空");
        }
        List<DataAssetCoding> dataAssetCodingList = Dbo.queryList(DataAssetCoding.class, "SELECT * FROM " + DataAssetCoding.TableName + " WHERE coding_id = ?", dataAssetCodingVo.getCoding_id());
        if (ObjectUtils.isEmpty(dataAssetCodingList) || dataAssetCodingList.size() > 1) {
            throw new SystemBusinessException("无法修改，该id：{}不存在数据或者数据错误", dataAssetCodingVo.getCoding_id());
        }
        Dbo.beginTransaction();
        Dbo.execute("DELETE FROM " + DataAssetCoding.TableName + " WHERE coding_id = ?", dataAssetCodingVo.getCoding_id());
        DataAssetCoding dataAssetCoding = dataAssetCodingList.get(0);
        dataAssetCoding.setDir_id(dataAssetCodingVo.getDir_id());
        dataAssetCoding.setRule_name_lev1(dataAssetCodingVo.getRule_name_lev1());
        dataAssetCoding.setRule_code_lev1(dataAssetCodingVo.getRule_code_lev1());
        dataAssetCoding.setDigit_lev1(dataAssetCodingVo.getDigit_lev1());
        dataAssetCoding.setStart_range_lev1(dataAssetCodingVo.getStart_range_lev1());
        dataAssetCoding.setEnd_range_lev1(dataAssetCodingVo.getEnd_range_lev1());
        dataAssetCoding.setRule_name_lev2(dataAssetCodingVo.getRule_name_lev2());
        dataAssetCoding.setRule_code_lev2(dataAssetCodingVo.getRule_code_lev2());
        dataAssetCoding.setDigit_lev2(dataAssetCodingVo.getDigit_lev2());
        dataAssetCoding.setStart_range_lev2(dataAssetCodingVo.getStart_range_lev2());
        dataAssetCoding.setEnd_range_lev2(dataAssetCodingVo.getEnd_range_lev2());
        dataAssetCoding.setRule_name_lev3(dataAssetCodingVo.getRule_name_lev3());
        dataAssetCoding.setRule_code_lev3(dataAssetCodingVo.getRule_code_lev3());
        dataAssetCoding.setDigit_lev3(dataAssetCodingVo.getDigit_lev3());
        dataAssetCoding.setStart_range_lev3(dataAssetCodingVo.getStart_range_lev3());
        dataAssetCoding.setEnd_range_lev3(dataAssetCodingVo.getEnd_range_lev3());
        dataAssetCoding.setCoding_split(dataAssetCodingVo.getCoding_split());
        dataAssetCoding.setAsset_rule_name(dataAssetCodingVo.getAsset_rule_name());
        dataAssetCoding.setAsset_rule_code(dataAssetCodingVo.getAsset_rule_code());
        dataAssetCoding.setAsset_digit(dataAssetCodingVo.getAsset_digit());
        dataAssetCoding.setStart_range_asset(dataAssetCodingVo.getStart_range_asset());
        dataAssetCoding.setEnd_range_asset(dataAssetCodingVo.getEnd_range_asset());
        dataAssetCoding.add(Dbo.db());
        Dbo.commitTransaction();
    }
}
