package hyren.serv6.n.biz.N1002;

import fd.ng.core.utils.BeanUtil;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.n.bean.*;
import hyren.serv6.n.entity.*;
import hyren.serv6.n.enums.AssetStatus;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class DataAssetRegistService {

    public void addDirAndAssetRel(DataAssetDirRelBean dataAssetDirRelBean) {
        if (ObjectUtils.isEmpty(dataAssetDirRelBean.getDirId()) && 0 == dataAssetDirRelBean.getDirId()) {
            throw new SystemBusinessException("登记失败，请选择一个目录");
        }
        if (ObjectUtils.isEmpty(dataAssetDirRelBean.getDataAssetRegistVoList())) {
            throw new SystemBusinessException("登记失败，请选择资产");
        }
        DataAssetDir dataAssetDir = Dbo.queryOneObject(DataAssetDir.class, "SELECT * FROM " + DataAssetDir.TableName + " WHERE dir_id = ?", dataAssetDirRelBean.getDirId()).orElseThrow(() -> new SystemBusinessException("选择的目录不存在"));
        Dbo.beginTransaction();
        Dbo.execute("DELETE FROM " + DataAssetDirRel.TableName + " WHERE dir_id = ?", dataAssetDir.getDir_id());
        dataAssetDirRelBean.getDataAssetRegistVoList().forEach(e -> {
            if (StringUtil.isBlank(e.getMdata_table_id())) {
                throw new SystemBusinessException("登记失败，缺少元数据业务主键：" + e.getAsset_cname());
            }
            if (StringUtil.isBlank(e.getAsset_ename())) {
                throw new SystemBusinessException("登记失败，缺少资产英文名称：" + e.getAsset_cname());
            }
            long assetId;
            DataAssetRegist dataAssetRegistCheck = Dbo.queryOneObject(DataAssetRegist.class, "SELECT * FROM " + DataAssetRegist.TableName + " WHERE mdata_table_id = ? and asset_ename = ?", e.getMdata_table_id(), e.getAsset_ename()).orElse(null);
            if (ObjectUtils.isEmpty(dataAssetRegistCheck)) {
                DataAssetRegist dataAssetRegist = new DataAssetRegist();
                dataAssetRegist.setAsset_id(PrimayKeyGener.getNextId());
                dataAssetRegist.setMdata_table_id(e.getMdata_table_id());
                dataAssetRegist.setAsset_type(e.getAsset_type());
                dataAssetRegist.setAsset_cname(e.getAsset_cname());
                dataAssetRegist.setAsset_ename(e.getAsset_ename());
                dataAssetRegist.setAsset_status(AssetStatus.DAIPANDIAN.getCode());
                dataAssetRegist.setCreate_date(DateUtil.getSysDate());
                dataAssetRegist.setCreate_time(DateUtil.getSysTime());
                dataAssetRegist.add(Dbo.db());
                assetId = dataAssetRegist.getAsset_id();
            } else {
                assetId = dataAssetRegistCheck.getAsset_id();
            }
            DataAssetDirRel dataAssetDirRel = new DataAssetDirRel();
            dataAssetDirRel.setRel_id(PrimayKeyGener.getNextId());
            dataAssetDirRel.setDir_id(dataAssetDir.getDir_id());
            dataAssetDirRel.setAsset_id(assetId);
            dataAssetDirRel.add(Dbo.db());
        });
        Dbo.commitTransaction();
    }

    public void updateDirAndAssetRel(DataAssetDirRelBean dataAssetDirRelBean) {
        if (ObjectUtils.isNotEmpty(dataAssetDirRelBean.getDirId()) && 0 != dataAssetDirRelBean.getDirId()) {
            throw new SystemBusinessException("修改失败，请选择一个目录");
        }
        Dbo.queryOneObject(DataAssetDir.class, "SELECT * FROM " + DataAssetDir.TableName + " WHERE dir_id = ?", dataAssetDirRelBean.getDirId()).orElseThrow(() -> new SystemBusinessException("选择的目录不存在"));
        if (ObjectUtils.isEmpty(dataAssetDirRelBean.getDataAssetRegistVoList())) {
            throw new SystemBusinessException("登记失败，请选择资产");
        }
        Dbo.beginTransaction();
        dataAssetDirRelBean.getDataAssetRegistVoList().forEach(e -> {
            Dbo.execute("update " + DataAssetDirRel.TableName + " set dir_id = ? where asset_id = ?", dataAssetDirRelBean.getDirId(), e.getAsset_id());
        });
        Dbo.commitTransaction();
    }

    public List<MetaDataObjDto> findMetaDataObj(long sourceId, String type) {
        String[] types = type.split(",");
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance().addSql("select cast(T1.obj_id as varchar) AS objId, T1.en_name as enName, T1.ch_name as chName, " + "(case when T1.type = '0' then '1' when T1.type = '1' then '2' " + "when T1.type = '3' then '2' else '3' end) AS type, T2.dsl_id AS layer, " + "(case when T3.asset_id is not null then '1' else '0' end) isConfig, " + "T2.source_id, T2.source_name " + "from meta_obj_info T1 " + "left join meta_data_source T2 on T1.source_id = T2.source_id " + "left join " + DataAssetRegist.TableName + " T3 " + "on cast(T1.obj_id as varchar) = T3.mdata_table_id " + "where T1.type != '2'");
        if (ObjectUtils.isNotEmpty(sourceId) && 0 != sourceId) {
            assembler.addSql(" AND T2.source_id = ? ").addParam(sourceId);
            if (StringUtil.isNotBlank(types[0])) {
                assembler.addORParam("T1.TYPE", types);
            }
        }
        if ((ObjectUtils.isEmpty(sourceId) || 0 == sourceId) && StringUtil.isNotBlank(types[0])) {
            assembler.addSql(" AND T1.en_name is not null ");
            assembler.addORParam("T1.TYPE", types);
        }
        return Dbo.queryList(MetaDataObjDto.class, assembler.sql(), assembler.params());
    }

    public List<MetaDataSourceDto> findMetaDataSource() {
        return Dbo.queryList(MetaDataSourceDto.class, "select distinct source_name, source_id from meta_data_source");
    }

    public List<MetaDataColumnDto> findMetaDataColumn(String objId) {
        return Dbo.queryList(MetaDataColumnDto.class, "select cast(T1.DTL_ID as varchar) as mdata_col_id, " + "T1.COL_EN_NAME as col_ename, T1.COL_CH_NAME as col_cname, " + "T1.col_type, T1.col_len, T1.col_prec, " + "T1.BIZ_DESC as col_business, T1.IS_PRI_KEY, T1.IS_NULL " + "from META_OBJ_TBL_COL T1 " + "where cast(T1.obj_id as varchar) = ? order by dtl_id asc", objId);
    }

    public void deleteDataAsset(Long[] assetIds) {
        Dbo.beginTransaction();
        for (long assetId : assetIds) {
            DataAssetRegist dataAssetRegist = Dbo.queryOneObject(DataAssetRegist.class, "SELECT * FROM " + DataAssetRegist.TableName + " WHERE asset_id = ?", assetId).orElseThrow(() -> new SystemBusinessException("选择的资产不存在"));
            if (AssetStatus.YIWANCHENGDENGJI.getCode().equals(dataAssetRegist.getAsset_status())) {
                throw new SystemBusinessException("已完成登记的资产不能删除");
            }
            Dbo.execute("DELETE FROM " + DataAssetColumn.TableName + " WHERE asset_id = ?", assetId);
            Dbo.execute("DELETE FROM " + DataAssetRegist.TableName + " WHERE asset_id = ?", assetId);
            Dbo.execute("DELETE FROM " + DataAssetDirRel.TableName + " WHERE asset_id = ?", assetId);
        }
        Dbo.commitTransaction();
    }

    public List<DataAssetRegistVo> queryByDirId(long dirId) {
        return Dbo.queryList(DataAssetRegistVo.class, "select T1.*, T2.dir_id from " + DataAssetRegist.TableName + " T1 LEFT JOIN " + DataAssetDirRel.TableName + " T2 ON T1.asset_id = T2.asset_id where T2.dir_id = ?", dirId);
    }

    public List<DataAssetRegistVo> queryByMdataId(Long[] mDataIds) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance().addSql("select T1.*, T2.dir_id from " + DataAssetRegist.TableName + " T1 LEFT JOIN " + DataAssetDirRel.TableName + " T2 ON T1.asset_id = T2.asset_id where T1.asset_id is not null ");
        List<String> mDataIdStrs = new ArrayList<>();
        for (long id : mDataIds) {
            mDataIdStrs.add(String.valueOf(id));
        }
        assembler.addORParam("T1.mdata_table_id", mDataIdStrs);
        return Dbo.queryList(DataAssetRegistVo.class, assembler.sql(), assembler.params());
    }

    public void registDataAsset(DataAssetRegistBean dataAssetRegistBean) {
        if (ObjectUtils.isEmpty(dataAssetRegistBean)) {
            throw new SystemBusinessException("登记失败，请选择一个资产");
        }
        if (AssetStatus.YIWANCHENGDENGJI.getCode().equals(dataAssetRegistBean.getDataAssetRegistVo().getAsset_status())) {
            for (DataAssetColumnVo dataAssetColumnVo : dataAssetRegistBean.getDataAssetColumnVo()) {
                if (StringUtil.isBlank(dataAssetColumnVo.getNorm_col_ename())) {
                    throw new SystemBusinessException("完成登记失败，字段缺少标准：" + dataAssetColumnVo.getCol_ename());
                }
            }
        }
        Dbo.beginTransaction();
        boolean isHis = false;
        if (ObjectUtils.isNotEmpty(dataAssetRegistBean.getDataAssetRegistVo())) {
            isHis = updateDataAsset(dataAssetRegistBean.getDataAssetRegistVo(), dataAssetRegistBean.getTaskId());
            if (ObjectUtils.isNotEmpty(dataAssetRegistBean.getDataAssetColumnVo())) {
                updateDataAssetColumn(dataAssetRegistBean.getDataAssetRegistVo().getAsset_id(), dataAssetRegistBean.getDataAssetColumnVo(), isHis);
            }
        }
        if (ObjectUtils.isNotEmpty(dataAssetRegistBean.getDataAssetEnumVoList())) {
            updateDataAssetEnum(dataAssetRegistBean.getDataAssetEnumVoList(), isHis);
        }
        Dbo.commitTransaction();
    }

    public List<DataAssetDepartDto> findDataAssetDepart() {
        return Dbo.queryList(DataAssetDepartDto.class, "select distinct T1.dep_id, T1.dep_name from department_info T1 JOIN " + DataAssetRegist.TableName + " T2 ON " + "cast(T1.dep_id as varchar) = T2.belong_depart or T1.dep_name = T2.belong_depart ");
    }

    public List<DataAssetRegist> findDataAssetByDepart(long depId, long catalogId, Page page) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance().addSql("SELECT * FROM " + DataAssetRegist.TableName);
        if (0 != depId) {
            assembler.addSql(" WHERE belong_depart = ? ").addParam(String.valueOf(depId));
        }
        if (0 != depId && 0 != catalogId) {
            assembler.addSql(" AND asset_id in ( " + "select T2.asset_id from " + DataAssetDir.TableName + " T1 JOIN " + DataAssetDirRel.TableName + " T2 ON T1.dir_id = T2.dir_id where T1.catalog_id = ? )").addParam(catalogId);
        }
        if (0 == depId && 0 != catalogId) {
            assembler.addSql(" WHERE asset_id in ( " + "select T2.asset_id from " + DataAssetDir.TableName + " T1 JOIN " + DataAssetDirRel.TableName + " T2 ON T1.dir_id = T2.dir_id where T1.catalog_id = ? )").addParam(catalogId);
        }
        return Dbo.queryPagedList(DataAssetRegist.class, page, assembler.sql(), assembler.params());
    }

    public List<DataAssetRegistVo> findDataAsset(long assetId, String assetCode, String assetName, Page page) {
        SqlOperator.Assembler assembler = findDataAsset(assetId, assetCode, assetName, false);
        return Dbo.queryPagedList(DataAssetRegistVo.class, page, assembler.sql(), assembler.params());
    }

    public List<DataAssetRegistVo> findDataAssetHis(long assetId, String assetCode, String assetName, Page page) {
        SqlOperator.Assembler assembler = findDataAsset(assetId, assetCode, assetName, true);
        return Dbo.queryPagedList(DataAssetRegistVo.class, page, assembler.sql(), assembler.params());
    }

    private SqlOperator.Assembler findDataAsset(long assetId, String assetCode, String assetName, boolean isHis) {
        String tableName = isHis ? DataAssetRegistHis.TableName : DataAssetRegist.TableName;
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance().addSql("SELECT T1.* FROM " + tableName + " T1 ");
        if (0 != assetId) {
            assembler.addSql(" WHERE T1.asset_id = ?").addParam(assetId);
        }
        if (0 != assetId && StringUtil.isNotBlank(assetCode)) {
            assetCode = "%" + assetCode + "%";
            assembler.addSql(" AND T1.asset_code like ?").addParam(assetCode);
        }
        if (0 == assetId && StringUtil.isNotBlank(assetCode)) {
            assetCode = "%" + assetCode + "%";
            assembler.addSql(" WHERE T1.asset_code like ?").addParam(assetCode);
        }
        if ((0 != assetId || ObjectUtils.isNotEmpty(assetCode)) && StringUtil.isNotBlank(assetName)) {
            assetName = "%" + assetName + "%";
            assembler.addSql(" AND (T1.asset_ename like ? or T1.asset_cname like ?)").addParam(assetName).addParam(assetName);
        }
        if (0 == assetId && ObjectUtils.isEmpty(assetCode) && StringUtil.isNotBlank(assetName)) {
            assetName = "%" + assetName + "%";
            assembler.addSql(" WHERE T1.asset_ename like ? or T1.asset_cname like ?").addParam(assetName).addParam(assetName);
        }
        return assembler;
    }

    public DataAssetColumn findDataAssetColumnById(long colId) {
        return Dbo.queryOneObject(DataAssetColumn.class, "SELECT * FROM " + DataAssetColumn.TableName + " WHERE col_id = ?", colId).orElseThrow(() -> new SystemBusinessException("查询的字段不存在"));
    }

    public List<DataAssetColumn> findDataAssetColumn(long assetId, String colName, Page page) {
        SqlOperator.Assembler assembler = findDataAssetColumn(assetId, colName, false);
        return Dbo.queryPagedList(DataAssetColumn.class, page, assembler.sql(), assembler.params());
    }

    public List<DataAssetColumnHis> findDataAssetColumnHis(long assetId, String colName, Page page) {
        SqlOperator.Assembler assembler = findDataAssetColumn(assetId, colName, true);
        return Dbo.queryPagedList(DataAssetColumnHis.class, page, assembler.sql(), assembler.params());
    }

    private SqlOperator.Assembler findDataAssetColumn(long assetId, String colName, boolean isHis) {
        String tableName = isHis ? DataAssetColumnHis.TableName : DataAssetColumn.TableName;
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance().addSql("SELECT * FROM " + tableName);
        if (0 != assetId) {
            assembler.addSql(" WHERE asset_id = ?").addParam(assetId);
        }
        if (0 != assetId && StringUtil.isNotBlank(colName)) {
            assembler.addSql(" AND (col_ename = ? or col_cname = ?)").addParam(colName).addParam(colName);
        }
        if (0 == assetId && StringUtil.isNotBlank(colName)) {
            assembler.addSql(" WHERE col_ename = ? or col_cname = ?").addParam(colName).addParam(colName);
        }
        return assembler;
    }

    public List<DataAssetEnum> findDataAssetEnum(Page page) {
        return Dbo.queryPagedList(DataAssetEnum.class, page, "SELECT distinct enum_ename, enum_cname FROM " + DataAssetEnum.TableName);
    }

    public List<DataAssetEnumHis> findDataAssetEnumHis(Page page) {
        return Dbo.queryPagedList(DataAssetEnumHis.class, page, "SELECT distinct enum_ename, enum_cname FROM " + DataAssetEnumHis.TableName);
    }

    public List<DataAssetEnum> findDataAssetEnumDetail(String enumEname, Page page) {
        SqlOperator.Assembler assembler = findDataAssetEnumDetail(enumEname, false);
        return Dbo.queryPagedList(DataAssetEnum.class, page, assembler.sql(), assembler.params());
    }

    public List<DataAssetEnumHis> findDataAssetEnumDetailHis(String enumEname, Page page) {
        SqlOperator.Assembler assembler = findDataAssetEnumDetail(enumEname, true);
        return Dbo.queryPagedList(DataAssetEnumHis.class, page, assembler.sql(), assembler.params());
    }

    private SqlOperator.Assembler findDataAssetEnumDetail(String enumEname, boolean isHis) {
        String tableName = isHis ? DataAssetEnumHis.TableName : DataAssetEnum.TableName;
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance().addSql("SELECT * FROM " + tableName);
        if (StringUtil.isNotEmpty(enumEname)) {
            assembler.addSql(" WHERE enum_ename = ?").addParam(enumEname);
        }
        return assembler;
    }

    private void updateDataAssetEnum(List<DataAssetEnumVo> dataAssetEnumVoList, boolean isHis) {
        List<String> checkEnumEname = new ArrayList<>();
        dataAssetEnumVoList.forEach(e -> {
            String enumEname = e.getEnum_ename().toUpperCase();
            if (!checkEnumEname.contains(enumEname)) {
                if (isHis) {
                    Dbo.execute("insert into " + DataAssetEnumHis.TableName + " select * from " + DataAssetEnum.TableName + " WHERE upper(enum_ename) = ?", enumEname);
                }
                Dbo.execute("DELETE FROM " + DataAssetEnum.TableName + " WHERE upper(enum_ename) = ?", enumEname);
                checkEnumEname.add(enumEname);
            }
            DataAssetEnum dataAssetEnum = new DataAssetEnum();
            dataAssetEnum.setEnum_id(e.getEnum_id());
            dataAssetEnum.setEnum_ename(e.getEnum_ename());
            dataAssetEnum.setEnum_cname(e.getEnum_cname());
            dataAssetEnum.setItem_ename(e.getItem_ename());
            dataAssetEnum.setItem_cname(e.getItem_cname());
            dataAssetEnum.setItem_value(e.getItem_value());
            if (ObjectUtils.isEmpty(e.getEnum_id()) || 0 == e.getEnum_id()) {
                dataAssetEnum.setEnum_id(PrimayKeyGener.getNextId());
            }
            dataAssetEnum.add(Dbo.db());
        });
    }

    private void updateDataAssetColumn(long assetId, List<DataAssetColumnVo> dataAssetColumnVo, boolean isHis) {
        if (ObjectUtils.isEmpty(dataAssetColumnVo) || ObjectUtils.isEmpty(assetId)) {
            return;
        }
        if (isHis) {
            Dbo.execute("insert into " + DataAssetColumnHis.TableName + " select * from " + DataAssetColumn.TableName + " where asset_id = ?", assetId);
        }
        Dbo.execute("DELETE FROM " + DataAssetColumn.TableName + " WHERE asset_id = ?", assetId);
        dataAssetColumnVo.forEach(e -> {
            if (ObjectUtils.isEmpty(e.getCol_id()) || 0 == e.getCol_id()) {
                e.setCol_id(PrimayKeyGener.getNextId());
            }
            DataAssetColumn dataAssetColumn = new DataAssetColumn();
            BeanUtil.copyProperties(e, dataAssetColumn);
            dataAssetColumn.setAsset_id(assetId);
            dataAssetColumn.setUpdate_date(DateUtil.getSysDate());
            dataAssetColumn.setUpdate_time(DateUtil.getSysTime());
            dataAssetColumn.add(Dbo.db());
        });
    }

    private boolean updateDataAsset(DataAssetRegistVo dataAssetRegistVo, long taskId) {
        boolean isHis = false;
        DataAssetRegist dataAssetRegist;
        if (ObjectUtils.isNotEmpty(dataAssetRegistVo.getAsset_id()) && 0 != dataAssetRegistVo.getAsset_id()) {
            dataAssetRegist = Dbo.queryOneObject(DataAssetRegist.class, "SELECT * FROM " + DataAssetRegist.TableName + " WHERE asset_id = ?", dataAssetRegistVo.getAsset_id()).orElseThrow(() -> new SystemBusinessException("该资产不存在" + dataAssetRegistVo.getAsset_id()));
            if (AssetStatus.YIWANCHENGDENGJI.getCode().equals(dataAssetRegist.getAsset_status())) {
                isHis = true;
                Dbo.execute("insert into " + DataAssetRegistHis.TableName + " select * from " + DataAssetRegist.TableName + " where asset_id = ?", dataAssetRegist.getAsset_id());
                dataAssetRegist.setAsset_status(AssetStatus.DAIPANDIAN.getCode());
            }
            Dbo.execute("DELETE FROM " + DataAssetRegist.TableName + " WHERE asset_id = ?", dataAssetRegistVo.getAsset_id());
        } else {
            dataAssetRegist = new DataAssetRegist();
            dataAssetRegist.setAsset_id(PrimayKeyGener.getNextId());
            dataAssetRegist.setMdata_table_id(dataAssetRegistVo.getMdata_table_id());
            dataAssetRegist.setAsset_ename(dataAssetRegistVo.getAsset_ename());
            dataAssetRegist.setAsset_status(AssetStatus.DAIPANDIAN.getCode());
            dataAssetRegist.setCreate_date(DateUtil.getSysDate());
            dataAssetRegist.setCreate_time(DateUtil.getSysTime());
        }
        dataAssetRegist.setAsset_code(dataAssetRegistVo.getAsset_code());
        dataAssetRegist.setAsset_cname(dataAssetRegistVo.getAsset_cname());
        dataAssetRegist.setAsset_type(dataAssetRegistVo.getAsset_type());
        dataAssetRegist.setBusiness_pk(dataAssetRegistVo.getBusiness_pk());
        dataAssetRegist.setBusiness_cname(dataAssetRegistVo.getBusiness_cname());
        dataAssetRegist.setData_source_type(dataAssetRegistVo.getData_source_type());
        dataAssetRegist.setTheme(dataAssetRegistVo.getTheme());
        dataAssetRegist.setBusiness_remark(dataAssetRegistVo.getBusiness_remark());
        dataAssetRegist.setLayer(dataAssetRegistVo.getLayer());
        dataAssetRegist.setStore_path(dataAssetRegistVo.getStore_path());
        dataAssetRegist.setProcess_frequen(dataAssetRegistVo.getProcess_frequen());
        dataAssetRegist.setProcess_rule(dataAssetRegistVo.getProcess_rule());
        dataAssetRegist.setTech_pk(dataAssetRegistVo.getTech_pk());
        dataAssetRegist.setTech_cname(dataAssetRegistVo.getTech_cname());
        dataAssetRegist.setData_auth_code(dataAssetRegistVo.getData_auth_code());
        dataAssetRegist.setBelong_depart(dataAssetRegistVo.getBelong_depart());
        dataAssetRegist.setBelong_by(dataAssetRegistVo.getBelong_by());
        dataAssetRegist.setManage_depart(dataAssetRegistVo.getManage_depart());
        dataAssetRegist.setManage_by(dataAssetRegistVo.getManage_by());
        if (StringUtil.isNotBlank(dataAssetRegistVo.getAsset_status())) {
            dataAssetRegist.setAsset_status(dataAssetRegistVo.getAsset_status());
        }
        dataAssetRegist.setAsset_by(String.valueOf(UserUtil.getUserId()));
        dataAssetRegist.setAsset_date(DateUtil.getSysDate());
        dataAssetRegist.setAsset_time(DateUtil.getSysTime());
        dataAssetRegist.setIs_master_data(dataAssetRegistVo.getIs_master_data());
        dataAssetRegist.setData_num(dataAssetRegistVo.getData_num());
        dataAssetRegist.add(Dbo.db());
        BeanUtil.copyProperties(dataAssetRegist, dataAssetRegistVo);
        if (StringUtil.isNotBlank(dataAssetRegistVo.getLayer())) {
            if (ObjectUtils.isNotEmpty(dataAssetRegistVo.getAsset_id()) && 0 != dataAssetRegistVo.getAsset_id()) {
                List<DataAssetDirRel> dataAssetDirRels = Dbo.queryList(DataAssetDirRel.class, "SELECT * FROM " + DataAssetDirRel.TableName + " WHERE asset_id = ?", dataAssetRegist.getAsset_id());
                if (ObjectUtils.isNotEmpty(dataAssetDirRels)) {
                    Dbo.execute("DELETE FROM " + DataAssetDirRel.TableName + " WHERE asset_id = ?", dataAssetRegist.getAsset_id());
                }
            }
            String[] dirIds = dataAssetRegistVo.getLayer().split(",");
            for (String dirId : dirIds) {
                DataAssetDirRel dataAssetDirRel = new DataAssetDirRel();
                dataAssetDirRel.setRel_id(PrimayKeyGener.getNextId());
                dataAssetDirRel.setAsset_id(dataAssetRegistVo.getAsset_id());
                dataAssetDirRel.setDir_id(Long.parseLong(dirId));
                dataAssetDirRel.setTask_id(taskId);
                dataAssetDirRel.add(Dbo.db());
            }
        }
        return isHis;
    }
}
