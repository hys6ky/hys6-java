package hyren.serv6.m.dataSource;

import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.serv6.base.entity.DataStoreLayer;
import hyren.serv6.base.entity.SysPara;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.m.contants.MetaObjTypeEnum;
import hyren.serv6.m.contants.MetadataSourceEnum;
import hyren.serv6.m.entity.MetaDataSource;
import hyren.serv6.m.entity.MetaObjInfo;
import hyren.serv6.m.entity.MetaSourceObjCache;
import hyren.serv6.m.entity.MetaTask;
import hyren.serv6.m.util.IdGenerator;
import hyren.serv6.m.vo.query.MetaDataSourceQueryVo;
import hyren.serv6.m.vo.save.MetaDataSourceSaveVo;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.stereotype.Service;
import fd.ng.db.jdbc.Page;
import hyren.daos.bizpot.commons.Dbo;
import org.springframework.beans.BeanUtils;
import org.springframework.web.multipart.MultipartFile;
import java.util.*;

@Service("metaDataSourceService")
public class MetaDataSourceService {

    public MetaDataSourceQueryVo queryById(Long sourceId) {
        return queryById(sourceId, Dbo.db());
    }

    public MetaDataSourceQueryVo queryById(Long sourceId, DatabaseWrapper db) {
        return SqlOperator.queryOneObject(db, MetaDataSourceQueryVo.class, "select * from " + MetaDataSource.TableName + " where source_id=?", sourceId).orElseThrow(() -> new SystemBusinessException("数据不存在"));
    }

    public List<MetaDataSourceQueryVo> queryByPage(MetaDataSourceQueryVo metaDataSourceQueryVo, Page page) {
        List<MetaDataSourceQueryVo> metaDataSourceQueryVos = Dbo.queryPagedList(MetaDataSourceQueryVo.class, page, "select * from " + MetaDataSource.TableName);
        List<Map<String, Object>> metaObjInfoNum = Dbo.queryList("SELECT source_id,type,COUNT(TYPE) AS type_num FROM  " + MetaObjInfo.TableName + " GROUP BY TYPE,source_id");
        metaDataSourceQueryVos.forEach(metaDataSourceQuery -> {
            metaObjInfoNum.forEach(metaObjInfo -> {
                if (metaDataSourceQuery.getSource_id().equals(metaObjInfo.get("source_id"))) {
                    if (MetaObjTypeEnum.TBL.getCode().equals(metaObjInfo.get("type"))) {
                        metaDataSourceQuery.setMoiTblNum(String.valueOf(metaObjInfo.get("type_num")));
                    } else if (MetaObjTypeEnum.VIEW.getCode().equals(metaObjInfo.get("type"))) {
                        metaDataSourceQuery.setMoiViewNum(String.valueOf(metaObjInfo.get("type_num")));
                    } else if (MetaObjTypeEnum.PROC.getCode().equals(metaObjInfo.get("type"))) {
                        metaDataSourceQuery.setMoiProcNum(String.valueOf(metaObjInfo.get("type_num")));
                    } else if (MetaObjTypeEnum.METER_VIEW.getCode().equals(metaObjInfo.get("type"))) {
                        metaDataSourceQuery.setMoiMeterViewNum(String.valueOf(metaObjInfo.get("type_num")));
                    }
                }
            });
            if (metaDataSourceQuery.getMoiTblNum() == null || StringUtil.isEmpty(metaDataSourceQuery.getMoiTblNum())) {
                metaDataSourceQuery.setMoiTblNum("0");
            }
            if (metaDataSourceQuery.getMoiProcNum() == null || StringUtil.isEmpty(metaDataSourceQuery.getMoiProcNum())) {
                metaDataSourceQuery.setMoiProcNum("0");
            }
            if (metaDataSourceQuery.getMoiViewNum() == null || StringUtil.isEmpty(metaDataSourceQuery.getMoiViewNum())) {
                metaDataSourceQuery.setMoiViewNum("0");
            }
            if (metaDataSourceQuery.getMoiMeterViewNum() == null || StringUtil.isEmpty(metaDataSourceQuery.getMoiMeterViewNum())) {
                metaDataSourceQuery.setMoiMeterViewNum("0");
            }
        });
        return metaDataSourceQueryVos;
    }

    public MetaDataSource insert(MetaDataSourceSaveVo metaDataSourceSaveVo) {
        CheckMetaDataName(metaDataSourceSaveVo);
        MetaDataSource metaDataSource = new MetaDataSource();
        BeanUtils.copyProperties(metaDataSourceSaveVo, metaDataSource);
        metaDataSource.setSource_id(IdGenerator.nextId());
        metaDataSource.setCreated_id(UserUtil.getUserId());
        metaDataSource.setCreated_by(UserUtil.getUser().getRoleName());
        metaDataSource.setCreated_date(DateUtil.getSysDate());
        metaDataSource.setCreated_time(DateUtil.getSysTime());
        metaDataSource.setC_proc_num(0);
        metaDataSource.setC_tbl_num(0);
        metaDataSource.setC_view_num(0);
        metaDataSource.setC_meter_view_num(0);
        metaDataSource.setF_proc_num(0);
        metaDataSource.setF_tbl_num(0);
        metaDataSource.setF_view_num(0);
        metaDataSource.setF_meter_view_num(0);
        metaDataSource.add(Dbo.db());
        return metaDataSource;
    }

    public void CheckMetaDataName(MetaDataSourceSaveVo metaDataSourceSaveVo) {
        SqlOperator.Assembler sql = SqlOperator.Assembler.newInstance();
        sql.addSql("SELECT count(1) FROM " + MetaDataSource.TableName + " WHERE SOURCE_NAME = ?  ").addParam(metaDataSourceSaveVo.getSource_name());
        if (ObjectUtils.isNotEmpty(metaDataSourceSaveVo.getSource_id())) {
            sql.addSql(" AND SOURCE_ID != ?").addParam(metaDataSourceSaveVo.getSource_id());
        }
        long l = Dbo.queryNumber(sql.sql(), sql.params()).orElse(0);
        if (l > 0) {
            throw new BusinessException(String.format("【%s】数据源名称出现重复", metaDataSourceSaveVo.getSource_name()));
        }
    }

    public MetaDataSource update(MetaDataSourceSaveVo metaDataSourceSaveVo) {
        CheckMetaDataName(metaDataSourceSaveVo);
        MetaDataSourceQueryVo queryVo = queryById(metaDataSourceSaveVo.getSource_id());
        MetaDataSource metaDataSource = new MetaDataSource();
        BeanUtils.copyProperties(queryVo, metaDataSource);
        BeanUtils.copyProperties(metaDataSourceSaveVo, metaDataSource);
        metaDataSource.setUpdated_id(UserUtil.getUserId());
        metaDataSource.setUpdated_by(UserUtil.getUser().getRoleName());
        metaDataSource.setUpdated_date(DateUtil.getSysDate());
        metaDataSource.setUpdated_time(DateUtil.getSysTime());
        metaDataSource.update(Dbo.db());
        Dbo.commitTransaction();
        return metaDataSource;
    }

    public boolean deleteById(Long sourceId) {
        MetaDataSource metaDataSource = new MetaDataSource();
        Long num = Dbo.queryNumber("select count(1) from " + MetaTask.TableName + " where source_id = ?", sourceId).orElse(0L);
        if (num != 0) {
            throw new BusinessException("元系统下存在任务，不能进行删除操作");
        }
        metaDataSource.setSource_id(sourceId);
        metaDataSource.delete(Dbo.db());
        Dbo.commitTransaction();
        return true;
    }

    public MetaDataSource updateFormalObjNum(Long sourceId) {
        return updateFormalObjNum(sourceId, Dbo.db());
    }

    public MetaDataSource updateFormalObjNum(Long sourceId, DatabaseWrapper db) {
        MetaDataSourceQueryVo queryVo = queryById(sourceId, db);
        MetaDataSource metaDataSource = new MetaDataSource();
        BeanUtils.copyProperties(queryVo, metaDataSource);
        long tblCount = SqlOperator.queryNumber(db, "select count(*) from " + MetaObjInfo.TableName + " where source_id=? and type=? ", sourceId, MetaObjTypeEnum.TBL.getCode()).orElse(0);
        long viewCount = SqlOperator.queryNumber(db, "select count(*) from " + MetaObjInfo.TableName + " where source_id=? and type=? ", sourceId, MetaObjTypeEnum.VIEW.getCode()).orElse(0);
        long meterViewCount = SqlOperator.queryNumber(db, "select count(*) from " + MetaObjInfo.TableName + " where source_id=? and type=? ", sourceId, MetaObjTypeEnum.METER_VIEW.getCode()).orElse(0);
        long procCount = SqlOperator.queryNumber(db, "select count(*) from " + MetaObjInfo.TableName + " where source_id=? and type=? ", sourceId, MetaObjTypeEnum.PROC.getCode()).orElse(0);
        metaDataSource.setF_tbl_num(Math.toIntExact(tblCount));
        metaDataSource.setF_view_num(Math.toIntExact(viewCount));
        metaDataSource.setF_meter_view_num(Math.toIntExact(meterViewCount));
        metaDataSource.setF_proc_num(Math.toIntExact(procCount));
        metaDataSource.update(db);
        return metaDataSource;
    }

    public MetaDataSource updateCacheObjNum(Long sourceId) {
        MetaDataSourceQueryVo queryVo = queryById(sourceId);
        MetaDataSource metaDataSource = new MetaDataSource();
        BeanUtils.copyProperties(queryVo, metaDataSource);
        long tblCount = Dbo.queryNumber("select count(*) from " + MetaSourceObjCache.TableName + " where source_id=? and type=? and is_col='1' ", sourceId, MetaObjTypeEnum.TBL.getCode()).orElse(0);
        long viewCount = Dbo.queryNumber("select count(*) from " + MetaSourceObjCache.TableName + " where source_id=? and type=? and is_col='1' ", sourceId, MetaObjTypeEnum.VIEW.getCode()).orElse(0);
        long meterViewCount = Dbo.queryNumber("select count(*) from " + MetaSourceObjCache.TableName + " where source_id=? and type=? and is_col='1' ", sourceId, MetaObjTypeEnum.METER_VIEW.getCode()).orElse(0);
        long procCount = Dbo.queryNumber("select count(*) from " + MetaSourceObjCache.TableName + " where source_id=? and type=? and is_col='1' ", sourceId, MetaObjTypeEnum.PROC.getCode()).orElse(0);
        metaDataSource.setF_tbl_num(Math.toIntExact(tblCount));
        metaDataSource.setF_view_num(Math.toIntExact(viewCount));
        metaDataSource.setF_meter_view_num(Math.toIntExact(meterViewCount));
        metaDataSource.setF_proc_num(Math.toIntExact(procCount));
        metaDataSource.update(Dbo.db());
        return metaDataSource;
    }

    public Map<String, String> getSysPara(String paraType) {
        List<SysPara> sysParas = Dbo.queryList(SysPara.class, "select * from " + SysPara.TableName + " where para_type = ?", paraType);
        Map<String, String> map = new HashMap<>();
        sysParas.forEach(para -> {
            map.put(para.getPara_name(), para.getPara_value());
        });
        return map;
    }

    public String getDslName(Long dslId) {
        Optional<DataStoreLayer> dataStoreLayer = Dbo.queryOneObject(DataStoreLayer.class, "SELECT dsl_name FROM " + DataStoreLayer.TableName + " where dsl_id = ?", dslId);
        if (dataStoreLayer.isPresent()) {
            return dataStoreLayer.get().getDsl_name();
        } else {
            return null;
        }
    }
}
