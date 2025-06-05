package hyren.serv6.n.biz.N1001;

import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.n.bean.DataAssetCatalogVo;
import hyren.serv6.n.bean.DataAssetDirDto;
import hyren.serv6.n.bean.DataAssetDirTreeDto;
import hyren.serv6.n.bean.DataAssetDirVo;
import hyren.serv6.n.entity.*;
import hyren.serv6.n.enums.ChangeStatus;
import hyren.serv6.n.enums.PublishStatus;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.stereotype.Service;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class DataAssetDirService {

    public void addOrUpdateDir(DataAssetDirVo dataAssetDirVo) {
        if (0 != dataAssetDirVo.getParent_id() && dataAssetDirVo.getDir_id() == dataAssetDirVo.getParent_id()) {
            throw new SystemBusinessException("操作失败，上级目录不能为自身");
        }
        if (ObjectUtils.isNotEmpty(dataAssetDirVo.getDir_id()) && 0 != dataAssetDirVo.getDir_id()) {
            updateDir(dataAssetDirVo);
        } else {
            addDir(dataAssetDirVo);
        }
    }

    public void deleteDir(long dirId, String isDelCoding) {
        if (ObjectUtils.isEmpty(dirId) || 0 == dirId) {
            throw new SystemBusinessException("无法删除，数据为空");
        }
        List<DataAssetDir> dataAssetDirList = Dbo.queryList(DataAssetDir.class, "SELECT * FROM " + DataAssetDir.TableName + " WHERE parent_id = ?", dirId);
        if (ObjectUtils.isNotEmpty(dataAssetDirList)) {
            throw new SystemBusinessException("无法删除，该目录下存在数据{}", dataAssetDirList.stream().map(DataAssetDir::getDir_name).collect(Collectors.joining(",")));
        }
        Dbo.beginTransaction();
        Dbo.execute("DELETE FROM " + DataAssetDir.TableName + " WHERE dir_id = ?", dirId);
        Dbo.execute("DELETE FROM " + DataAssetDirRel.TableName + " WHERE dir_id = ?", dirId);
        if (IsFlag.Shi.getCode().equals(isDelCoding)) {
            Dbo.execute("DELETE FROM " + DataAssetCoding.TableName + " WHERE dir_id = ?", dirId);
        }
        Dbo.commitTransaction();
    }

    public void addOrUpdateCatalog(DataAssetCatalogVo dataAssetCatalogVo) {
        if (ObjectUtils.isNotEmpty(dataAssetCatalogVo.getCatalog_id()) && 0 != dataAssetCatalogVo.getCatalog_id()) {
            updateCatalog(dataAssetCatalogVo);
        } else {
            addCatalog(dataAssetCatalogVo);
        }
    }

    public void updateCatalog(DataAssetCatalogVo dataAssetCatalogVo) {
        List<DataAssetCatalog> dataAssetCatalogs = Dbo.queryList(DataAssetCatalog.class, "SELECT * FROM " + DataAssetCatalog.TableName + " WHERE catalog_id = ?", dataAssetCatalogVo.getCatalog_id());
        if (ObjectUtils.isEmpty(dataAssetCatalogs)) {
            throw new SystemBusinessException("无法修改，数据为空");
        }
        Dbo.beginTransaction();
        Dbo.execute("delete from " + DataAssetCatalog.TableName + " where catalog_id = ?", dataAssetCatalogVo.getCatalog_id());
        DataAssetCatalog dataAssetCatalog = dataAssetCatalogs.get(0);
        dataAssetCatalog.setCatalog_id(dataAssetCatalogVo.getCatalog_id());
        dataAssetCatalog.setCatalog_code(dataAssetCatalogVo.getCatalog_code());
        dataAssetCatalog.setCatalog_name(dataAssetCatalogVo.getCatalog_name());
        if (StringUtil.isNotBlank(dataAssetCatalogVo.getPublish_status())) {
            dataAssetCatalog.setPublish_status(dataAssetCatalogVo.getPublish_status());
        }
        dataAssetCatalog.add(Dbo.db());
        Dbo.commitTransaction();
    }

    public void addCatalog(DataAssetCatalogVo dataAssetCatalogVo) {
        DataAssetCatalog dataAssetCatalog = new DataAssetCatalog();
        dataAssetCatalog.setCatalog_id(PrimayKeyGener.getNextId());
        dataAssetCatalog.setCatalog_code(dataAssetCatalogVo.getCatalog_code());
        dataAssetCatalog.setCatalog_name(dataAssetCatalogVo.getCatalog_name());
        dataAssetCatalog.setChange_status(ChangeStatus.WEIBIANGENG.getCode());
        if (StringUtil.isNotBlank(dataAssetCatalogVo.getPublish_status())) {
            dataAssetCatalog.setPublish_status(dataAssetCatalogVo.getPublish_status());
        } else {
            dataAssetCatalog.setPublish_status(PublishStatus.WEIFABU.getCode());
        }
        dataAssetCatalog.setCreate_by(String.valueOf(UserUtil.getUserId()));
        dataAssetCatalog.setCreate_date(DateUtil.getSysDate());
        dataAssetCatalog.setCreate_time(DateUtil.getSysTime());
        dataAssetCatalog.add(Dbo.db());
    }

    public void deleteCatalog(long catalog_id) {
        if (0 == catalog_id) {
            return;
        }
        List<DataAssetDir> dataAssetDirList = Dbo.queryList(DataAssetDir.class, "SELECT * FROM " + DataAssetDir.TableName + " WHERE catalog_id = ?", catalog_id);
        if (ObjectUtils.isNotEmpty(dataAssetDirList)) {
            throw new SystemBusinessException("无法删除编目，编码下存在目录");
        }
        Dbo.execute("delete from " + DataAssetCatalog.TableName + " where catalog_id = ?", catalog_id);
    }

    public void publishCatalog(long catalog_id) {
        if (0 == catalog_id) {
            return;
        }
        DataAssetCatalog dataAssetCatalog = Dbo.queryOneObject(DataAssetCatalog.class, "select * from " + DataAssetCatalog.TableName + " where catalog_id = ?", catalog_id).orElseThrow(() -> new BusinessException("无法发布编目，编目不存在"));
        Dbo.execute("update " + DataAssetCatalog.TableName + " set publish_status = ? where catalog_id = ?", PublishStatus.YIFABU.getCode(), dataAssetCatalog.getCatalog_id());
    }

    public List<DataAssetCatalog> findCatalog(String catalogName, Page page) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance().addSql("SELECT * FROM " + DataAssetCatalog.TableName);
        if (StringUtil.isNotBlank(catalogName)) {
            catalogName = "%" + catalogName + "%";
            assembler.addSql(" where catalog_name like ? or catalog_code like ?").addParam(catalogName).addParam(catalogName);
        }
        return Dbo.queryPagedList(DataAssetCatalog.class, page, assembler.sql(), assembler.params());
    }

    public List<DataAssetDirTreeDto> queryDir(long catalogId) {
        return Dbo.queryList(DataAssetDirTreeDto.class, "SELECT dir_id as id, dir_name as name, dir_name as label, " + "dir_code as code, '0' as isLeaf, '' as status, '' as type " + "FROM " + DataAssetDir.TableName + " WHERE catalog_id = ? and (parent_id is null or parent_id = 0)", catalogId);
    }

    public List<DataAssetDir> queryDirById(long dirId) {
        return Dbo.queryList(DataAssetDir.class, "SELECT * FROM " + DataAssetDir.TableName + " WHERE dir_id = ?", dirId);
    }

    public List<DataAssetDirDto> queryDirByParentId(long parentId) {
        return Dbo.queryList(DataAssetDirDto.class, "SELECT T1.*, T2.catalog_name, T2.catalog_code, T2.change_status, " + "T2.change_date, T2.change_time, T2.publish_status FROM " + DataAssetDir.TableName + " T1 JOIN " + DataAssetCatalog.TableName + " T2 ON T1.catalog_id = T2.catalog_id WHERE T1.parent_id = ?", parentId);
    }

    public List<DataAssetDirDto> queryDirById(Long[] dirIds) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance().addSql("SELECT T1.*, T2.catalog_name, T2.catalog_code, T2.change_status, " + "T2.change_date, T2.change_time, T2.publish_status FROM " + DataAssetDir.TableName + " T1 JOIN " + DataAssetCatalog.TableName + " T2 ON T1.catalog_id = T2.catalog_id WHERE T1.dir_id is not null ");
        if (ObjectUtils.isNotEmpty(dirIds)) {
            assembler.addORParam("T1.dir_id", Arrays.asList(dirIds));
        }
        return Dbo.queryList(DataAssetDirDto.class, assembler.sql(), assembler.params());
    }

    public List<DataAssetDirTreeDto> queryByParentId(long parentId, long catalogId, String catalogStatus) {
        if (ObjectUtils.isEmpty(parentId) || 0 == parentId) {
            return queryDir(catalogId);
        }
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        if (StringUtil.isNotBlank(catalogStatus)) {
            PublishStatus publishStatus = PublishStatus.ofEnumByCode(catalogStatus);
            assembler.addSql("SELECT T1.dir_id as id, T1.dir_name as name, T1.dir_name as label, " + " T1.dir_code as code, '0' as isLeaf, '' as status, '' as type " + " FROM " + DataAssetDir.TableName + " T1 JOIN " + DataAssetCatalog.TableName + " T2 ON T1.catalog_id = T2.catalog_id " + " WHERE T1.parent_id = ? and T2.publish_status = ? " + " UNION ALL SELECT T1.asset_id AS id, T1.asset_cname as name, " + " T1.asset_cname as label, T1.asset_code as code, '1' as isLeaf, " + " T1.asset_status as status, T1.asset_type as type " + " FROM " + DataAssetRegist.TableName + " T1 JOIN " + DataAssetDirRel.TableName + " T2 ON T1.asset_id = T2.asset_id JOIN " + DataAssetDir.TableName + " T3 ON T2.dir_id = T3.dir_id JOIN " + DataAssetCatalog.TableName + " T4 ON T3.catalog_id = T4.catalog_id WHERE T3.dir_id = ? AND T4.publish_status = ? ").addParam(parentId).addParam(publishStatus.getCode()).addParam(parentId).addParam(publishStatus.getCode());
        } else {
            assembler.addSql("SELECT dir_id as id, dir_name as name, dir_name as label, " + "dir_code as code, '0' as isLeaf, '' as status, '' as type " + "FROM " + DataAssetDir.TableName + " WHERE parent_id = ? " + "UNION ALL SELECT T1.asset_id AS id, T1.asset_cname as name, " + "T1.asset_cname as label, T1.asset_code as code, '1' as isLeaf, " + "T1.asset_status as status, T1.asset_type as type " + "FROM " + DataAssetRegist.TableName + " T1 JOIN " + DataAssetDirRel.TableName + " T2 ON T1.asset_id = T2.asset_id WHERE T2.dir_id = ? ").addParam(parentId).addParam(parentId);
        }
        return Dbo.queryList(DataAssetDirTreeDto.class, assembler.sql(), assembler.params());
    }

    public void addDir(DataAssetDirVo dataAssetDirVo) {
        DataAssetDir dataAssetDir = new DataAssetDir();
        dataAssetDir.setDir_id(PrimayKeyGener.getNextId());
        dataAssetDir.setDir_name(dataAssetDirVo.getDir_name());
        dataAssetDir.setDir_code(dataAssetDirVo.getDir_code());
        dataAssetDir.setCatalog_id(dataAssetDirVo.getCatalog_id());
        dataAssetDir.setParent_id(dataAssetDirVo.getParent_id());
        dataAssetDir.setCreate_by(String.valueOf(UserUtil.getUserId()));
        dataAssetDir.setCreate_date(DateUtil.getSysDate());
        dataAssetDir.setCreate_time(DateUtil.getSysTime());
        if (ObjectUtils.isNotEmpty(dataAssetDirVo.getParent_id()) && ObjectUtils.isEmpty(dataAssetDirVo.getCatalog_id())) {
            List<DataAssetDir> dataAssetDirList = queryDirById(dataAssetDirVo.getParent_id());
            if (ObjectUtils.isNotEmpty(dataAssetDirList)) {
                dataAssetDir.setCatalog_id(dataAssetDirList.get(0).getCatalog_id());
            }
        }
        dataAssetDir.add(Dbo.db());
    }

    public void updateDir(DataAssetDirVo dataAssetDirVo) {
        if (ObjectUtils.isEmpty(dataAssetDirVo) || ObjectUtils.isEmpty(dataAssetDirVo.getDir_id()) || 0 == dataAssetDirVo.getDir_id()) {
            throw new SystemBusinessException("无法修改，数据为空");
        }
        List<DataAssetDir> dataAssetDirList = Dbo.queryList(DataAssetDir.class, "SELECT * FROM " + DataAssetDir.TableName + " WHERE dir_id = ?", dataAssetDirVo.getDir_id());
        if (ObjectUtils.isEmpty(dataAssetDirList)) {
            throw new SystemBusinessException("无法修改，该id：{}不存在数据", dataAssetDirVo.getDir_id());
        }
        Dbo.beginTransaction();
        Dbo.execute("DELETE FROM " + DataAssetDir.TableName + " WHERE dir_id = ?", dataAssetDirVo.getDir_id());
        DataAssetDir dataAssetDir = dataAssetDirList.get(0);
        dataAssetDir.setDir_name(dataAssetDirVo.getDir_name());
        dataAssetDir.setDir_code(dataAssetDirVo.getDir_code());
        dataAssetDir.setParent_id(dataAssetDirVo.getParent_id());
        dataAssetDir.add(Dbo.db());
        Dbo.commitTransaction();
    }
}
