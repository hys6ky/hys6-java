package hyren.serv6.n.biz.N1004;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.n.bean.DataAssetSearchDto;
import hyren.serv6.n.entity.DataAssetColumn;
import hyren.serv6.n.entity.DataAssetRegist;
import hyren.serv6.n.enums.AssetStatus;
import hyren.serv6.n.enums.AssetType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.List;

@Service
@Slf4j
public class DataAssetSearchService {

    public List<DataAssetSearchDto> searchDataAsset(String searchText, String assetType, Page page) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        if (StringUtil.isNotBlank(assetType)) {
            AssetType assetTypeEnum = AssetType.ofEnumByCode(assetType);
            if (AssetType.ZIDUAN != assetTypeEnum) {
                assembler.addSql("select asset_id as assetId, mdata_table_id as mdataId, " + "asset_code as assetCode, asset_cname as assetCName, asset_ename as assetEName, " + "asset_cname as assetNormCName, asset_ename as assetNormEName, business_remark as businessRemark, " + "'' as assetRemark, business_pk as businessPk, business_cname as businessCName, " + "tech_pk as techPk, tech_cname as techCName, data_auth_code as dataAuthCode, " + "belong_depart as belongDepart, belong_by as belongBy, manage_depart as manageDepart, " + "manage_by as manageBy, asset_by as assetBy, asset_date as assetDate, asset_time as assetTime, " + "asset_type as assetType, '' as shareType, '' as shareMetho, '' as securityLevel, '' as amountUnit " + "from " + DataAssetRegist.TableName + " where asset_status = ? ").addParam(AssetStatus.YIWANCHENGDENGJI.getCode());
                if (StringUtil.isNotBlank(searchText)) {
                    searchText = "%" + searchText + "%";
                    assembler.addSql(" and (asset_ename like ? or asset_code like ? or business_remark like ?) ").addParam(searchText).addParam(searchText).addParam(searchText);
                }
                assembler.addSql(" and asset_type = ? ").addParam(assetTypeEnum.getCode());
                assembler.addSql(" order by asset_date, asset_time ");
            } else {
                assembler.addSql("select T1.col_id as assetId, T1.mdata_col_id as mdataId, '' as assetCode, " + "T1.col_cname as assetCName, T1.col_ename as assetCName, T1.norm_col_cname as assetNormCName, " + "T1.norm_col_ename as assetNormEName, T1.col_business as businessRemark, " + "T2.business_remark AS assetRemark, T2.business_pk as businessPk, T2.business_cname as businessCName, " + "T2.tech_pk as techPk, T2.tech_cname as techCName, T2.data_auth_code as dataAuthCode, " + "T2.belong_depart as belongDepart, T2.belong_by as belongBy, T2.manage_depart as manageDepart, " + "T2.manage_by as manageBy, T2.asset_by as assetBy, T2.asset_date as assetDate, T2.asset_time as assetTime, " + "? as assetType, T1.share_type as shareType, T1.share_metho as shareMetho, T1.security_level as securityLevel, " + "T1.amount_unit as amountUnit from " + DataAssetColumn.TableName + " T1 JOIN " + DataAssetRegist.TableName + " T2 ON T1.asset_id = T2.asset_id " + "where T2.asset_status = ? ").addParam(AssetType.ZIDUAN.getCode()).addParam(AssetStatus.YIWANCHENGDENGJI.getCode());
                if (StringUtil.isNotBlank(searchText)) {
                    searchText = "%" + searchText + "%";
                    assembler.addSql(" and (T1.norm_col_ename like ? or T1.norm_col_cname like ? or T1.col_business like ?) ").addParam(searchText).addParam(searchText).addParam(searchText);
                }
                assembler.addSql(" order by T1.update_date, T1.update_time ");
            }
        } else {
            assembler.addSql("select asset_id as assetId, mdata_table_id as mdataId, " + "asset_code as assetCode, asset_cname as assetCName, asset_ename as assetEName, " + "asset_cname as assetNormCName, asset_ename as assetNormEName, business_remark as businessRemark, " + "'' as assetRemark, business_pk as businessPk, business_cname as businessCName, " + "tech_pk as techPk, tech_cname as techCName, data_auth_code as dataAuthCode, " + "belong_depart as belongDepart, belong_by as belongBy, manage_depart as manageDepart, " + "manage_by as manageBy, asset_by as assetBy, asset_date as assetDate, asset_time as assetTime, " + "asset_type as assetType, '' as shareType, '' as shareMetho, '' as securityLevel, '' as amountUnit " + "from " + DataAssetRegist.TableName + " where asset_status = ? ").addParam(AssetStatus.YIWANCHENGDENGJI.getCode());
            if (StringUtil.isNotBlank(searchText)) {
                searchText = "%" + searchText + "%";
                assembler.addSql(" and (asset_ename like ? or asset_code like ? or business_remark like ?) ").addParam(searchText).addParam(searchText).addParam(searchText);
            }
            assembler.addSql(" union all select T1.col_id as assetId, T1.mdata_col_id as mdataId, '' as assetCode, " + "T1.col_cname as assetCName, T1.col_ename as assetCName, T1.norm_col_cname as assetNormCName, " + "T1.norm_col_ename as assetNormEName, T1.col_business as businessRemark, " + "T2.business_remark AS assetRemark, T2.business_pk as businessPk, T2.business_cname as businessCName, " + "T2.tech_pk as techPk, T2.tech_cname as techCName, T2.data_auth_code as dataAuthCode, " + "T2.belong_depart as belongDepart, T2.belong_by as belongBy, T2.manage_depart as manageDepart, " + "T2.manage_by as manageBy, T2.asset_by as assetBy, T2.asset_date as assetDate, T2.asset_time as assetTime, " + "? as assetType, T1.share_type as shareType, T1.share_metho as shareMetho, T1.security_level as securityLevel, " + "T1.amount_unit as amountUnit from " + DataAssetColumn.TableName + " T1 JOIN " + DataAssetRegist.TableName + " T2 ON T1.asset_id = T2.asset_id " + "where T2.asset_status = ? ").addParam(AssetType.ZIDUAN.getCode()).addParam(AssetStatus.YIWANCHENGDENGJI.getCode());
            if (StringUtil.isNotBlank(searchText)) {
                searchText = "%" + searchText + "%";
                assembler.addSql(" and (T1.norm_col_ename like ? or T1.norm_col_cname like ? or T1.col_business like ?) ").addParam(searchText).addParam(searchText).addParam(searchText);
            }
        }
        return Dbo.queryPagedList(DataAssetSearchDto.class, page, assembler.sql(), assembler.params());
    }
}
