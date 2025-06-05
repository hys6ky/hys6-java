package hyren.serv6.k.standard.standardData;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.k.entity.DbmNormbasic;
import hyren.serv6.k.entity.StandardImpInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class StandardDataServe {

    public List<Map<String, Object>> getStandardData(Long source_id) {
        List<Map<String, Object>> sourcrs = getSourcr(source_id);
        List<Map<String, Object>> sourcrList = new ArrayList<>();
        sourcrs.forEach(sourcr -> {
            Long stanImpCount = StanImpInfoCount((Long) sourcr.get("source_id"), null);
            if (stanImpCount != 0) {
                Long metaColCount = getMetaTableCount((Long) sourcr.get("source_id"));
                Long stanImpSHICount = StanImpInfoCount((Long) sourcr.get("source_id"), IsFlag.Shi.getCode().toString());
                sourcr.put("metaColCount", metaColCount);
                sourcr.put("stanImpCount", stanImpCount);
                sourcr.put("stanImpSHICount", stanImpSHICount);
                DecimalFormat df = new DecimalFormat("0.00");
                if (stanImpCount != 0 && metaColCount != 0) {
                    sourcr.put("strengthProd", df.format(Float.valueOf(stanImpCount) / Float.valueOf(metaColCount) * 100) + "%");
                } else {
                    sourcr.put("strengthProd", "0%");
                }
                if (stanImpCount != 0 && stanImpSHICount != 0) {
                    sourcr.put("standardthProd", df.format(Float.valueOf(stanImpSHICount) / Float.valueOf(stanImpCount) * 100) + "%");
                } else {
                    sourcr.put("standardthProd", "0%");
                }
                sourcrList.add(sourcr);
            }
        });
        return sourcrList;
    }

    public Long StanImpInfoCount(Long source_id, String code) {
        SqlOperator.Assembler sql = SqlOperator.Assembler.newInstance();
        sql.addSql(" SELECT count(1) FROM " + StandardImpInfo.TableName + "  WHERE " + " obj_id in (SELECT obj_id FROM meta_obj_info WHERE SOURCE_ID = ? )").addParam(source_id);
        if (IsFlag.Shi.getCode().toString().equals(code)) {
            sql.addSql(" AND IMP_RESULT = ?").addParam(IsFlag.Shi.getCode().toString());
        }
        return Dbo.queryNumber(sql.sql(), sql.params()).getAsLong();
    }

    public List<Map<String, Object>> getSourcr(Long source_id) {
        SqlOperator.Assembler sql = SqlOperator.Assembler.newInstance();
        sql.addSql("select * from META_DATA_SOURCE ");
        if (source_id != null) {
            sql.addSql(" where  SOURCE_ID  = ? ").addParam(source_id);
        }
        return Dbo.queryList(sql);
    }

    public Long getMetaTableCount(Long source_id) {
        return Dbo.queryNumber(" SELECT COUNT(1) FROM meta_obj_tbl_col WHERE  " + "obj_id  IN (SELECT obj_id FROM meta_obj_info WHERE SOURCE_ID = ?  AND type = '0')", source_id).getAsLong();
    }

    public List<StandardImpInfo> getdetailsList(Long source_id, String en_name, String col_name, Long sort_id, Page page) {
        SqlOperator.Assembler sql = SqlOperator.Assembler.newInstance();
        sql.addSql(" SELECT * FROM " + StandardImpInfo.TableName + "  WHERE " + " obj_id in (SELECT obj_id FROM meta_obj_info WHERE SOURCE_ID = ? )").addParam(source_id);
        if (!StringUtil.isEmpty(en_name)) {
            sql.addLikeParam("TABLE_ENAME", "%" + en_name + "%");
        }
        if (!StringUtil.isEmpty(col_name)) {
            sql.addLikeParam("SRC_COL_ENAME", "%" + col_name + "%");
        }
        if (null != sort_id) {
            List<DbmNormbasic> dbmNormbasics = Dbo.queryList(DbmNormbasic.class, "select * from " + DbmNormbasic.TableName + " where sort_id = ? ", sort_id);
            if (dbmNormbasics.size() != 0) {
                StringBuilder sb = new StringBuilder();
                dbmNormbasics.forEach(dbmNormbasic -> {
                    sb.append("?,");
                    sql.addParam(dbmNormbasic.getBasic_id());
                });
                sb.deleteCharAt(sb.length() - 1);
                sql.addSql(" AND BASIC_ID in (" + sb.toString() + ") ");
            } else {
                sql.addSql(" AND BASIC_ID = '0' ");
            }
        }
        return Dbo.queryPagedList(StandardImpInfo.class, page, sql);
    }

    public List<Map<String, Object>> getTableName(Long source_id) {
        SqlOperator.Assembler sql = SqlOperator.Assembler.newInstance();
        sql.addSql(" SELECT TABLE_ENAME,TABLE_CNAME FROM " + StandardImpInfo.TableName + "  WHERE " + " obj_id in (SELECT obj_id FROM meta_obj_info WHERE SOURCE_ID = ? ) group BY TABLE_ENAME,TABLE_CNAME").addParam(source_id);
        return Dbo.queryList(sql);
    }
}
