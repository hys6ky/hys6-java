package hyren.serv6.t.tableAssign;

import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.t.contants.ReqCategoryEnum;
import hyren.serv6.t.entity.TskTblAssign;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.List;

@Service
public class TskTableAssignServe {

    public void deleteByCategoryId(Long categoryId, ReqCategoryEnum categoryEnum) {
        Dbo.execute("delete from " + TskTblAssign.TableName + " where category_id=? and category=? ", categoryId, categoryEnum.getCode());
        Dbo.commitTransaction();
    }

    public List<TskTblAssign> queryList(Long categoryId, ReqCategoryEnum categoryEnum) {
        return Dbo.queryList(TskTblAssign.class, "select * from " + TskTblAssign.TableName + " where category_id=? and category=?", categoryId, categoryEnum.getCode());
    }

    public void batchSave(List<TskTblAssign> saveAssignList) {
        List<List<Object>> splitList = splitList(saveAssignList, 50);
        for (List<Object> objList : splitList) {
            List<Object[]> etlJobDefParams = new ArrayList<>();
            for (Object obj : objList) {
                TskTblAssign tblAssign = (TskTblAssign) obj;
                Object[] objects = new Object[6];
                objects[0] = tblAssign.getId();
                objects[1] = tblAssign.getCategory_id();
                objects[2] = tblAssign.getCategory();
                objects[3] = tblAssign.getTbl_id();
                objects[4] = tblAssign.getTbl_name();
                objects[5] = tblAssign.getTbl_en_name();
                etlJobDefParams.add(objects);
            }
            SqlOperator.executeBatch(Dbo.db(), "INSERT INTO " + TskTblAssign.TableName + " " + " (id,category_id,category,tbl_id,tbl_name,tbl_en_name) " + " VALUES (?,?,?,?,?,?)", etlJobDefParams);
        }
    }

    public static List<List<Object>> splitList(Object listObj, int chunkSize) {
        List<Object> originalList = (List<Object>) listObj;
        List<List<Object>> result = new ArrayList<>();
        for (int i = 0; i < originalList.size(); i += chunkSize) {
            int endIndex = Math.min(i + chunkSize, originalList.size());
            List<Object> subList = originalList.subList(i, endIndex);
            result.add(subList);
        }
        return result;
    }
}
