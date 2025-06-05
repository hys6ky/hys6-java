package hyren.serv6.h.market.dmcategory;

import fd.ng.core.utils.DateUtil;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.DmCategory;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Optional;

@Service
@Slf4j
public class DmCategoryService {

    public boolean addDmCategory(DmCategory dmCategory) {
        try {
            dmCategory.setCategory_id(PrimayKeyGener.getNextId());
            dmCategory.setCreate_date(DateUtil.getSysDate());
            dmCategory.setCreate_time(DateUtil.getSysTime());
            if (dmCategory.getParent_category_id() == null) {
                dmCategory.setParent_category_id(dmCategory.getCategory_id());
            }
            dmCategory.setCreate_id(UserUtil.getUserId());
            int add = dmCategory.add(Dbo.db());
            return add == 1;
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("sql 执行错误");
        }
    }

    public boolean delDmCategory(Long dmCategoryId) {
        try {
            int execute = Dbo.execute("delete from " + DmCategory.TableName + " where category_id = ?", dmCategoryId);
            return execute == 1;
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("sql 执行错误");
        }
    }

    public boolean updateDmCategory(DmCategory dmCategory) {
        try {
            dmCategory.setCreate_date(DateUtil.getSysDate());
            dmCategory.setCreate_time(DateUtil.getSysTime());
            int update = dmCategory.update(Dbo.db());
            return update == 1;
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("sql 执行错误");
        }
    }

    public List<DmCategory> findDmCategorys() {
        try {
            return Dbo.queryList(DmCategory.class, "select * from " + DmCategory.TableName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("sql 执行错误");
        }
    }

    public DmCategory findDmCategoryById(Long categoryId) {
        try {
            Optional<DmCategory> dmInfo = Dbo.queryOneObject(DmCategory.class, "select * from " + DmCategory.TableName + " where category_id = ?", categoryId);
            return dmInfo.orElse(null);
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("sql 执行错误");
        }
    }

    public List<DmCategory> findDmCategorysByDmInfoId(Long data_mart_id) {
        try {
            return Dbo.queryList(DmCategory.class, "select * from " + DmCategory.TableName + " where data_mart_id = ?", data_mart_id);
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("sql 执行错误");
        }
    }
}
