package hyren.serv6.t.taskDev;

import hyren.serv6.t.entity.TskTaskData;
import hyren.serv6.t.util.IdGenerator;
import org.springframework.stereotype.Service;
import fd.ng.db.jdbc.Page;
import hyren.daos.bizpot.commons.Dbo;
import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service("tskTaskDataService")
public class TskTaskDataService {

    public TskTaskData queryById(Long id) {
        return Dbo.queryOneObject(TskTaskData.class, "select * from " + TskTaskData.TableName + " where id=?", id).orElse(null);
    }

    public List<TskTaskData> queryByPage(TskTaskData tskTaskData, Page page) {
        return Dbo.queryPagedList(TskTaskData.class, page, "select * from " + TskTaskData.TableName);
    }

    public TskTaskData insert(TskTaskData tskTaskData) {
        tskTaskData.setId(IdGenerator.nextId());
        tskTaskData.add(Dbo.db());
        return tskTaskData;
    }

    public TskTaskData update(TskTaskData tskTaskData) {
        tskTaskData.update(Dbo.db());
        Dbo.commitTransaction();
        return tskTaskData;
    }

    public boolean deleteById(Long id) {
        TskTaskData tskTaskData = new TskTaskData();
        tskTaskData.setId(id);
        tskTaskData.delete(Dbo.db());
        Dbo.commitTransaction();
        return true;
    }

    public TskTaskData queryByTaskIdAntCategory(Long taskId, String taskCategory) {
        return Dbo.queryOneObject(TskTaskData.class, "select * from " + TskTaskData.TableName + " where task_id=? and task_category=? ", taskId, taskCategory).orElse(null);
    }

    public Boolean deleteByDataIdAndTaskCategory(String dataId, String taskCategory) {
        Dbo.execute("delete from " + TskTaskData.TableName + " where data_id=? and task_category=? ", dataId, taskCategory);
        Dbo.commitTransaction();
        return true;
    }

    public Boolean deleteByTaskId(Long taskId) {
        Dbo.execute("delete from " + TskTaskData.TableName + " where task_id=? ", taskId);
        Dbo.commitTransaction();
        return true;
    }
}
