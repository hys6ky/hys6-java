package hyren.serv6.t.devTest;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.daos.bizpot.commons.ContextDataHolder;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.User;
import hyren.serv6.t.contants.TaskStatusEnum;
import hyren.serv6.t.entity.*;
import hyren.serv6.t.util.IdGenerator;
import hyren.serv6.t.util.ThreadPoolUtil;
import hyren.serv6.t.vo.query.*;
import org.springframework.stereotype.Service;
import fd.ng.db.jdbc.Page;
import hyren.daos.bizpot.commons.Dbo;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

@Service("tskTaskPointRelService")
public class TskTaskPointRelService {

    @Resource(name = "commonFixedPool")
    private ThreadPoolUtil threadPoolUtil;

    public Map<String, Object> queryById(Long task_id) {
        TskTaskDevQueryVo tskTaskDev = Dbo.queryOneObject(TskTaskDevQueryVo.class, "SELECT biz_name,data_req_name,a.* from " + TskTaskDev.TableName + " a " + "LEFT JOIN " + TskBizReq.TableName + " b ON a.biz_id=b.biz_id " + "LEFT JOIN " + TskDataReq.TableName + " c ON a.data_req_id=c.data_req_id where a.task_id = ?", task_id).orElse(null);
        List<TskTaskPointRelQueryVo> tskTestPoints = Dbo.queryList(TskTaskPointRelQueryVo.class, "select a.*,b.* from " + TskTaskPointRel.TableName + " a " + "LEFT JOIN " + TskTestPoint.TableName + " b ON a.point_id = b.point_id " + "where a.task_id = ?", task_id);
        Map<String, Object> detailMap = new HashMap<>();
        detailMap.put("taskInfo", tskTaskDev);
        detailMap.put("testPointInfo", tskTestPoints);
        return detailMap;
    }

    public Map<String, Object> queryByPage(TskTaskTestQueryVo tskTaskTestQueryVo) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT biz_name,data_req_name,a.* from " + TskTaskDev.TableName + " a " + "LEFT JOIN " + TskBizReq.TableName + " b ON a.biz_id=b.biz_id " + "LEFT JOIN " + TskDataReq.TableName + " c ON a.data_req_id=c.data_req_id");
        sql.append(" where 1=1");
        sql.append(" and a.task_status != ").append("'" + TaskStatusEnum.TO_BE_DEV.getCode() + "'");
        if (StringUtil.isNotBlank(tskTaskTestQueryVo.getTask_name())) {
            System.out.println("t1");
            sql.append(" and a.task_name like '" + '%' + tskTaskTestQueryVo.getTask_name() + '%' + "'");
        }
        if (StringUtil.isNotBlank(tskTaskTestQueryVo.getEnd_date())) {
            sql.append(" and a.end_date = '" + tskTaskTestQueryVo.getEnd_date() + "'");
        }
        if (StringUtil.isNotBlank(tskTaskTestQueryVo.getOwner_id())) {
            sql.append(" and a.owner_id = '" + tskTaskTestQueryVo.getOwner_id() + "'");
        }
        if (StringUtil.isNotBlank(tskTaskTestQueryVo.getTester_id())) {
            sql.append(" and a.tester_id = " + tskTaskTestQueryVo.getTester_id() + "");
        }
        if (StringUtil.isNotBlank(tskTaskTestQueryVo.getTask_status())) {
            sql.append(" and a.task_status = '" + tskTaskTestQueryVo.getTask_status() + "'");
        }
        if (StringUtil.isNotBlank(tskTaskTestQueryVo.getTest_status())) {
            sql.append(" and a.test_status = '" + tskTaskTestQueryVo.getTest_status() + "'");
        }
        sql.append(" order by end_date desc");
        List<TskTaskDevQueryVo> list = Dbo.queryPagedList(TskTaskDevQueryVo.class, tskTaskTestQueryVo, sql.toString());
        Map<String, Object> tskTaskDevMap = new HashMap<>();
        tskTaskDevMap.put("tskTaskDevList", list);
        tskTaskDevMap.put("totalSize", tskTaskTestQueryVo.getTotalSize());
        return tskTaskDevMap;
    }

    public List<TskTestPoint> selectPointByTaskCategory(String taskCategory, Page page) {
        String sql = "select * from " + TskTestPoint.TableName + " where task_category = '" + taskCategory + "' order by updated_time desc";
        List<TskTestPoint> list = Dbo.queryPagedList(TskTestPoint.class, page, sql);
        return list;
    }

    public void insertRel(Long task_id, Long[] ids) {
        if (ids.length > 0) {
            if (!checkRepeat(task_id, ids)) {
                throw new BusinessException("要点选择不可重复添加");
            }
            List<Object[]> list = new ArrayList<>();
            User user = ContextDataHolder.getUserInfo(User.class);
            for (int i = 0; i < ids.length; i++) {
                Object[] tskTaskPointRelInfo = new Object[5];
                tskTaskPointRelInfo[0] = IdGenerator.nextId();
                tskTaskPointRelInfo[1] = user.getUserId();
                tskTaskPointRelInfo[2] = user.getUsername();
                tskTaskPointRelInfo[3] = task_id;
                tskTaskPointRelInfo[4] = ids[i];
                list.add(tskTaskPointRelInfo);
            }
            Dbo.executeBatch("INSERT INTO" + " tsk_task_point_rel(rel_id,rel_user_id,rel_user,task_id,point_id) VALUES(?,?,?,?,?)", list);
        } else {
            throw new BusinessException("要点选择不可为空");
        }
    }

    public boolean checkRepeat(Long task_id, Long[] ids) {
        for (Long id : ids) {
            List<TskTaskPointRel> list = Dbo.queryList(TskTaskPointRel.class, "select * from " + TskTaskPointRel.TableName + " where task_id = ? and POINT_ID = ?", task_id, id);
            if (list.size() != 0) {
                return false;
            }
        }
        return true;
    }

    public boolean deleteByRelId(Long rel_id) {
        TskTaskPointRel tskTaskPointRel = new TskTaskPointRel();
        tskTaskPointRel.setRel_id(rel_id);
        tskTaskPointRel.delete(Dbo.db());
        Dbo.execute("delete from " + TskTestPointVarConf.TableName + " where rel_id = ?", rel_id);
        Dbo.commitTransaction();
        return true;
    }

    public Map<String, Object> queryConfigByRelId(Long rel_id) {
        TskTaskPointRel tskTaskPointRel = Dbo.queryOneObject(TskTaskPointRel.class, "select * from " + TskTaskPointRel.TableName + " where rel_id = ?", rel_id).orElse(null);
        TskTestPoint tskTestPoint = Dbo.queryOneObject(TskTestPoint.class, "select * from " + TskTestPoint.TableName + " where point_id = ?", tskTaskPointRel.getPoint_id()).orElse(null);
        List<TskTestPointVarConf> list = Dbo.queryList(TskTestPointVarConf.class, "select * from " + TskTestPointVarConf.TableName + " where rel_id =?", rel_id);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("testPoint", tskTestPoint);
        resultMap.put("pointCofig", list);
        return resultMap;
    }

    public void commitTest(Long task_id) {
        testConfig(task_id);
        Dbo.execute("update " + TskTaskDev.TableName + " set TASK_STATUS = ? where task_id = ?", TaskStatusEnum.TESTING.getCode(), task_id);
    }

    public void testConfig(Long task_id) {
        Map<String, Object> stringObjectMap = queryById(task_id);
        TskTaskDevQueryVo taskInfo = (TskTaskDevQueryVo) stringObjectMap.get("taskInfo");
        if (taskInfo.getTask_status().equals(TaskStatusEnum.CMT_TEST.getCode()) || taskInfo.getTask_status().equals(TaskStatusEnum.TEST_RJT.getCode())) {
            List<TskTaskPointRel> list = Dbo.queryList(TskTaskPointRel.class, "select * from " + TskTaskPointRel.TableName + " where task_id = ?", task_id);
            Map<Long, Future<String>> receiveResultMap = new HashMap<>();
            for (TskTaskPointRel e : list) {
                TskTestPoint tskTestPoint = Dbo.queryOneObject(TskTestPoint.class, "select * from " + TskTestPoint.TableName + " where point_id = ?", e.getPoint_id()).orElse(null);
                String test_sql = tskTestPoint.getTest_sql();
                List<TskTestPointVarConf> confs = Dbo.queryList(TskTestPointVarConf.class, "select * from " + TskTestPointVarConf.TableName + " where rel_id = ?", e.getRel_id());
                for (TskTestPointVarConf conf : confs) {
                    test_sql = test_sql.replace(conf.getVar_key(), conf.getVar_val());
                }
                receiveResultMap.put(e.getRel_id(), executeTestSqlThread(test_sql));
            }
            threadPoolUtil.execute(executeReceiveResultThread(task_id, receiveResultMap));
            Dbo.execute("update " + TskTaskDev.TableName + " set TASK_STATUS = ? where task_id = ?", TaskStatusEnum.TESTING.getCode(), task_id);
        } else {
            throw new SystemBusinessException("该状态下的任务，无法提交测试");
        }
    }

    private Thread executeReceiveResultThread(Long task_id, Map<Long, Future<String>> receiveResultMap) {
        return new Thread(() -> {
            DatabaseWrapper db = new DatabaseWrapper();
            receiveResultMap.forEach((test_id, future) -> {
                String result;
                try {
                    result = future.get();
                } catch (InterruptedException | ExecutionException e) {
                    result = "程序执行异常，原因" + e.getMessage();
                }
                SqlOperator.execute(db, "update " + TskTaskPointRel.TableName + " set test_result=? where rel_id=? ", result, test_id);
            });
            SqlOperator.execute(db, "update " + TskTaskDev.TableName + " set TASK_STATUS = ? where task_id = ?", TaskStatusEnum.TO_BE_FB.getCode(), task_id);
            db.commit();
            db.close();
        });
    }

    private Future<String> executeTestSqlThread(String test_sql) {
        return threadPoolUtil.submit(() -> {
            int ttt = ThreadLocalRandom.current().nextInt(1000, 10000 + 1);
            System.out.println(ttt);
            Thread.sleep(ttt);
            return String.valueOf(ttt);
        });
    }

    public List<TskTaskPointRelQueryVo> testBack(Long task_id) {
        return Dbo.queryList(TskTaskPointRelQueryVo.class, "select a.* , b.* ,c.test_note from " + TskTaskPointRel.TableName + " a " + "LEFT JOIN " + TskTestPoint.TableName + " b ON a.point_id = b.point_id " + "LEFT JOIN " + TskTaskDev.TableName + " c ON a.task_id = c.task_id " + "where a.task_id = ?", task_id);
    }

    public void testPass(Long task_id, String test_note) {
        Dbo.execute("update " + TskTaskDev.TableName + " set TASK_STATUS = ?,test_status = ?,test_note = ? where task_id = ?", TaskStatusEnum.TEST_CPLT.getCode(), IsFlag.Shi.getCode(), test_note, task_id);
    }

    public void testNoPass(Long task_id, String test_note) {
        Dbo.execute("update " + TskTaskDev.TableName + " set TASK_STATUS = ?,test_status = ?,test_note = ? where task_id = ?", TaskStatusEnum.TEST_RJT.getCode(), IsFlag.Fou.getCode(), test_note, task_id);
    }
}
