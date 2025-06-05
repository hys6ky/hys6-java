package hyren.serv6.h.process.loader.context;

import fd.ng.core.utils.DateUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.JobExecuteState;
import hyren.serv6.base.entity.DmModuleTable;
import hyren.serv6.base.entity.DtabRelationStore;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.h.process.bean.ProcessJobTableConfBean;
import hyren.serv6.h.process.loader.IContext;
import hyren.serv6.h.process.version.VersionManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ContextImpl implements IContext {

    private final ProcessJobTableConfBean processJobTableConfBean;

    private final DtabRelationStore dtabRelationStore;

    private final DmModuleTable dmModuleTable;

    private long jobStartTime;

    final Thread shutdownThread;

    public ContextImpl(ProcessJobTableConfBean processJobTableConfBean) {
        this.processJobTableConfBean = processJobTableConfBean;
        this.dtabRelationStore = processJobTableConfBean.getDtabRelationStore();
        this.dmModuleTable = processJobTableConfBean.getDmModuleTable();
        shutdownThread = new Thread(() -> endJob(false));
    }

    @Override
    public void startJob() {
        IsFlag isTempTableFlag = processJobTableConfBean.getIsTempFlag();
        if (isTempTableFlag == IsFlag.Fou) {
            String jobStateCode = dtabRelationStore.getIs_successful();
            if (JobExecuteState.YunXing.getCode().equals(jobStateCode)) {
                log.error("加工作业步骤正在运行中, 请勿重复提交! 表: [ {} ] ", processJobTableConfBean.getTarTableName());
                System.exit(-2);
            }
        }
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            db.beginTrans();
            dtabRelationStore.setIs_successful(JobExecuteState.YunXing.getCode());
            dtabRelationStore.update(db);
            db.commit();
        } catch (Exception e) {
            throw new BusinessException(String.format("处理开始作业状态信息 更新作业状态为: [ %s ] 时发生异常! e: [ %s ]", dtabRelationStore.getIs_successful(), e));
        }
        Runtime.getRuntime().addShutdownHook(shutdownThread);
        this.jobStartTime = System.currentTimeMillis();
        log.info(String.format("时间: [ %s ], 加工作业表: [ %s ] 开始运行, etlDate: [ %s ], 重跑标识: [ %s ].", DateUtil.getDateTime(DateUtil.DATETIME_ZHCN), processJobTableConfBean.getTarTableName(), processJobTableConfBean.getEtlDate(), processJobTableConfBean.isReRun()));
    }

    @Override
    public void endJob(boolean isSuccessful) {
        JobExecuteState jobExecuteState = isSuccessful ? JobExecuteState.WanCheng : JobExecuteState.ShiBai;
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            db.beginTrans();
            dtabRelationStore.setIs_successful(jobExecuteState.getCode());
            dtabRelationStore.update(db);
            dmModuleTable.setData_u_date(DateUtil.getSysDate());
            dmModuleTable.setData_u_time(DateUtil.getSysTime());
            if (jobExecuteState == JobExecuteState.WanCheng) {
                dmModuleTable.setEtl_date(processJobTableConfBean.getEtlDate());
                try {
                    updateVersion();
                } catch (Exception e) {
                    throw new AppSystemException("版本更新失败：" + e);
                }
            }
            dmModuleTable.update(db);
            db.commit();
        } catch (Exception e) {
            throw new AppSystemException("作业执行失败后! 处理结束作业信息时发生异常! " + e);
        } finally {
            Runtime.getRuntime().removeShutdownHook(shutdownThread);
            log.info(String.format("[ %s ] 作业运行结束 [ %s ]: successful: [ %s ], lastTime: [ %s s. ]", DateUtil.getDateTime(DateUtil.DATETIME_ZHCN), processJobTableConfBean.getModuleTableId(), isSuccessful, (System.currentTimeMillis() - jobStartTime) / 1000F));
        }
    }

    private void updateVersion() {
        VersionManager versionManager = new VersionManager(processJobTableConfBean);
        versionManager.updateJobVersion();
        versionManager.updateModuleVersion();
    }
}
