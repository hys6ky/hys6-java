package hyren.serv6.g.interfaceusemonitor.interfaceuserinfo;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.InterfaceState;
import hyren.serv6.base.entity.InterfaceUse;
import hyren.serv6.base.entity.InterfaceUseLog;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.g.enumerate.StateType;
import hyren.serv6.g.init.InterfaceManager;
import org.springframework.stereotype.Service;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

@Service
public class InterfaceUseInfoService {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result searchInterfaceInfo() {
        return searchInterfaceUseInfo("", null);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "use_valid_date", desc = "", range = "", nullable = true)
    @Param(name = "user_id", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    private Result searchInterfaceUseInfo(String use_valid_date, Long user_id) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.clean();
        assembler.addSql("select interface_name,interface_code,user_name,start_use_date,use_valid_date," + "interface_use_id,use_state from " + InterfaceUse.TableName + " WHERE create_id = ?").addParam(UserUtil.getUserId());
        if (user_id != null) {
            assembler.addSql(" AND user_id = ?").addParam(user_id);
        }
        if (StringUtil.isNotBlank(use_valid_date)) {
            assembler.addSql("AND use_valid_date = ?").addParam(use_valid_date);
        }
        assembler.addSql(" order by interface_use_id");
        Result infoResult = Dbo.queryResult(assembler.sql(), assembler.params());
        for (int i = 0; i < infoResult.getRowCount(); i++) {
            Result response = Dbo.queryResult("SELECT round(avg(response_time)) avg,MIN(response_time) min," + "MAX(response_time) max FROM " + InterfaceUseLog.TableName + " WHERE request_state = ? AND interface_use_id=?", StateType.NORMAL.name(), infoResult.getLong(i, "interface_use_id"));
            String avg = response.getString(0, "avg");
            String min = response.getString(0, "min");
            String max = response.getString(0, "max");
            if (StringUtil.isBlank(avg)) {
                avg = "0";
            }
            if (StringUtil.isBlank(min)) {
                min = "0";
            }
            if (StringUtil.isBlank(max)) {
                max = "0";
            }
            infoResult.setObject(i, "min", min);
            infoResult.setObject(i, "avg", avg);
            infoResult.setObject(i, "max", max);
        }
        return infoResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "use_valid_date", desc = "", range = "", nullable = true)
    @Param(name = "user_id", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public Result searchInterfaceInfoByIdOrDate(Long user_id, String use_valid_date) {
        return searchInterfaceUseInfo(use_valid_date, user_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "interface_use_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> searchInterfaceUseInfoById(Long interface_use_id) {
        return Dbo.queryOneObject("SELECT interface_use_id,start_use_date,use_valid_date FROM " + InterfaceUse.TableName + " WHERE interface_use_id = ?", interface_use_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "interface_use_id", desc = "", range = "")
    @Param(name = "use_valid_date", desc = "", range = "")
    @Param(name = "start_use_date", desc = "", range = "")
    public void updateInterfaceUseInfo(Long interface_use_id, String start_use_date, String use_valid_date) {
        if (start_use_date.contains("-") && start_use_date.length() == 10) {
            start_use_date = StringUtil.replace(start_use_date, "-", "");
        }
        if (use_valid_date.contains("-") && use_valid_date.length() == 10) {
            use_valid_date = StringUtil.replace(use_valid_date, "-", "");
        }
        DboExecute.updatesOrThrow("更新接口使用信息失败", " update " + InterfaceUse.TableName + " set start_use_date=?,use_valid_date=? where interface_use_id = ?", start_use_date, use_valid_date, interface_use_id);
        InterfaceManager.initInterface(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "use_state", desc = "", range = "")
    @Param(name = "interface_use_id", desc = "", range = "")
    public void interfaceDisableEnable(Long interface_use_id, String use_state) {
        try {
            InterfaceState.ofEnumByCode(use_state);
        } catch (Exception e) {
            throw new BusinessException("根据use_state=" + use_state + "没有找到对应代码项值");
        }
        DboExecute.updatesOrThrow("更新接口状态失败", "UPDATE " + InterfaceUse.TableName + " set use_state = ? WHERE interface_use_id = ?", use_state, interface_use_id);
        InterfaceManager.initInterface(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "interface_use_id", desc = "", range = "")
    public void deleteInterfaceUseInfo(Long interface_use_id) {
        DboExecute.deletesOrThrow("当前接口ID对应接口信息不存在", "DELETE FROM " + InterfaceUse.TableName + " WHERE interface_use_id = ?", interface_use_id);
        InterfaceManager.initInterface(Dbo.db());
    }

    public static void main(String[] args) {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateStr = sdf.format(date);
        System.out.println("当前时间为：" + dateStr);
        System.out.println("---------------------------------------------------------------------");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        System.out.println(calendar.get(Calendar.YEAR) + "-" + (calendar.get(Calendar.MONTH) + 1 + "-" + calendar.get(Calendar.DATE) + " " + calendar.get(Calendar.HOUR_OF_DAY) + ":" + calendar.get(Calendar.MINUTE) + ":" + calendar.get(Calendar.SECOND)));
        System.out.println("---------------------------------------------------------------------");
        Instant instant = Instant.now();
        System.out.println("获取纳秒级别的时间戳：" + instant.toEpochMilli());
        LocalDate localDate = LocalDate.now();
        LocalTime localTime = LocalTime.now();
        LocalDateTime localDateTime = LocalDateTime.now();
        System.out.println("获取当前日期: " + localDate);
        System.out.println("获取当前时间： " + localTime);
        System.out.println("获取当前日期时间： " + localDateTime);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String format = dateTimeFormatter.format(localDateTime);
        System.out.println("经过DateTimeFormatter格式化的日期时间: " + format);
        System.out.println(localDateTime.getYear());
        System.out.println(localDateTime.getMonthValue());
        System.out.println(localDateTime.getDayOfMonth());
        System.out.println(localDateTime.getHour());
        System.out.println(localDateTime.getMinute());
        System.out.println(localDateTime.getSecond());
    }
}
