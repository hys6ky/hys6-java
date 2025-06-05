package hyren.serv6.control.task.helper;

import fd.ng.core.utils.ArrayUtil;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.Dispatch_Frequency;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.jobUtil.task.TaskSqlHelper;
import lombok.extern.slf4j.Slf4j;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Stack;

@Slf4j
public class TaskJobHelper {

    private static final String PARASEPARATOR = "@";

    public static final DateTimeFormatter DATETIME_DEFAULT = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");

    private TaskJobHelper() {
    }

    public static String transformDirOrName(String currBathDate, long etl_sys_id, String dirOrName) {
        String[] params = TaskJobHelper.transformPara(currBathDate, etl_sys_id, dirOrName);
        if (params.length < 1) {
            return dirOrName;
        }
        StringBuilder newDirOrName = new StringBuilder();
        for (String param : params) {
            newDirOrName.append(param);
        }
        return newDirOrName.toString();
    }

    public static String transformProgramPara(String currBathDate, long etl_sys_id, String programPara) {
        String[] params = TaskJobHelper.transformPara(currBathDate, etl_sys_id, programPara);
        if (params.length < 1) {
            return programPara;
        }
        StringBuilder newProgramPara = new StringBuilder(params[0]);
        for (int i = 1; i < params.length; i++) {
            newProgramPara.append(PARASEPARATOR).append(params[i]);
        }
        return newProgramPara.toString();
    }

    private static String[] transformPara(String currBathDate, long etl_sys_id, String para) {
        if (StringUtil.isEmpty(para)) {
            return ArrayUtil.EMPTY_STRING_ARRAY;
        }
        String[] arr = para.split(PARASEPARATOR);
        String[] newArr = new String[arr.length];
        Stack<Character> stack = new Stack<>();
        for (int i = 0; i < arr.length; i++) {
            char[] ca = arr[i].toCharArray();
            for (char c : ca) {
                stack.push(c);
                if (!stack.peek().equals('}')) {
                    continue;
                }
                StringBuilder resultst = new StringBuilder();
                while (!stack.peek().equals('{')) {
                    resultst.append(stack.pop());
                }
                stack.pop();
                resultst.append(stack.pop());
                stack.clear();
                char[] charArry = resultst.toString().toCharArray();
                StringBuilder paraCdSB = new StringBuilder();
                for (int num = charArry.length - 1; num >= 1; num--) {
                    paraCdSB.append(charArry[num]);
                }
                char prefix = paraCdSB.charAt(0);
                if ('#' == prefix) {
                    String paraCd = TaskSqlHelper.getParaByPara(paraCdSB.toString().toLowerCase());
                    LocalDate date = LocalDate.parse(currBathDate, DateUtil.DATE_DEFAULT);
                    if ("#txdate".equals(paraCd)) {
                        arr[i] = arr[i].replace("#{txdate}", date.format(DateUtil.DATE_DEFAULT));
                    } else if ("#date".equals(paraCd)) {
                        arr[i] = arr[i].replace("#{date}", LocalDate.now().format(DateUtil.DATE_DEFAULT));
                    } else if ("#txdate_pre".equals(paraCd)) {
                        arr[i] = arr[i].replace("#{txdate_pre}", date.plus(-1, ChronoUnit.DAYS).format(DateUtil.DATE_DEFAULT));
                    } else if ("#txdate_next".equals(paraCd)) {
                        arr[i] = arr[i].replace("#{txdate_next}", date.plus(1, ChronoUnit.DAYS).format(DateUtil.DATE_DEFAULT));
                    }
                } else if ('!' == prefix) {
                    String paraCd = paraCdSB.toString();
                    String paraVal = TaskSqlHelper.getEtlParameterVal(etl_sys_id, paraCd);
                    LocalDate date = LocalDate.parse(currBathDate, DateUtil.DATE_DEFAULT);
                    DateTimeFormatter pattern;
                    String strsc = paraCdSB.substring(1);
                    switch(paraCd) {
                        case "!txdate":
                            pattern = DateTimeFormatter.ofPattern(paraVal);
                            arr[i] = arr[i].replace("!{" + strsc + "}", date.format(pattern));
                            break;
                        case "!date":
                            pattern = DateTimeFormatter.ofPattern(paraVal);
                            arr[i] = arr[i].replace("!{" + strsc + "}", LocalDate.now().format(pattern));
                            break;
                        case "!txdate_pre":
                            pattern = DateTimeFormatter.ofPattern(paraVal);
                            arr[i] = arr[i].replace("!{" + strsc + "}", date.plus(-1, ChronoUnit.DAYS).format(pattern));
                            break;
                        case "!txdate_next":
                            pattern = DateTimeFormatter.ofPattern(paraVal);
                            arr[i] = arr[i].replace("!{" + strsc + "}", date.plus(1, ChronoUnit.DAYS).format(pattern));
                            break;
                        default:
                            arr[i] = arr[i].replace("!{" + strsc + "}", paraVal);
                            break;
                    }
                } else {
                    log.error("无法识别的参数 para:" + para + "关键字前缀: " + prefix);
                }
            }
            newArr[i] = arr[i];
        }
        return newArr;
    }

    public static String getNextBathDate(String currBathDateStr) {
        return LocalDate.parse(currBathDateStr, DateUtil.DATE_DEFAULT).plus(1, ChronoUnit.DAYS).format(DateUtil.DATE_DEFAULT);
    }

    public static String getNextExecuteDate(String currBathDate, String freqType) {
        return getExecuteDate(currBathDate, freqType, 1);
    }

    public static String getPreExecuteDate(String currBathDate, String freqType) {
        return getExecuteDate(currBathDate, freqType, -1);
    }

    private static String getExecuteDate(String currBathDateStr, String freqType, int offset) {
        LocalDate currBathDate = LocalDate.parse(currBathDateStr, DateUtil.DATE_DEFAULT);
        if (Dispatch_Frequency.DAILY.getCode().equals(freqType)) {
            currBathDate = currBathDate.plus(offset, ChronoUnit.DAYS);
        } else if (Dispatch_Frequency.MONTHLY.getCode().equals(freqType)) {
            currBathDate = currBathDate.plus(offset, ChronoUnit.MONTHS);
        } else if (Dispatch_Frequency.WEEKLY.getCode().equals(freqType)) {
            currBathDate = currBathDate.plus(offset, ChronoUnit.WEEKS);
        } else if (Dispatch_Frequency.YEARLY.getCode().equals(freqType)) {
            currBathDate = currBathDate.plus(offset, ChronoUnit.YEARS);
        } else if (Dispatch_Frequency.PinLv.getCode().equals(freqType)) {
            log.info("频率类型：{}", freqType);
            return "";
        } else {
            throw new AppSystemException("不支持的频率类型：" + freqType);
        }
        return currBathDate.format(DateUtil.DATE_DEFAULT);
    }

    public static LocalDateTime getExecuteTimeByTPlus1(String strDateTime) {
        return LocalDateTime.parse(strDateTime, TaskJobHelper.DATETIME_DEFAULT).plus(1, ChronoUnit.DAYS);
    }
}
