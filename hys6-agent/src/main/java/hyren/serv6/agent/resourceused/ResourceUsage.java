package hyren.serv6.agent.resourceused;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@Api(tags = "")
@RestController
@RequestMapping("/resourceused")
public class ResourceUsage {

    @ApiOperation(value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "startTime", value = "", example = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "endTime", value = "", example = "", dataTypeClass = Long.class) })
    @RequestMapping("/readResourceInfo")
    public List<Object> readResourceInfo(@NotNull(message = "") long startTime, @NotNull(message = "") long endTime) {
        if (startTime == 0) {
            startTime = Long.parseLong(DateUtil.getSysDate() + DateUtil.getSysTime());
        }
        if (endTime == 0) {
            endTime = Long.parseLong(DateUtil.getSysDate() + DateUtil.getSysTime());
        }
        List<Object> msgList = new ArrayList<>();
        try {
            long integrationStart = Long.parseLong(String.valueOf(startTime).substring(0, String.valueOf(startTime).length() - 4));
            long integrationsEnd = Long.parseLong(String.valueOf(endTime).substring(0, String.valueOf(endTime).length() - 4));
            for (long i = integrationStart; i <= integrationsEnd; i++) {
                String regexFileName = String.valueOf(i);
                File[] files = new File(WriterResource.filePath).listFiles(new FilenameFilter() {

                    private final Pattern pattern = Pattern.compile(regexFileName);

                    @Override
                    public boolean accept(File dir, String name) {
                        return pattern.matcher(name.toUpperCase()).matches();
                    }
                });
                if (files != null && files.length > 0) {
                    msgList.addAll(readFileMsg(files[0], startTime, endTime));
                }
            }
            return msgList;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return msgList;
    }

    @ApiOperation(value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "filePath", value = "", example = "", dataTypeClass = File.class), @ApiImplicitParam(name = "startTime", value = "", example = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "endTime", value = "", example = "", dataTypeClass = Long.class) })
    @PostMapping("/readFileMsg")
    private List<Object> readFileMsg(File filePath, long startTime, long endTime) {
        List<Object> msgList = new ArrayList<>();
        try {
            List<String> fileInfo = FileUtils.readLines(filePath, "UTF-8");
            List<Map<String, Object>> collect = fileInfo.stream().map(obj -> JsonUtil.toObject(JsonUtil.toJson(obj), new TypeReference<Map<String, Object>>() {
            })).filter(item -> Long.parseLong(item.get("recordTime").toString()) >= startTime && Long.parseLong(item.get("recordTime").toString()) <= endTime).collect(Collectors.toList());
            msgList.addAll(collect);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return msgList;
    }

    public static void main(String[] args) {
        ResourceUsage resourceUsage = new ResourceUsage();
        List<Object> list = resourceUsage.readResourceInfo(20211009140010L, 20211009160818L);
        System.out.println(list);
    }
}
