package hyren.serv6.stream.agent.realtimecollection.streamdatadictionary;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.base.exception.BusinessException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Api(tags = "")
@RestController
@RequestMapping("/streamdatadictionary")
@Configuration
public class StreamDataDictionaryInfoController {

    private static final String DATADICTIONARY = "dd_data.json";

    @ApiOperation(value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "file_path", value = "", example = "", dataTypeClass = String.class) })
    @PostMapping("/readDataDictionary")
    public String readDataDictionary(String file_path) throws IOException {
        FileInputStream in = null;
        List<Map<String, Object>> jsonArray = new ArrayList<>();
        String dataDictionaryFile = file_path + File.separator + DATADICTIONARY;
        String dd_data = "";
        try {
            File file = new File(dataDictionaryFile);
            if (file.exists()) {
                in = new FileInputStream(file);
                Long filelength = file.length();
                byte[] filecontent = new byte[filelength.intValue()];
                in.read(filecontent);
                dd_data = new String(filecontent, "UTF-8");
                jsonArray = JsonUtil.toObject(dd_data, new TypeReference<List<Map<String, Object>>>() {
                });
            } else {
                throw new BusinessException("数据字典文件不存在!");
            }
        } catch (Exception e) {
            throw new BusinessException("读取数据字典失败!");
        } finally {
            if (in != null) {
                in.close();
            }
        }
        return jsonArray.toString();
    }
}
