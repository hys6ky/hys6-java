package hyren.serv6.g.commons;

import hyren.daos.bizpot.commons.ContextDataHolder;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.entity.InterfaceFileInfo;
import hyren.serv6.commons.utils.RequestUtil;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.Map;

public class FileDownload {

    public HttpServletResponse downLoadFile(String uuid, Long user_id) {
        OutputStream out;
        BufferedInputStream in;
        HttpServletResponse response = ContextDataHolder.getResponse();
        try {
            Map<String, Object> fileInfo = fileInfo(uuid, user_id);
            String file_path = fileInfo.get("file_path").toString();
            String data_class = fileInfo.get("data_class").toString();
            uuid = uuid + '.' + data_class;
            file_path = file_path + File.separator + uuid;
            response.reset();
            if (RequestUtil.getRequest().getHeader("User-Agent").toLowerCase().indexOf("firefox") > 0) {
                response.setHeader("content-disposition", "attachment;filename=" + uuid);
            } else {
                response.setHeader("content-disposition", "attachment;filename=" + uuid);
            }
            response.setHeader("content-type", "text/application;charset=" + DataBaseCode.UTF_8.getValue());
            response.setCharacterEncoding(DataBaseCode.UTF_8.getValue());
            response.setContentType("APPLICATION/OCTET-STREAM");
            in = new BufferedInputStream(new FileInputStream(file_path));
            out = response.getOutputStream();
            byte[] bytes = new byte[10 * 1024 * 1024];
            int read;
            while ((read = in.read(bytes)) != -1) {
                out.write(bytes, 0, read);
                out.flush();
            }
        } catch (IOException e) {
            response.setStatus(500);
            e.printStackTrace();
        }
        return response;
    }

    public Map<String, Object> fileInfo(String uuid, Long user_id) {
        InterfaceFileInfo info = new InterfaceFileInfo();
        info.setUser_id(user_id);
        info.setFile_id(uuid);
        return Dbo.queryOneObject("SELECT * FROM " + InterfaceFileInfo.TableName + " WHERE file_id = ? AND user_id=?", info.getFile_id(), info.getUser_id());
    }
}
