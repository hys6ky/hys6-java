package hyren.serv6.commons.utils;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.serv6.base.exception.BusinessException;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import java.io.Closeable;
import java.io.File;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

@DocClass(desc = "", author = "zxz", createdate = "2019/10/12 15:44")
public class MapDBHelper implements Closeable {

    private DB db;

    public MapDBHelper(String db_file_dir, String db_file_name) {
        File dir = new File(db_file_dir);
        if (!dir.exists()) {
            final boolean mkdirs = dir.mkdirs();
            if (!mkdirs) {
                throw new BusinessException("创建文件夹" + dir + "失败");
            }
        }
        db = DBMaker.newFileDB(new File(db_file_dir + File.separator + db_file_name)).mmapFileEnableIfSupported().closeOnJvmShutdown().cacheSize(500).make();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "name", desc = "", range = "")
    @Param(name = "interval", range = "", desc = "")
    @Return(desc = "", range = "")
    public ConcurrentMap<String, String> htMap(String name, int interval) {
        ConcurrentMap<String, String> map;
        if (db.exists(name)) {
            map = db.getHashMap(name);
        } else {
            map = db.createHashMap(name).keySerializer(Serializer.STRING).valueSerializer(Serializer.STRING).expireAfterWrite(interval, TimeUnit.MINUTES).make();
        }
        return map;
    }

    @Method(desc = "", logicStep = "")
    @Override
    public void close() {
        if (db != null && !db.isClosed()) {
            db.close();
        }
    }

    @Method(desc = "", logicStep = "")
    public void commit() {
        if (db != null && !db.isClosed()) {
            db.commit();
        }
    }

    public static void main(String[] args) {
        try (MapDBHelper mapDBHelper = new MapDBHelper("E:\\hyren\\by-hll\\hrsv5-java\\mapDb\\880396553578090496\\880400002642350080", "880400002642350080.db")) {
            ConcurrentMap<String, String> zzz = mapDBHelper.htMap("880400002642350080", 1);
            zzz.forEach((k, v) -> System.out.println(k + "," + v));
            mapDBHelper.commit();
        }
    }
}
