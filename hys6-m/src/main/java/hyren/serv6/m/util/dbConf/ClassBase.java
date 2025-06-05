package hyren.serv6.m.util.dbConf;

public class ClassBase {

    private static String HadoopImp = "hyren.serv6.commons.hadoop.imp.HadoopImp";

    public static hyren.serv6.m.util.dbConf.IHadoop hadoopInstance() {
        return newInstance(HadoopImp);
    }

    @SuppressWarnings("unchecked")
    private static <T> T newInstance(String aclass) {
        try {
            return (T) Class.forName(aclass).newInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
