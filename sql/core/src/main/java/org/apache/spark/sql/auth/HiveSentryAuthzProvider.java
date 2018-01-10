package org.apache.spark.sql.auth;

import com.google.common.base.Splitter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.*;

/**
 * Created by chenfolin on 2017/10/23.
 */
public class HiveSentryAuthzProvider {

    public static final String SENTRY_HOOK_KEY = "hive.server2.session.hook";
    public static final String SENTRY_HOOK = "org.apache.sentry.binding.hive.HiveAuthzBindingSessionHook";

    private static final Logger LOG = LoggerFactory.getLogger(HiveSentryAuthzProvider.class);

    private static final Splitter ROLE_SET_SPLITTER = Splitter.on(",").trimResults()
            .omitEmptyStrings();

    static HiveSentryAuthzProvider provider;
    static Boolean inited = false;

    static HiveConf hiveConf;

    private HiveSentryAuthzProviderInterface pinstance;

    public static ThreadLocal<String> cuser = new ThreadLocal<String>();

    public static void setUser(String user) {
        cuser.set(user);
    }

    public static String getUser() {
        return cuser.get();
    }

    static{
        hiveConf = new HiveConf();
    }

    public HiveSentryAuthzProvider() {
        try {
            init();
        } catch (Exception e) {
            LOG.error("Create HiveSentryAuthzProvider Error.", e);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void init() throws Exception{
        String className = "org.apache.spark.sql.auth.HiveSentryAuthzProviderImpl";
        Constructor<?> constructor =
                Class.forName(className).getDeclaredConstructor(HiveConf.class);
        this.pinstance = (HiveSentryAuthzProviderInterface)constructor.newInstance(new Object[] {hiveConf});
    }

    public void authorize(HashSet<AuthzEntity> tables, String currentdb, String username) throws AuthorizationException{
        pinstance.authorize(tables, currentdb, username);
    }

    public String[] filterDatabase(String[] databases) {
        return pinstance.filterDatabase(cuser.get(), databases);
    }

    public String[] filterTable(String[] tables, String db) {
        return pinstance.filterTable(cuser.get(), tables, db);
    }

    public static boolean useSentryAuth(){
        return SENTRY_HOOK.equals(hiveConf.get(SENTRY_HOOK_KEY));
    }

    public static  HiveSentryAuthzProvider getInstance(){
        if(provider == null || inited == false){
            synchronized (inited){
                if(inited == false){
                    provider = new HiveSentryAuthzProvider();
                    inited = true;
                }
            }
        }
        return provider;
    }

}
