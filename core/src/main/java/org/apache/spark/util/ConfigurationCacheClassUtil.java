package org.apache.spark.util;

import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * Created by chenfolin on 2017/9/28.
 */
public class ConfigurationCacheClassUtil {

    public static Map getCacheClassField() throws Exception{
        Field f = Configuration.class.getDeclaredField("CACHE_CLASSES");
        f.setAccessible(true);
        Map map = (Map) f.get(null);
        return map;
    }

}
