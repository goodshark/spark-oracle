package org.apache.hive.basesql;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dengrb1 on 4/1 0001.
 */
public class EngineManager {

    private static Map<String, Engine> registerdEngine = new HashMap<>();

    public static void registerEngine(String name, Engine engine) {
        registerdEngine.put(name, engine);
    }

    public static Engine getEngine(String name) {
        return registerdEngine.get(name);
    }
}
