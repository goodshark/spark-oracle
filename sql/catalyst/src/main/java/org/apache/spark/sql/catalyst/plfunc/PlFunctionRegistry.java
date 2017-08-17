package org.apache.spark.sql.catalyst.plfunc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by chenfolin on 2017/8/9.
 */
public class PlFunctionRegistry {

    private static final Logger logger = LoggerFactory.getLogger(PlFunctionRegistry.class);

    private static PlFunctionRegistry plFunctionRegistry;
    private static AtomicBoolean singleLetonDone = new AtomicBoolean(false);

    private Map<String,Map<String,PlFunctionDescription>> oraclePlfuncs = new HashMap<>();
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private boolean loaded = false;

    public List<String> listOraclePlFunc(String db){
        readLock();
        try {
            if(oraclePlfuncs.get(db) != null){
                List<String> result = new ArrayList<>();
                result.addAll(oraclePlfuncs.get(db).keySet());
                return result;
            } else {
                return new ArrayList<>();
            }
        } finally {
            readUnLock();
        }
    }

    public boolean isLoaded(){
        return loaded;
    }

    public boolean loadOraclePlFuncFromMetadata(List<PlFunctionDescription> list){
        writeLock();
        try {
            if(!loaded){
                for(PlFunctionDescription f : list){
                    registerOraclePlFunc(f);
                }
                loaded = true;
            }
            return loaded;
        } finally {
            writeUnLock();
        }
    }

    public PlFunctionDescription getOraclePlFunc(PlFunctionIdentify id) {
        readLock();
        try {
            if(id != null){
                if(oraclePlfuncs.get(id.getDb()) != null){
                    return oraclePlfuncs.get(id.getDb()).get(id.getName());
                } else {
                    return null;
                }
            } else {
                return null;
            }
        } finally {
            readUnLock();
        }
    }

    public boolean registerOrReplaceOraclePlFunc(PlFunctionDescription function) {
        logger.info("register function : " + function.getCode());
        writeLock();
        try {
            if(function != null){
                Map<String, PlFunctionDescription> dbfuncs = oraclePlfuncs.get(function.getFunc().getDb());
                if(dbfuncs == null){
                    dbfuncs = new HashMap<>();
                    dbfuncs.put(function.getFunc().getName(), function);
                    oraclePlfuncs.put(function.getFunc().getDb(), dbfuncs);
                    return true;
                } else {
                    dbfuncs.put(function.getFunc().getName(), function);
                    return true;
                }
            } else {
                return false;
            }
        } finally {
            writeUnLock();
        }
    }

    public boolean registerOraclePlFunc(PlFunctionDescription function) {
        logger.info("register oracle function : " + function.getCode());
        writeLock();
        try {
            if(function != null){
                Map<String, PlFunctionDescription> dbfuncs = oraclePlfuncs.get(function.getFunc().getDb());
                if(dbfuncs == null){
                    dbfuncs = new HashMap<>();
                    dbfuncs.put(function.getFunc().getName(), function);
                    oraclePlfuncs.put(function.getFunc().getDb(), dbfuncs);
                    return true;
                } else {
                    if(dbfuncs.get(function.getFunc().getName()) != null){
                        return false;
                    } else {
                        dbfuncs.put(function.getFunc().getName(), function);
                        return true;
                    }
                }
            } else {
                return false;
            }
        } finally {
            writeUnLock();
        }
    }

    private void readLock(){
        lock.readLock().lock();
    }
    private void readUnLock(){
        lock.readLock().unlock();
    }
    private void writeLock(){
        lock.writeLock().lock();
    }
    private void writeUnLock(){
        lock.writeLock().unlock();
    }

    public static PlFunctionRegistry getInstance(){
        if(singleLetonDone.get() == false){
            synchronized (singleLetonDone) {
                if(plFunctionRegistry == null){
                    plFunctionRegistry = new PlFunctionRegistry();
                    singleLetonDone.set(true);
                }
            }
        }
        return plFunctionRegistry;
    }

    public static class PlFunctionDescription {
        private PlFunctionIdentify func;
        private String body;
        private String md5;
        private String code;
        private String returnType;
        private List<PlFunctionIdentify> childPlfuncs;
        public PlFunctionDescription(PlFunctionIdentify func, String body, String md5, String code, String returnType, List<PlFunctionIdentify> childPlfuncs){
            this.func = func;
            this.body = body;
            this.md5 = md5;
            this.code = code;
            this.returnType = returnType;
            this.childPlfuncs = childPlfuncs;
        }

        public PlFunctionIdentify getFunc() {
            return func;
        }

        public void setFunc(PlFunctionIdentify func) {
            this.func = func;
        }

        public String getBody() {
            return body;
        }

        public void setBody(String body) {
            this.body = body;
        }

        public String getMd5() {
            return md5;
        }

        public void setMd5(String md5) {
            this.md5 = md5;
        }

        public String getCode() {
            return code;
        }

        public void setCode(String code) {
            this.code = code;
        }

        public String getReturnType() {
            return returnType;
        }

        public void setReturnType(String returnType) {
            this.returnType = returnType;
        }

        public List<PlFunctionIdentify> getChildPlfuncs() {
            return childPlfuncs;
        }

        public void setChildPlfuncs(List<PlFunctionIdentify> childPlfuncs) {
            this.childPlfuncs = childPlfuncs;
        }
    }

    public static class PlFunctionIdentify {
        private String name;
        private String db;
        public PlFunctionIdentify(String name, String db){
            this.name = name;
            this.db = db;
        }
        public String getName(){
            return name;
        }
        public String getDb(){
            return db;
        }

        public String toString(){
            return db + "." + name;
        }

        public int hashCode(){
            return Objects.hash(name,db);
        }

        public boolean equals(Object obj) {
            if(obj == null){
                return false;
            }
            if (obj instanceof PlFunctionIdentify) {
                PlFunctionIdentify o = (PlFunctionIdentify) obj;
                return this.name.equalsIgnoreCase(o.getName()) && this.db.equalsIgnoreCase(o.getDb());
            } else {
                return false;
            }
        }
    }

}
