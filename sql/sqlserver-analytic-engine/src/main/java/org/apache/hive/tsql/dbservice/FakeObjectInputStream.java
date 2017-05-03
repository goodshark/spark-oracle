package org.apache.hive.tsql.dbservice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**
 * Created by wangsm9 on 2017/5/2.
 */
public class FakeObjectInputStream extends ObjectInputStream {
    private static final Logger LOG = LoggerFactory.getLogger(FakeObjectInputStream.class);

    public FakeObjectInputStream(InputStream in) throws IOException {
        super(in);
    }

    public FakeObjectInputStream() throws SecurityException, IOException {
        super();
    }


 /*   @Override
    protected ObjectStreamClass readClassDescriptor() throws IOException, ClassNotFoundException {
        ObjectStreamClass objInputStream = super.readClassDescriptor();
        Class<?> localClass = Class.forName(objInputStream.getName());
        ObjectStreamClass localInputStream = ObjectStreamClass.lookup(localClass);
        if (localInputStream != null) {
            final long localUID = localInputStream.getSerialVersionUID();
            final long objUID = objInputStream.getSerialVersionUID();
            if (localUID != objUID) {
                return localInputStream;
            }
        }
        return objInputStream;
    }*/

    public boolean checkSerialVersionUIDIsRight() throws Exception {
        boolean flag = false;
        ObjectStreamClass objInputStream = super.readClassDescriptor();
        Class<?> localClass = Class.forName(objInputStream.getName());
        ObjectStreamClass localInputStream = ObjectStreamClass.lookup(localClass);
        if (localInputStream != null) {
            final long localUID = localInputStream.getSerialVersionUID();
            final long objUID = objInputStream.getSerialVersionUID();
            if (localUID == objUID) {
                LOG.warn(" objUID is:" + objUID + ", equal localUID :" + localUID + "");
                flag = true;
            } else {
                LOG.warn(" objUID is:" + objUID + ", localUid is :" + localUID + " ,will update pro Serial object in db");
            }
        }
        return flag;
    }

}
