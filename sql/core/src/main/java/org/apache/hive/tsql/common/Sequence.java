package org.apache.hive.tsql.common;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhongdg1 on 2016/12/13.
 * 用于生产nodeID
 */
public class Sequence {
    private AtomicInteger nodeId = new AtomicInteger(0);//节点ID

    private static class SequenceHolder {
        private final static Sequence sequence = new Sequence();

    }

    private Sequence() {
    }


    public static Integer getNodeId() {
        return SequenceHolder.sequence.nodeId.getAndIncrement();
    }

}
