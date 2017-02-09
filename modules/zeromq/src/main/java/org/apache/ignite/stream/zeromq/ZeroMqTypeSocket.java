package org.apache.ignite.stream.zeromq;

import org.zeromq.ZMQ;

public enum ZeroMqTypeSocket {
    PAIR(ZMQ.PAIR),
    SUB(ZMQ.SUB),
    PULL(ZMQ.PULL);

    private int type;

    ZeroMqTypeSocket(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public static boolean check(ZeroMqTypeSocket type) {
        for (ZeroMqTypeSocket ts : ZeroMqTypeSocket.values()) {
            if (ts.getType() == type.getType())
                return true;
        }
        return false;
    }
}
