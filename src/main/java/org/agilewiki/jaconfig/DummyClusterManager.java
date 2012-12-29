package org.agilewiki.jaconfig;

public class DummyClusterManager extends RestartableServer {
    @Override
    protected String serverName() {
        return "clusterManager";
    }
}
