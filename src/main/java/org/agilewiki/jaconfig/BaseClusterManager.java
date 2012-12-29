package org.agilewiki.jaconfig;

public class BaseClusterManager extends RestartableServer {
    @Override
    protected String serverName() {
        return "clusterManager";
    }
}
