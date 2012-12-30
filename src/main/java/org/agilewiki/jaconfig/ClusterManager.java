package org.agilewiki.jaconfig;

import org.agilewiki.jaconfig.db.ConfigListener;
import org.agilewiki.jaconfig.db.SubscribeConfig;
import org.agilewiki.jaconfig.db.UnsubscribeConfig;
import org.agilewiki.jaconfig.db.impl.ConfigServer;
import org.agilewiki.jactor.RP;
import org.agilewiki.jactor.lpc.JLPCActor;
import org.agilewiki.jasocket.cluster.GetLocalServer;
import org.agilewiki.jasocket.cluster.SubscribeServerNameNotifications;
import org.agilewiki.jasocket.cluster.UnsubscribeServerNameNotifications;
import org.agilewiki.jasocket.jid.PrintJid;
import org.agilewiki.jasocket.serverNameListener.ServerNameListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class ClusterManager extends ManagedServer implements ServerNameListener, ConfigListener {
    public static Logger logger = LoggerFactory.getLogger(ClusterManager.class);

    private ConfigServer configServer;
    private String configPrefix;
    private HashMap<String, HashSet<String>> serverAddresses = new HashMap<String, HashSet<String>>();
    private HashMap<String, String> serverConfigs = new HashMap<String, String>();

    @Override
    protected void startServer(final PrintJid out, final RP rp) throws Exception {
        configPrefix = serverName() + ".";
        (new SubscribeServerNameNotifications(this)).sendEvent(this, agentChannelManager());
        (new GetLocalServer("config")).send(this, agentChannelManager(), new RP<JLPCActor>() {
            @Override
            public void processResponse(JLPCActor response) throws Exception {
                configServer = (ConfigServer) response;
                (new SubscribeConfig(ClusterManager.this)).sendEvent(ClusterManager.this, configServer);
                ClusterManager.super.startServer(out, rp);
            }
        });
    }

    @Override
    public void close() {
        try {
            (new UnsubscribeServerNameNotifications(this)).sendEvent(this, agentChannelManager());
            (new UnsubscribeConfig(this)).sendEvent(this, configServer);
        } catch (Exception ex) {
        }
        super.close();
    }

    @Override
    public void serverNameAdded(String address, String name) throws Exception {
        if (!name.startsWith(configPrefix))
            return;
        if (name.length() <= configPrefix.length()) {
            logger.error("invalid server name (missing server name postfix): " + name);
            return;
        }
        HashSet<String> saddresses = serverAddresses.get(name);
        if (saddresses == null) {
            saddresses = new HashSet<String>();
            serverAddresses.put(name, saddresses);
        }
        saddresses.add(address);
        if (!serverConfigs.containsKey(name) || saddresses.size() > 1)
            shutdown(name, address);
    }

    @Override
    public void serverNameRemoved(String address, String name) throws Exception {
        if (!name.startsWith(configPrefix))
            return;
        if (name.length() <= configPrefix.length()) {
            logger.error("invalid server name (missing server name postfix): " + name);
            return;
        }
        HashSet<String> saddresses = serverAddresses.get(name);
        if (saddresses == null)
            return;
        saddresses.remove(address);
        if (saddresses.isEmpty()) {
            serverAddresses.remove(name);
            if (serverConfigs.containsKey(name))
                startup(name);
        }
    }

    @Override
    public void assigned(String name, String value) throws Exception {
        if (!name.startsWith(configPrefix))
            return;
        if (name.length() <= configPrefix.length()) {
            logger.error("invalid configuration name (missing server name postfix): " + name);
            return;
        }
        if (value.length() == 0) {
            serverConfigs.remove(name);
        } else {
            serverConfigs.put(name, value);
        }
        if (!serverAddresses.containsKey(name)) {
            startup(name);
            return;
        }
        HashSet<String> saddresses = serverAddresses.get(name);
        Iterator<String> it = saddresses.iterator();
        while (it.hasNext()) {
            String address = it.next();
            shutdown(name, address);
        }
    }

    private void startup(String name) throws Exception {

    }

    private void shutdown(String name, String address) throws Exception {

    }
}
