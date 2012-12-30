package org.agilewiki.jaconfig;

import org.agilewiki.jaconfig.db.ConfigListener;
import org.agilewiki.jaconfig.db.SubscribeConfig;
import org.agilewiki.jaconfig.db.UnsubscribeConfig;
import org.agilewiki.jaconfig.db.impl.ConfigServer;
import org.agilewiki.jaconfig.rank.RankerServer;
import org.agilewiki.jaconfig.rank.Ranking;
import org.agilewiki.jactor.RP;
import org.agilewiki.jactor.lpc.JLPCActor;
import org.agilewiki.jasocket.JASocketFactories;
import org.agilewiki.jasocket.cluster.GetLocalServer;
import org.agilewiki.jasocket.cluster.SubscribeServerNameNotifications;
import org.agilewiki.jasocket.cluster.UnsubscribeServerNameNotifications;
import org.agilewiki.jasocket.jid.PrintJid;
import org.agilewiki.jasocket.node.Node;
import org.agilewiki.jasocket.server.Server;
import org.agilewiki.jasocket.server.Startup;
import org.agilewiki.jasocket.serverNameListener.ServerNameListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

public class ClusterManager extends ManagedServer implements ServerNameListener, ConfigListener {
    public static Logger logger = LoggerFactory.getLogger(ClusterManager.class);

    private ConfigServer configServer;
    private RankerServer rankerServer;
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
                (new GetLocalServer("ranker")).send(ClusterManager.this, agentChannelManager(), new RP<JLPCActor>() {
                    @Override
                    public void processResponse(JLPCActor response) throws Exception {
                        rankerServer = (RankerServer) response;
                        ClusterManager.super.startServer(out, rp);
                    }
                });
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

    private void startup(final String name) throws Exception {
        final String args = serverConfigs.get(name);
        Thread.sleep(1000);
        Ranking.req.send(this, rankerServer, new RP<List<String>>() {
            @Override
            public void processResponse(List<String> response) throws Exception {
                String address = response.get(0);
                if (agentChannelManager().isLocalAddress(address)) {
                    localStartup(name, args);
                } else {
                    System.out.println("todo--remote startup");
                }
            }
        });
    }

    private void localStartup(String name, String args) throws Exception {
        int i = args.indexOf(' ');
        String serverClassName = args;
        if (i > -1) {
            serverClassName = args.substring(0, i);
            args = args.substring(i + 1).trim();
        } else {
            args = "";
        }
        args = name + " " + args;
        Node node = agentChannelManager().node;
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        final Class<Server> serverClass = (Class<Server>) classLoader.loadClass(serverClassName);
        ManagedServer managedServer = (ManagedServer) node.initializeServer(serverClass);
        final PrintJid out = (PrintJid) node().factory().newActor(
                JASocketFactories.PRINT_JID_FACTORY,
                node().mailboxFactory().createMailbox());
        Startup startup = new Startup(node, args, out);
        startup.send(this, managedServer, new RP<PrintJid>() {
            @Override
            public void processResponse(PrintJid response) throws Exception {
                StringBuilder sb = new StringBuilder();
                sb.append(serverClass.getName() + ":\n");
                out.appendto(sb);
                logger.info(sb.toString().trim());
            }
        });
    }

    private void shutdown(String name, String address) throws Exception {

    }
}
