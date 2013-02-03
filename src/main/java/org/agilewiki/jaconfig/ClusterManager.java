package org.agilewiki.jaconfig;

import org.agilewiki.jaconfig.db.ConfigListener;
import org.agilewiki.jaconfig.db.SubscribeConfig;
import org.agilewiki.jaconfig.db.UnsubscribeConfig;
import org.agilewiki.jaconfig.db.impl.ConfigServer;
import org.agilewiki.jaconfig.quorum.StartupServer;
import org.agilewiki.jactor.RP;
import org.agilewiki.jasocket.agentChannel.AgentChannel;
import org.agilewiki.jasocket.agentChannel.ShipAgent;
import org.agilewiki.jasocket.cluster.GetAgentChannel;
import org.agilewiki.jasocket.cluster.GetLocalServer;
import org.agilewiki.jasocket.cluster.SubscribeServerNameNotifications;
import org.agilewiki.jasocket.cluster.UnsubscribeServerNameNotifications;
import org.agilewiki.jasocket.commands.ServerEvalAgent;
import org.agilewiki.jasocket.commands.ServerEvalAgentFactory;
import org.agilewiki.jasocket.jid.PrintJid;
import org.agilewiki.jasocket.server.Server;
import org.agilewiki.jasocket.serverNameListener.ServerNameListener;
import org.agilewiki.jid.Jid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;

public class ClusterManager extends ManagedServer implements ServerNameListener, ConfigListener {
    public static Logger logger = LoggerFactory.getLogger(ClusterManager.class);

    private ConfigServer configServer;
    private String configPrefix;
    private HashMap<String, TreeSet<String>> serverAddresses = new HashMap<String, TreeSet<String>>();
    private HashMap<String, String> serverConfigs = new HashMap<String, String>();
    private HashSet<String> restart = new HashSet<String>();
    private boolean initialized;

    @Override
    protected void startManagedServer(final PrintJid out, final RP rp) throws Exception {
        configPrefix = serverName() + ".";
        (new GetLocalServer("config")).send(this, agentChannelManager(), new RP<Server>() {
            @Override
            public void processResponse(Server response) throws Exception {
                configServer = (ConfigServer) response;
                (new SubscribeConfig(ClusterManager.this)).
                        send(ClusterManager.this, configServer, new RP<Boolean>() {
                            @Override
                            public void processResponse(Boolean subscribedConfig) throws Exception {
                                (new SubscribeServerNameNotifications(ClusterManager.this)).
                                        send(ClusterManager.this, agentChannelManager(), new RP<Boolean>() {
                                            @Override
                                            public void processResponse(Boolean response) throws Exception {
                                                ClusterManager.super.startManagedServer(out, new RP() {
                                                    @Override
                                                    public void processResponse(Object response) throws Exception {
                                                        initialized = true;
                                                        perform();
                                                        rp.processResponse(response);
                                                    }
                                                });
                                            }
                                        });
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

    private void perform() throws Exception {
        Iterator<String> vit = serverConfigs.keySet().iterator();
        while (vit.hasNext()) {
            String name = vit.next();
            String value = serverConfigs.get(name);
            if (value.length() == 0 ||
                    !name.startsWith(configPrefix) ||
                    name.length() <= configPrefix.length() ||
                    serverAddresses.containsKey(name))
                continue;
            startup(name);
        }
        Iterator<String> ait = serverAddresses.keySet().iterator();
        while (ait.hasNext()) {
            String name = ait.next();
            TreeSet<String> saddresses = serverAddresses.get(name);
            String value = serverConfigs.get(name);
            int maxSize = 1;
            if (value == null || value.length() == 0)
                maxSize = 0;
            while (saddresses.size() > maxSize) {
                String address = saddresses.last();
                shutdown(name, address);
            }
            if (maxSize == 0)
                serverAddresses.remove(name);
        }
    }

    @Override
    public void serverNameAdded(String address, String name) throws Exception {
        if (!name.startsWith(configPrefix))
            return;
        if (name.length() <= configPrefix.length()) {
            logger.error("invalid server name (missing server name postfix): " + name);
            return;
        }
        TreeSet<String> saddresses = serverAddresses.get(name);
        if (saddresses == null) {
            saddresses = new TreeSet<String>();
            serverAddresses.put(name, saddresses);
        }
        saddresses.add(address);
        if (!initialized)
            return;
        if (!serverConfigs.containsKey(name) || saddresses.size() > 1) {
            logger.warn("shutdown duplicate " + name + address);
            shutdown(name, address);
        }
    }

    @Override
    public void serverNameRemoved(final String address, final String name) throws Exception {
        if (!name.startsWith(configPrefix))
            return;
        if (name.length() <= configPrefix.length()) {
            logger.error("invalid server name (missing server name postfix): " + name);
            return;
        }
        TreeSet<String> saddresses = serverAddresses.get(name);
        if (saddresses == null)
            return;
        saddresses.remove(address);
        if (saddresses.isEmpty()) {
            serverAddresses.remove(name);
            if (!initialized)
                return;
            if (serverConfigs.containsKey(name))
                if (restart.contains(name))
                    startup(name);
                else if (agentChannelManager().isLocalAddress(address))
                    logger.warn("down: " + name + " " + address);
                else
                    (new GetAgentChannel(address)).send(this, agentChannelManager(), new RP<AgentChannel>() {
                        @Override
                        public void processResponse(AgentChannel response) throws Exception {
                            if (response == null)
                                startup(name);
                            else
                                logger.warn("down: " + name + " " + address);
                        }
                    });
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
        if (!initialized)
            return;
        if (!serverAddresses.containsKey(name)) {
            if (value.length() > 0)
                startup(name);
            return;
        }
        TreeSet<String> saddresses = serverAddresses.get(name);
        Iterator<String> it = saddresses.iterator();
        while (it.hasNext()) {
            String address = it.next();
            if (value.length() > 0)
                restart.add(name);
            shutdown(name, address);
        }
    }

    private void startup(String name) throws Exception {
        String args = serverConfigs.get(name);
        String serverClass = args;
        String serverArgs = "";
        int i = args.indexOf(' ');
        if (i > -1) {
            serverClass = args.substring(0, i);
            serverArgs = args.substring(i + 1).trim();
        }
        StartupServer startupServer = new StartupServer(
                "*clusterManager*",
                name,
                serverClass,
                serverArgs,
                "ranker");
        startupServer.sendEvent(this, quorumServer);
    }

    private void shutdown(final String name, String address) throws Exception {
        if (agentChannelManager().isLocalAddress(address)) {
            localShutdown(name);
        } else {
            (new GetAgentChannel(address)).send(ClusterManager.this, agentChannelManager(), new RP<AgentChannel>() {
                @Override
                public void processResponse(AgentChannel response) throws Exception {
                    if (response != null) {
                        ServerEvalAgent serverEvalAgent = (ServerEvalAgent) node().factory().newActor(
                                ServerEvalAgentFactory.fac.actorType, getMailbox());
                        serverEvalAgent.configure("*clusterManager*", null, "shutdown");
                        (new ShipAgent(serverEvalAgent)).send(ClusterManager.this, response, new RP<Jid>() {
                            @Override
                            public void processResponse(Jid response) throws Exception {
                                PrintJid out = (PrintJid) response;
                                StringBuilder sb = new StringBuilder();
                                sb.append(name + ":\n");
                                out.appendto(sb);
                                logger.info(sb.toString().trim());
                            }
                        });
                    }
                }
            });
        }
    }

    private void localShutdown(String name) throws Exception {
        (new GetLocalServer(name)).send(this, agentChannelManager(), new RP<Server>() {
            @Override
            public void processResponse(Server response) throws Exception {
                if (response != null)
                    ((ManagedServer) response).close();
            }
        });
    }
}
