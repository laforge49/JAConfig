/*
 * Copyright 2012 Bill La Forge
 *
 * This file is part of AgileWiki and is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License (LGPL) as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This code is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 * or navigate to the following url http://www.gnu.org/licenses/lgpl-2.1.txt
 *
 * Note however that only Scala, Java and JavaScript files are being covered by LGPL.
 * All other files are covered by the Common Public License (CPL).
 * A copy of this license is also included and can be
 * found as well at http://www.opensource.org/licenses/cpl1.0.txt
 */
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
    protected String applicableHostPrefix;
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private ConfigServer configServer;
    private String configPrefix;
    private HashMap<String, TreeSet<String>> serverAddresses = new HashMap<String, TreeSet<String>>();
    private HashMap<String, String> serverConfigs = new HashMap<String, String>();
    private HashSet<String> restart = new HashSet<String>();
    private boolean initialized;

    protected boolean isApplicableHost(String address) throws Exception {
        if (applicableHostPrefix == null) {
            applicableHostPrefix = "";
        }
        return true;
    }

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
        System.out.println("!");
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
        if (!name.startsWith(configPrefix)) {
            return;
        }
        if (name.length() <= configPrefix.length()) {
            logger.error("invalid server name (missing server name postfix): " + name);
            return;
        }
        if (!isApplicableHost(address))
            return;
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
            restart.add(name);
            shutdown(name, saddresses.first());
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
        if (!isApplicableHost(address))
            return;
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
        isApplicableHost("");
        StartupServer startupServer = new StartupServer(
                applicableHostPrefix,
                "*" + serverName() + "*",
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
                        serverEvalAgent.configure("*" + serverName() + "*", null, name + " shutdown");
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
