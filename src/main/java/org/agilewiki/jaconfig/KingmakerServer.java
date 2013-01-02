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

import org.agilewiki.jaconfig.db.impl.ConfigServer;
import org.agilewiki.jaconfig.quorum.QuorumListener;
import org.agilewiki.jaconfig.quorum.QuorumServer;
import org.agilewiki.jaconfig.quorum.SubscribeQuorum;
import org.agilewiki.jaconfig.rank.simple.SimpleRanker;
import org.agilewiki.jactor.RP;
import org.agilewiki.jactor.lpc.JLPCActor;
import org.agilewiki.jasocket.JASocketFactories;
import org.agilewiki.jasocket.cluster.GetLocalServer;
import org.agilewiki.jasocket.cluster.SubscribeServerNameNotifications;
import org.agilewiki.jasocket.jid.PrintJid;
import org.agilewiki.jasocket.node.ConsoleApp;
import org.agilewiki.jasocket.node.Node;
import org.agilewiki.jasocket.server.Server;
import org.agilewiki.jasocket.server.Startup;
import org.agilewiki.jasocket.serverNameListener.ServerNameListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TreeSet;

public class KingmakerServer extends Server implements ServerNameListener, QuorumListener {
    public static Logger logger = LoggerFactory.getLogger(KingmakerServer.class);

    private boolean quorum;
    private ClusterManager clusterManager;
    private TreeSet<String> kingmakers = new TreeSet<String>();
    private TreeSet<String> clusterManagers = new TreeSet<String>();
    private boolean starting;

    @Override
    protected String serverName() {
        return "kingmaker";
    }

    @Override
    protected void startServer(final PrintJid out, final RP rp) throws Exception {
        (new SubscribeServerNameNotifications(this)).sendEvent(this, agentChannelManager());
        (new GetLocalServer("quorum")).send(this, agentChannelManager(), new RP<JLPCActor>() {
            @Override
            public void processResponse(JLPCActor response) throws Exception {
                (new SubscribeQuorum(KingmakerServer.this)).sendEvent(KingmakerServer.this, response);
                KingmakerServer.super.startServer(out, rp);
            }
        });
    }

    @Override
    public void serverNameAdded(String address, String name) throws Exception {
        if ("kingmaker".equals(name))
            kingmakers.add(address);
        else if ("clusterManager".equals(name))
            clusterManagers.add(address);
        if (!quorum)
            return;
    }

    @Override
    public void serverNameRemoved(String address, String name) throws Exception {
        if ("kingmaker".equals(name)) {
            kingmakers.remove(address);
            System.err.println("*************************************************************removed kingmaker");
            perform();
        } else if ("clusterManager".equals(name)) {
            clusterManagers.remove(address);
            System.err.println("*************************************************************removed clusterManager");
            perform();
        }
    }

    @Override
    public void quorum(boolean status) throws Exception {
        quorum = status;
        perform();
    }

    private void perform() throws Exception {
        if (!quorum)
            return;
        if (clusterManagers.size() == 1)
            return;
        if (clusterManagers.isEmpty()) {
            if (kingmakers.isEmpty() || agentChannelManager().isLocalAddress(kingmakers.first())) {
                startClusterManager();
                System.err.println("*************************************************************start clusterManager");
            }
        } else if (clusterManagers.contains(agentChannelManager().agentChannelManagerAddress())) {
            if (!agentChannelManager().isLocalAddress(clusterManagers.last())) {
                clusterManager.close();
                System.err.println("*************************************************************close clusterManager");
            }
        }
    }

    private void startClusterManager() throws Exception {
        if (starting) {
            System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^already started");
            return;
        }
        starting = true;
        System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^starting");
        String args = startupArgs();
        int i = args.indexOf(' ');
        String serverClassName = args;
        if (i > -1) {
            serverClassName = args.substring(0, i);
            args = args.substring(i + 1).trim();
        } else {
            args = "";
        }
        args = "clusterManager " + args;
        Node node = agentChannelManager().node;
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        final Class<Server> serverClass = (Class<Server>) classLoader.loadClass(serverClassName);
        clusterManager = (ClusterManager) node.initializeServer(serverClass);
        final PrintJid out = (PrintJid) node().factory().newActor(
                JASocketFactories.PRINT_JID_FACTORY,
                node().mailboxFactory().createMailbox());
        Startup startup = new Startup(node, args, out);
        startup.send(this, clusterManager, new RP<PrintJid>() {
            @Override
            public void processResponse(PrintJid response) throws Exception {
                StringBuilder sb = new StringBuilder();
                sb.append(serverClass.getName() + ":\n");
                out.appendto(sb);
                logger.info(sb.toString().trim());
                starting = false;
                System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^finished starting");
            }
        });
    }

    public static void main(String[] args) throws Exception {
        Node node = new Node(args, 100);
        try {
            node.process();
            node.startup(ConfigServer.class, "");
            node.startup(SimpleRanker.class, "");
            node.startup(QuorumServer.class, "kingmaker");
            node.startup(KingmakerServer.class, ClusterManager.class.getName());
            (new ConsoleApp()).create(node);
        } catch (Exception ex) {
            node.mailboxFactory().close();
            throw ex;
        }
    }
}
