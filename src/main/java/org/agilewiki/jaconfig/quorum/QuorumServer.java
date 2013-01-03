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
package org.agilewiki.jaconfig.quorum;

import org.agilewiki.jaconfig.db.ConfigListener;
import org.agilewiki.jaconfig.db.SubscribeConfig;
import org.agilewiki.jaconfig.db.UnsubscribeConfig;
import org.agilewiki.jaconfig.db.impl.ConfigServer;
import org.agilewiki.jactor.RP;
import org.agilewiki.jactor.lpc.JLPCActor;
import org.agilewiki.jasocket.agentChannel.AgentChannel;
import org.agilewiki.jasocket.cluster.GetLocalServer;
import org.agilewiki.jasocket.cluster.SubscribeServerNameNotifications;
import org.agilewiki.jasocket.cluster.UnsubscribeServerNameNotifications;
import org.agilewiki.jasocket.jid.PrintJid;
import org.agilewiki.jasocket.node.ConsoleApp;
import org.agilewiki.jasocket.node.Node;
import org.agilewiki.jasocket.server.Server;
import org.agilewiki.jasocket.serverNameListener.ServerNameListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class QuorumServer extends Server implements ServerNameListener, ConfigListener {
    public static final String TOTAL_HOST_COUNT = "totalHostCount";
    public static Logger logger = LoggerFactory.getLogger(QuorumServer.class);

    protected ConfigServer configServer;
    private HashMap<String, HashSet<String>> hosts = new HashMap<String, HashSet<String>>();
    private int totalHostCount;
    private boolean quorum;
    private HashSet<QuorumListener> listeners = new HashSet<QuorumListener>();
    private String thcName;
    private ArrayDeque<StartupEntry> startupQueue = new ArrayDeque();

    protected String quorumServerName() {
        return startupArgs();
    }

    @Override
    protected String serverName() {
        return "quorum";
    }

    @Override
    protected void startServer(final PrintJid out, final RP rp) throws Exception {
        thcName = serverName() + "." + TOTAL_HOST_COUNT;
        String myAddress = agentChannelManager().agentChannelManagerAddress();
        int p = myAddress.indexOf(":");
        String myipa = myAddress.substring(0, p);
        String myp = myAddress.substring(p + 1);
        HashSet<String> ps = new HashSet<String>();
        ps.add(myp);
        hosts.put(myipa, ps);
        (new SubscribeServerNameNotifications(this)).sendEvent(this, agentChannelManager());
        (new GetLocalServer("config")).send(this, agentChannelManager(), new RP<JLPCActor>() {
            @Override
            public void processResponse(JLPCActor response) throws Exception {
                configServer = (ConfigServer) response;
                (new SubscribeConfig(QuorumServer.this)).sendEvent(QuorumServer.this, configServer);
                QuorumServer.super.startServer(out, rp);
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
    public void assigned(String name, String value) throws Exception {
        int oldthc = totalHostCount;
        if (!thcName.equals(name))
            return;
        if (value.length() == 0) {
            totalHostCount = 0;
            quorumUpdate();
            return;
        }
        totalHostCount = Integer.valueOf(value);
        quorumUpdate();
    }

    public boolean subscribe(QuorumListener listener) throws Exception {
        boolean subscribed = listeners.add(listener);
        (new Quorum(quorum)).sendEvent(this, listener);
        return subscribed;
    }

    public boolean unsubscribe(QuorumListener listener) {
        return listeners.remove(listener);
    }

    @Override
    public void serverNameAdded(String address, String name) throws Exception {
        if (!quorumServerName().equals(name))
            return;
        AgentChannel agentChannel = agentChannelManager().getAgentChannel(address);
        if (agentChannel == null)
            return;
        int k = address.indexOf(':');
        String ipa = address.substring(0, k);
        String p = address.substring(k + 1);
        HashSet<String> hps = hosts.get(ipa);
        if (hps == null) {
            hps = new HashSet<String>();
            hosts.put(ipa, hps);
        }
        hps.add(p);
        quorumUpdate();
    }

    @Override
    public void serverNameRemoved(String address, String name) throws Exception {
        if (!quorumServerName().equals(name))
            return;
        int k = address.indexOf(':');
        String ipa = address.substring(0, k);
        String p = address.substring(k + 1);
        HashSet<String> hps = hosts.get(ipa);
        if (hps != null) {
            hps.remove(p);
            if (hps.size() == 0) {
                hosts.remove(ipa);
                quorumUpdate();
            }
        }
    }

    private void quorumUpdate() throws Exception {
        boolean nq = (totalHostCount > 0) && (hosts.size() >= (totalHostCount / 2 + 1));
        if (nq != quorum)
            setQuorum(nq);
    }

    public void setQuorum(boolean quorum) throws Exception {
        if (!quorum)
            startupQueue.clear();
        this.quorum = quorum;
        logger.info("quorum: " + quorum + " hosts=" + hosts.size() + " quorum=" + (totalHostCount / 2 + 1));
        Iterator<QuorumListener> it = listeners.iterator();
        Quorum q = new Quorum(quorum);
        while (it.hasNext()) {
            q.sendEvent(this, it.next());
        }
    }

    public static void main(String[] args) throws Exception {
        Node node = new Node(args, 100);
        try {
            node.process();
            node.startup(ConfigServer.class, "");
            node.startup(QuorumServer.class, "quorum");
            (new ConsoleApp()).create(node);
        } catch (Exception ex) {
            node.mailboxFactory().close();
            throw ex;
        }
    }

    void processStartupEntry() throws Exception {
    }
}
