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
package org.agilewiki.jaconfig.db;

import org.agilewiki.jactor.RP;
import org.agilewiki.jactor.factory.JAFactory;
import org.agilewiki.jasocket.JASocketFactories;
import org.agilewiki.jasocket.agentChannel.AgentChannel;
import org.agilewiki.jasocket.agentChannel.ShipAgent;
import org.agilewiki.jasocket.cluster.ShipAgentEventToAll;
import org.agilewiki.jasocket.jid.PrintJid;
import org.agilewiki.jasocket.node.ConsoleApp;
import org.agilewiki.jasocket.node.Node;
import org.agilewiki.jasocket.server.Server;
import org.agilewiki.jasocket.server.ServerCommand;
import org.agilewiki.jasocket.serverNameListener.ServerNameListener;
import org.agilewiki.jfile.JFile;
import org.agilewiki.jfile.JFileFactories;
import org.agilewiki.jfile.block.Block;
import org.agilewiki.jfile.block.LA32Block;
import org.agilewiki.jid.JidFactories;
import org.agilewiki.jid.collection.vlenc.map.MapEntry;
import org.agilewiki.jid.collection.vlenc.map.StringBMapJid;
import org.agilewiki.jid.collection.vlenc.map.StringBMapJidFactory;
import org.agilewiki.jid.scalar.vlens.actor.RootJid;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.HashSet;

public class ConfigServer extends Server implements ServerNameListener {
    public static final String NAME_TIME_VALUE_TYPE = "nameTimeValue";
    public static final StringBMapJidFactory nameTimeValueMapFactory = new StringBMapJidFactory(
            NAME_TIME_VALUE_TYPE, TimeValueJidFactory.fac);
    public static final String TOTAL_HOST_COUNT = "totalHostCount";

    private HashMap<String, HashSet<String>> hosts = new HashMap<String, HashSet<String>>();
    private int totalHostCount;
    private JFile jFile;
    private RootJid rootJid;
    private StringBMapJid<TimeValueJid> map;
    private Block block;
    private boolean quarum;

    @Override
    protected String serverName() {
        return "config";
    }

    protected int maxSize() {
        return 1000000000;
    }

    @Override
    protected void startServer(final PrintJid out, final RP rp) throws Exception {
        String myAddress = agentChannelManager().agentChannelManagerAddress();
        int p = myAddress.indexOf(":");
        String myipa = myAddress.substring(0, p);
        String myp = myAddress.substring(p + 1);
        HashSet<String> ps = new HashSet<String>();
        ps.add(myp);
        hosts.put(myipa, ps);
        (new JFileFactories()).initialize(getParent());
        JASocketFactories f = node().factory();
        f.registerActorFactory(AssignAgentFactory.fac);
        f.registerActorFactory(TimeValueJidFactory.fac);
        f.registerActorFactory(nameTimeValueMapFactory);
        Path dbPath = new File(node().nodeDirectory(), "config.db").toPath();
        jFile = new JFile();
        jFile.initialize(getMailbox(), getParent());
        jFile.open(
                dbPath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);
        block = newDbBlock();
        block.setCurrentPosition(0L);
        block.setFileName(dbPath.toString());
        jFile.readRootJid(block, maxSize());
        if (block.isEmpty()) {
            JAFactory factory = (JAFactory) getAncestor(JAFactory.class);
            block.setRootJid(
                    (RootJid) factory.newActor(
                            JidFactories.ROOT_JID_TYPE,
                            getMailbox(),
                            jFile));
        }
        rootJid = block.getRootJid(getMailbox(), getParent());
        rootJid.makeValue(NAME_TIME_VALUE_TYPE);
        map = (StringBMapJid<TimeValueJid>) rootJid.getValue();
        agentChannelManager().subscribeServerNameNotifications(this);
        registerServerCommand(new ServerCommand("values", "list all names and their assigned values") {
            @Override
            public void eval(String args, PrintJid out, RP<PrintJid> rp) throws Exception {
                int s = map.size();
                int i = 0;
                while (i < s) {
                    MapEntry<String, TimeValueJid> me = map.iGet(i);
                    String name = me.getKey();
                    TimeValueJid tv = me.getValue();
                    String value = tv.getValue();
                    if (value.length() > 0)
                        out.println(name + " = " + value);
                    i += 1;
                }
                rp.processResponse(out);
            }
        });
        registerServerCommand(new ServerCommand("assign", "set a name to a value") {
            @Override
            public void eval(String args, PrintJid out, RP<PrintJid> rp) throws Exception {
                if (args.length() == 0) {
                    out.println("missing name");
                } else {
                    String name = args;
                    String value = "";
                    int i = args.indexOf(" ");
                    if (i > -1) {
                        name = args.substring(0, i);
                        value = args.substring(i + 1).trim();
                    }
                    long timestamp = System.currentTimeMillis();
                    if (assign(name, timestamp, value))
                        out.println("OK");
                    else
                        throw new ClockingException();
                }
                rp.processResponse(out);
            }
        });
        super.startServer(out, rp);
        int nthc = 0;
        String thc = get(TOTAL_HOST_COUNT);
        if (thc != null)
            nthc = Integer.valueOf(thc);
        if (nthc > 0)
            totalHostCountUpdate(nthc);
    }

    public boolean assign(String name, long timestamp, String value) throws Exception {
        map.kMake(name);
        TimeValueJid tv = map.kGet(name);
        long oldTimestamp = tv.getTimestamp();
        String oldValue = tv.getValue();
        if (timestamp < oldTimestamp)
            return false;
        if (timestamp == oldTimestamp && value.compareTo(oldValue) <= 0)
            return false;
        tv.setTimestamp(timestamp);
        tv.setValue(value);
        block.setCurrentPosition(0L);
        jFile.writeRootJid(block, maxSize());
        AssignAgent assignAgent = (AssignAgent)
                JAFactory.newActor(this, AssignAgentFactory.ASSIGN_AGENT, getMailbox(), this);
        assignAgent.set(serverName(), name, timestamp, value);
        (new ShipAgentEventToAll(assignAgent)).sendEvent(this, agentChannelManager());
        if (name.equals(TOTAL_HOST_COUNT)) {
            int nthc = 0;
            if (value.length() > 0)
                nthc = Integer.valueOf(value);
            if (nthc != totalHostCount) {
                totalHostCountUpdate(nthc);
            }
        }
        return true;
    }

    public String get(String name) throws Exception {
        TimeValueJid tv = map.kGet(name);
        return tv.getValue();
    }

    @Override
    public void close() {
        if (jFile != null) {
            jFile.close();
            jFile = null;
        }
        super.close();
    }

    protected Block newDbBlock() {
        return new LA32Block();
    }

    public static void main(String[] args) throws Exception {
        Node node = new Node(args, 100);
        try {
            node.process();
            node.startup(ConfigServer.class, "");
            (new ConsoleApp()).create(node);
        } catch (Exception ex) {
            node.mailboxFactory().close();
            throw ex;
        }
    }

    @Override
    public void serverNameAdded(String address, String name) throws Exception {
        if (!serverName().equals(name))
            return;
        if (agentChannelManager().isLocalAddress(address))
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
        int s = map.size();
        int i = 0;
        while (i < s) {
            MapEntry<String, TimeValueJid> mapEntry = map.iGet(i);
            TimeValueJid tv = mapEntry.getValue();
            AssignAgent assignAgent = (AssignAgent)
                    JAFactory.newActor(this, AssignAgentFactory.ASSIGN_AGENT, getMailbox(), this);
            assignAgent.set(serverName(), mapEntry.getKey(), tv.getTimestamp(), tv.getValue());
            ShipAgent shipAgent = new ShipAgent(assignAgent);
            shipAgent.sendEvent(this, agentChannel);
            i += 1;
        }
    }

    @Override
    public void serverNameRemoved(String address, String name) {
        int k = address.indexOf(':');
        String ipa = address.substring(0, k);
        String p = address.substring(k + 1);
        HashSet<String> hps = hosts.get(ipa);
        if (hps != null) {
            hps.remove(p);
            if (hps.size() == 0) {
                hosts.remove(ipa);
            }
        }
    }

    private void totalHostCountUpdate(int nthc) {
        totalHostCount = nthc;
        boolean nq = hosts.size() >= (totalHostCount / 2 + 1);
        if (nq != quarum)
            if (nq)
                quarumAchieved();
            else
                quarumLost();
    }

    public void quarumAchieved() {
        quarum = true;
    }

    public void quarumLost() {
        quarum = false;
    }
}
