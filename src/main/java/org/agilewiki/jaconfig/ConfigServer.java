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

import org.agilewiki.jactor.RP;
import org.agilewiki.jactor.factory.JAFactory;
import org.agilewiki.jasocket.jid.PrintJid;
import org.agilewiki.jasocket.node.Node;
import org.agilewiki.jasocket.server.Server;
import org.agilewiki.jfile.JFile;
import org.agilewiki.jfile.JFileFactories;
import org.agilewiki.jfile.block.Block;
import org.agilewiki.jfile.block.LA32Block;
import org.agilewiki.jid.JidFactories;
import org.agilewiki.jid.scalar.vlens.actor.RootJid;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class ConfigServer extends Server {
    private JFile dbFile;
    private RootJid rootJid;

    @Override
    protected String serverName() {
        return "configServer";
    }

    protected int maxSize() {
        return 1000000000;
    }

    @Override
    protected void startServer(final PrintJid out, final RP rp) throws Exception {
        (new JFileFactories()).initialize(getParent());
        node().factory().registerActorFactory(TimeValueJidFactory.fac);
        Path dbPath = new File(node().nodeDirectory(), "config.db").toPath();
        dbFile = new JFile();
        dbFile.initialize(getMailbox());
        dbFile.open(
                dbPath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);
        Block block0 = newDbBlock();
        block0.setCurrentPosition(0L);
        block0.setFileName(dbPath.toString());
        dbFile.readRootJid(block0, maxSize());
        if (block0.isEmpty()) {
            JAFactory factory = (JAFactory) getAncestor(JAFactory.class);
            block0.setRootJid(
                    (RootJid) factory.newActor(
                            JidFactories.ROOT_JID_TYPE,
                            getMailbox(),
                            dbFile));
        }
        rootJid = block0.getRootJid(getMailbox(), getParent());
    }

    @Override
    public void close() {
        if (dbFile != null) {
            dbFile.close();
            dbFile = null;
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
        } catch (Exception ex) {
            node.mailboxFactory().close();
            throw ex;
        }
    }
}
