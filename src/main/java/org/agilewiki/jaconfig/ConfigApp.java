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

import org.agilewiki.jasocket.JASApplication;
import org.agilewiki.jasocket.JASocketFactories;
import org.agilewiki.jasocket.node.Node;
import org.agilewiki.jfile.JFileFactories;
import org.agilewiki.jfile.transactions.db.inMemory.IMDB;

import java.io.File;
import java.nio.file.Path;

public class ConfigApp implements JASApplication {
    private Node node;
    private IMDB configIMDB;

    @Override
    public void create(Node node, String[] args, JASocketFactories factory) throws Exception {
        this.node = node;
        (new JFileFactories()).initialize(factory);
    }

    @Override
    public void open() throws Exception {
        Path dbPath = new File(node.nodeDirectory(), "configDB").toPath();
        configIMDB = new IMDB(node.mailboxFactory(), node.agentChannelManager(), dbPath);
        ConfigDB configDB = new ConfigDB(node, 1024 * 1024, configIMDB);
        configDB.initialize(node.mailboxFactory().createAsyncMailbox(), node.agentChannelManager());
        OpenConfigDB.req.sendEvent(configDB);
    }

    @Override
    public void close() {
        if (configIMDB != null)
            configIMDB.closeDbFile();
    }

    public static void main(String[] args) throws Exception {
        Node node = new Node(100);
        try {
            node.addApplication(new ConfigApp());
            node.process(args);
        } catch (Exception ex) {
            node.mailboxFactory().close();
            throw ex;
        }
    }
}
