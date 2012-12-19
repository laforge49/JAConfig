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
import org.agilewiki.jasocket.application.Application;
import org.agilewiki.jasocket.node.Node;
import org.agilewiki.jfile.JFileFactories;
import org.agilewiki.jfile.transactions.db.OpenDbFile;
import org.agilewiki.jfile.transactions.db.inMemory.IMDB;
import org.agilewiki.jid.collection.vlenc.BListJid;
import org.agilewiki.jid.scalar.vlens.string.StringJid;

import java.io.File;
import java.nio.file.Path;

public class ConfigServer extends Application {
    private IMDB configIMDB;

    @Override
    protected String applicationName() {
        return "configServer";
    }

    @Override
    protected void startApplication(final BListJid<StringJid> out, final RP rp) throws Exception {
        (new JFileFactories()).initialize(node().factory());
        Path dbPath = new File(node().nodeDirectory(), "configDB").toPath();
        configIMDB = new IMDB(getMailboxFactory(), node().agentChannelManager(), dbPath);
        OpenDbFile openDbFile = new OpenDbFile(1024 * 1024);
        openDbFile.send(this, configIMDB, new RP() {
            @Override
            public void processResponse(Object response) throws Exception {
                ConfigServer.super.startApplication(out, rp);
            }
        });
    }

    @Override
    public void close() {
        if (configIMDB != null)
            configIMDB.closeDbFile();
        super.close();
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
