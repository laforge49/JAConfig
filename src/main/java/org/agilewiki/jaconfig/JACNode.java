/*
 * Copyright 2013 Bill La Forge
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
import org.agilewiki.jaconfig.quorum.QuorumServer;
import org.agilewiki.jaconfig.rank.simple.SimpleRanker;
import org.agilewiki.jasocket.node.IntCon;
import org.agilewiki.jasocket.node.Node;
import org.apache.sshd.server.PasswordAuthenticator;

public class JACNode extends Node {
    public ConfigServer configServer;

    public JACNode(String[] args, int threadCount) throws Exception {
        super(args, threadCount);
    }

    public PasswordAuthenticator passwordAuthenticator() {
        return configServer;
    }

    public static void main(String[] args) throws Exception {
        Node node = new JACNode(args, 100);
        try {
            node.process();
            node.startup(ConfigServer.class, "");
            node.startup(SimpleRanker.class, "ranker");
            node.startup(QuorumServer.class, "kingmaker");
            node.startup(KingmakerServer.class, ClusterManager.class.getName() + " " + HostManager.class.getName());
            if (args.length == 0)
                (new IntCon()).create(node);
        } catch (Exception ex) {
            node.mailboxFactory().close();
            throw ex;
        }
    }
}
