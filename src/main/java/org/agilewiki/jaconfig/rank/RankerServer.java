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
package org.agilewiki.jaconfig.rank;

import org.agilewiki.jactor.RP;
import org.agilewiki.jasocket.agentChannel.AgentChannel;
import org.agilewiki.jasocket.jid.PrintJid;
import org.agilewiki.jasocket.server.Server;
import org.agilewiki.jasocket.server.ServerCommand;

import java.util.Iterator;
import java.util.List;

abstract public class RankerServer extends Server {
    abstract public void ranking(String serverName, RP<List<String>> rp) throws Exception;

    @Override
    protected String serverName() {
        return "ranker";
    }

    protected void startServer(PrintJid out, RP rp) throws Exception {
        registerServerCommand(new ServerCommand("nodes", "list nodes, least busy first") {
            @Override
            public void eval(String operatorName,
                             String id,
                             AgentChannel agentChannel,
                             String args,
                             final PrintJid out,
                             long requestId,
                             final RP<PrintJid> rp) throws Exception {
                ranking(args, new RP<List<String>>() {
                    @Override
                    public void processResponse(List<String> response) throws Exception {
                        Iterator<String> it = response.iterator();
                        while (it.hasNext())
                            out.println(it.next());
                        rp.processResponse(out);
                    }
                });
            }
        });
        super.startServer(out, rp);
    }
}
