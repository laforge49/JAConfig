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
import org.agilewiki.jactor.RP;
import org.agilewiki.jactor.lpc.JLPCActor;
import org.agilewiki.jasocket.cluster.GetLocalServer;
import org.agilewiki.jasocket.jid.PrintJid;
import org.agilewiki.jasocket.server.Server;

public class QuorumServer extends Server implements ConfigListener {
    protected ConfigServer configServer;

    @Override
    protected void startServer(final PrintJid out, final RP rp) throws Exception {
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
            (new UnsubscribeConfig(this)).sendEvent(this, configServer);
        } catch (Exception ex) {}
        super.close();
    }

    @Override
    public void quorum(boolean status) {
        if (!status)
            close();
    }

    @Override
    public void assigned(String name, String value) {
    }
}
