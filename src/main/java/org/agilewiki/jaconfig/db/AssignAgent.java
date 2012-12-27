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
import org.agilewiki.jactor.lpc.JLPCActor;
import org.agilewiki.jasocket.cluster.GetLocalServer;
import org.agilewiki.jasocket.jid.agent.AgentJid;
import org.agilewiki.jid.Jid;
import org.agilewiki.jid.scalar.flens.lng.LongJid;
import org.agilewiki.jid.scalar.vlens.string.StringJid;

public class AssignAgent extends AgentJid {
    private StringJid configNameJid() throws Exception {
        return (StringJid) _iGet(0);
    }

    private StringJid nameJid() throws Exception {
        return (StringJid) _iGet(1);
    }

    private LongJid timestampJid() throws Exception {
        return (LongJid) _iGet(2);
    }

    private StringJid valueJid() throws Exception {
        return (StringJid) _iGet(3);
    }

    public void set(String configName, String name, long timestamp, String value) throws Exception {
        configNameJid().setValue(configName);
        nameJid().setValue(name);
        timestampJid().setValue(timestamp);
        valueJid().setValue(value);
    }

    @Override
    public void start(final RP<Jid> rp) throws Exception {
        (new GetLocalServer(configNameJid().getValue())).send(this, agentChannelManager(), new RP<JLPCActor>() {
            @Override
            public void processResponse(JLPCActor response) throws Exception {
                if (response == null) {
                    rp.processResponse(null);
                    return;
                }
                Assign assign = new Assign(nameJid().getValue(), timestampJid().getValue(), valueJid().getValue());
                assign.send(AssignAgent.this, response, (RP) rp);
            }
        });
    }
}
