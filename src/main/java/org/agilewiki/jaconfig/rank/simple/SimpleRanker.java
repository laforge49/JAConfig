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
package org.agilewiki.jaconfig.rank.simple;

import org.agilewiki.jaconfig.rank.RankerServer;
import org.agilewiki.jactor.RP;
import org.agilewiki.jasocket.cluster.ServerNames;

import java.util.*;

public class SimpleRanker extends RankerServer {
    @Override
    public void ranking(String serverName, final RP<List<String>> rp) throws Exception {
        ServerNames.req.send(this, agentChannelManager(), new RP<TreeSet<String>>() {
            @Override
            public void processResponse(TreeSet<String> response) throws Exception {
                ArrayList<String> ranking = new ArrayList<String>();
                try {
                    HashMap<String, Integer> counts = new HashMap<String, Integer>();
                    Iterator<String> it1 = response.iterator();
                    while (it1.hasNext()) {
                        String raw = it1.next();
                        int i = raw.indexOf(' ');
                        String address = raw.substring(0, i);
                        Integer c = counts.get(address);
                        if (c == null)
                            c = new Integer(1);
                        else
                            c = c + 1;
                        counts.put(address, c);
                    }
                    HashMap<Integer, TreeSet<String>> addresses = new HashMap<Integer, TreeSet<String>>();
                    Iterator<String> it2 = counts.keySet().iterator();
                    while (it2.hasNext()) {
                        String address = it2.next();
                        Integer c = counts.get(address);
                        TreeSet<String> as = addresses.get(c);
                        if (as == null) {
                            as = new TreeSet<String>();
                            addresses.put(c, as);
                        }
                        as.add(address);
                    }
                    Iterator<Integer> it3 = addresses.keySet().iterator();
                    while (it3.hasNext()) {
                        Integer c = it3.next();
                        TreeSet<String> as = addresses.get(c);
                        ranking.addAll(as);
                    }
                } catch (Exception x) {
                    x.printStackTrace();
                    throw x;
                }
                rp.processResponse(ranking);
            }
        });
    }
}
