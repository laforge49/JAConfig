package org.agilewiki.jaconfig.rank.simple;

import org.agilewiki.jaconfig.db.impl.ConfigServer;
import org.agilewiki.jaconfig.rank.RankerServer;
import org.agilewiki.jactor.RP;
import org.agilewiki.jasocket.cluster.ServerNames;
import org.agilewiki.jasocket.console.SunInterrupter;
import org.agilewiki.jasocket.node.ConsoleApp;
import org.agilewiki.jasocket.node.Node;

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

    public static void main(String[] args) throws Exception {
        Node node = new Node(args, 100);
        try {
            node.process();
            node.startup(ConfigServer.class, "");
            node.startup(SimpleRanker.class, "");
            (new ConsoleApp()).create(node, new SunInterrupter());
        } catch (Exception ex) {
            node.mailboxFactory().close();
            throw ex;
        }
    }
}
