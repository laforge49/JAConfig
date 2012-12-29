package org.agilewiki.jaconfig.rank.simple;

import org.agilewiki.jaconfig.db.impl.ConfigServer;
import org.agilewiki.jaconfig.rank.Ranker;
import org.agilewiki.jactor.RP;
import org.agilewiki.jasocket.cluster.ServerNames;
import org.agilewiki.jasocket.node.ConsoleApp;
import org.agilewiki.jasocket.node.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class SimpleRanker extends Ranker {
    @Override
    public void ranking(final RP<List<String>> rp) throws Exception {
        ServerNames.req.send(this, agentChannelManager(), new RP<TreeSet<String>>() {
            @Override
            public void processResponse(TreeSet<String> response) throws Exception {
                ArrayList<String> ranking = new ArrayList<String>();
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
            (new ConsoleApp()).create(node);
        } catch (Exception ex) {
            node.mailboxFactory().close();
            throw ex;
        }
    }
}
