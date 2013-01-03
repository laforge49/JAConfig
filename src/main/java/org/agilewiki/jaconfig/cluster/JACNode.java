package org.agilewiki.jaconfig.cluster;

import org.agilewiki.jasocket.node.Node;

public class JACNode extends Node {
    public JACNode(String[] args, int threadCount) throws Exception {
        super(args, threadCount);
    }

    protected void createAgentChannelManager() {
        agentChannelManager = new JACChannelManager();
    }
}
