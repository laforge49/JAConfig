package org.agilewiki.jaconfig.cluster;

import org.agilewiki.jaconfig.quorum.ProcessStartupEntry;
import org.agilewiki.jaconfig.quorum.StartupEntry;
import org.agilewiki.jaconfig.rank.Ranking;
import org.agilewiki.jactor.ExceptionHandler;
import org.agilewiki.jactor.RP;
import org.agilewiki.jactor.factory.JAFactory;
import org.agilewiki.jasocket.JASocketFactories;
import org.agilewiki.jasocket.agentChannel.AgentChannel;
import org.agilewiki.jasocket.cluster.AgentChannelManager;
import org.agilewiki.jasocket.jid.PrintJid;
import org.agilewiki.jasocket.server.Server;

import java.util.ArrayDeque;
import java.util.List;

public class JACChannelManager extends AgentChannelManager {
    private ArrayDeque<StartupEntry> startupQueue = new ArrayDeque();

    public void startupServer(
            String serverName,
            String className,
            String serverArgs,
            String rankerName,
            RP rp) throws Exception {
        StartupEntry startupEntry = new StartupEntry(serverName, className, serverArgs, rankerName, rp);
        startupQueue.addLast(startupEntry);
        if (startupQueue.size() > 1)
            return;
        ProcessStartupEntry.req.sendEvent(this, this);
    }

    void processStartupEntry() throws Exception {
        if (startupQueue.size() == 0)
            return;
        final StartupEntry startupEntry = startupQueue.peekFirst();
        setExceptionHandler(new ExceptionHandler() {
            @Override
            public void process(Exception e) throws Exception {
                processNextStartupEntry();
                startupEntry.rp.processResponse(e);
            }
        });
        final PrintJid out = (PrintJid) JAFactory.
                newActor(this, JASocketFactories.PRINT_JID_FACTORY, getMailboxFactory().createMailbox());
        String rankerName = startupEntry.rankerName;
        Server rankerServer = getLocalServer(rankerName);
        if (rankerServer == null) {
            out.println("unknown ranker server: " + rankerName);
            startupEntry.rp.processResponse(out);
            processNextStartupEntry();
            return;
        }
        Ranking.req.send(this, rankerServer, new RP<List<String>>() {
            @Override
            public void processResponse(List<String> strings) throws Exception {
                String address = strings.get(0);
                if (isLocalAddress(address)) {
                    localStartup(startupEntry);
                } else {
                    AgentChannel agentChannel = getAgentChannel(address);

                    //todo
                    processNextStartupEntry();
                }
            }
        });
    }

    private void localStartup(StartupEntry startupEntry) throws Exception {
        //todo
        processNextStartupEntry();
    }

    private void processNextStartupEntry() throws Exception {
        startupQueue.removeFirst();
        ProcessStartupEntry.req.sendEvent(this, this);
    }
}
