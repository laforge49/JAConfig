package org.agilewiki.jaconfig.rank.simple;

import org.agilewiki.jaconfig.rank.Ranker;
import org.agilewiki.jactor.RP;

import java.util.ArrayList;
import java.util.List;

public class SimpleRanker extends Ranker {
    @Override
    public List<String> ranking(RP<List<String>> rp) throws Exception {
        return new ArrayList<String>();
    }
}
