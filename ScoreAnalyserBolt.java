

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.Status;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Receives tweets and emits its words over a certain length.
 */
public class ScoreAnalyserBolt extends BaseRichBolt {

    private int negativeTweetsCount;
    private int positiveTweetsCount;
    private int totalTweetsCount;

	private final int minWordLength;

    private OutputCollector collector;

    public ScoreAnalyserBolt(int minWordLength) {
        this.minWordLength = minWordLength;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
    	//int positiveTweets = (Integer) (input.getValueByField("positiveTweets"));
    	//String[] words = (String[]) input.getValueByField("words");
    	int positiveTweets = (Integer) (input.getValueByField("positiveTweets"));
    	int negativeTweets = (Integer) (input.getValueByField("negativeTweets"));
    	
    	
    	
        if(positiveTweets > 0 && negativeTweets > 0) {
        	if(positiveTweets > negativeTweets) {
        		positiveTweetsCount += 1;
        	}
        	else if(negativeTweets > positiveTweets) {
        		negativeTweetsCount += 1;
        	}
        }
        totalTweetsCount += 1;
        System.out.println("Tweets: "+totalTweetsCount+" neg: "+negativeTweetsCount+" pos: "+positiveTweetsCount);
        	
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
