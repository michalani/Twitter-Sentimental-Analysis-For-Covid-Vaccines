

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.Status;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *  Do not forward the specific NEGATIVE words to the next bolt.
 */
public class IgnoreWordsBolt extends BaseRichBolt {
	
	private final int minWordLength;
	
    public IgnoreWordsBolt(int minWordLength) {
        this.minWordLength = minWordLength;
    }
	
	private Set<String> IGNORE_LIST = new HashSet<String>(Arrays.asList(new String[] {
            "http", "https", "football","the", "you", "que", "and", "for", "that", "like", "have", "this", "just", "with", "all", "get", 
            "about", "can", "was", "not", "your", "but", "are", "one", "what", "out", "when", "get", "lol", "now", "para", "por",
            "want", "will", "know", "good", "from", "las", "don", "people", "got", "why", "con", "time", "would",
    }));
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }
    @Override
    public void execute(Tuple input) {
        Status tweet = (Status) input.getValueByField("tweet");
        String lang = tweet.getUser().getLang();
        String text = tweet.getText().replaceAll("\\p{Punct}", " ").replaceAll("\\r|\\n", "").toLowerCase();
        String[] words = text.split(" ");
        List<String> freshWords = new ArrayList<String>();
        for (String word : words) {        	
            if (word.length() >= minWordLength) {
                if (!IGNORE_LIST.contains(word)) {
                	freshWords.add(word);
                    //collector.emit(new Values(lang, word));
                }
            }
        }
        
        if(freshWords.isEmpty() == false) {
        	String[] freshWordsArr = new String[freshWords.size()];
        	freshWordsArr = freshWords.toArray(freshWordsArr);
        	collector.emit(new Values(lang, freshWordsArr));
        	//collector.emit(new Values(lang, freshWordsArr));

        }
        
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("lang", "word"));
    }
}
