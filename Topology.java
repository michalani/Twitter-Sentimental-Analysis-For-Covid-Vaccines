

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * Storm topology (sets up the barebones graph outline for the project)
 * Please include Twitter credentials in a twitter4j.properties file as a security measure.
 */
public class Topology {

	static final String TOPOLOGY_NAME = "storm-twitter-covid-sentiment-analyzer";

	public static void main(String[] args) {
		Config config = new Config();
		config.setNumWorkers(3);
		config.setMessageTimeoutSecs(120);

		TopologyBuilder b = new TopologyBuilder();
		b.setSpout("TwitterSampleSpout", new TwitterFeedSpout(),3); //TWITTER INPUT FEED SPOUT
        b.setBolt("IgnoreWordsBolt", new IgnoreWordsBolt(5),3).shuffleGrouping("TwitterSampleSpout"); //WORDS IGNORER
        b.setBolt("PositiveSentimentAnalyserBolt", new PositiveSentimentAnalyserBolt(10, 5 * 60, 50),3).shuffleGrouping("IgnoreWordsBolt"); //POSITIVITY ANALYSER
        b.setBolt("NegativeSentimentAnalyserBolt", new NegativeSentimentAnalyserBolt(0),3).shuffleGrouping("PositiveSentimentAnalyserBolt"); //NEGATIVITY ANALYSER
        b.setBolt("ScoreAnalyserBolt", new ScoreAnalyserBolt(0),1).shuffleGrouping("NegativeSentimentAnalyserBolt"); // SCORE COUNTER
		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				cluster.killTopology(TOPOLOGY_NAME);
				cluster.shutdown();
			}
		});

	}

}
