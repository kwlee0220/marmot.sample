package module;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.PropertyConfigurator;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Point;

import common.SampleUtils;
import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.process.geo.KMeansParameters;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleKMeans {
	private static final String SGG = "구역/시군구";
	private static final String INPUT = "토지/용도지역지구";
	private static final String TEMP = "tmp/centers";
	private static final String OUTPUT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotCommands.getMarmotHost(cl);
		int port = MarmotCommands.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		KMeansParameters params = new KMeansParameters();
		params.inputDataset(INPUT);
		params.outputDataset(OUTPUT);
		params.featureColumns(Lists.newArrayList("center"));
		params.clusterColumn("cluster_id");
//		params.initialCentroids(getInitCentroids(marmot, 9, 0.025));
		params.terminationDistance(100);
		params.terminationIteration(30);
		
		marmot.deleteDataSet(OUTPUT);
		marmot.executeProcess("kmeans", params.toMap());
		
		DataSet output = marmot.getDataSet(OUTPUT);
		SampleUtils.printPrefix(output, 10);
	}
	
	private static List<Point> getInitCentroids(MarmotRuntime marmot, int ncentroids,
												double ratio) {
		Plan plan = marmot.planBuilder("get_init_centroids")
								.load(SGG)
								.sample(ratio)
								.take(ncentroids)
								.project("the_geom")
								.centroid("the_geom")
								.build();
		return marmot.executeLocally(plan).stream()
					.map(r -> (Point)r.getGeometry(0))
					.collect(Collectors.toList());
	}
}
