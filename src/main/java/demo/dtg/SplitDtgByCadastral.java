package demo.dtg;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Polygon;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.geo.GeoClientUtils;
import marmot.geo.command.ClusterDataSetOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SplitDtgByCadastral {
	private static final String POLITICAL = "구역/시군구";
	private static final String DTG = "교통/dtg";
	private static final String TEMP_POLITICAL = "tmp/dtg/political";
	private static final String RESULT = "tmp/dtg/splits";
	
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
		
//		DataSet political = getWgsPolitical(marmot, TEMP_POLITICAL);
		DataSet political = marmot.getDataSet(TEMP_POLITICAL);
		Polygon validBounds = GeoClientUtils.toPolygon(political.getBounds());
		
		DataSet ds = marmot.getDataSet(DTG);
		int nworkers = Math.max((int)(ds.length() / UnitUtils.parseByteSize("20gb")), 1);

		Plan plan;
		plan = marmot.planBuilder("split_dtg")
					.load(DTG)

					.toPoint("x좌표", "y좌표", "the_geom")
					.intersects("the_geom", validBounds)
					
					.spatialJoin("the_geom", TEMP_POLITICAL, "*-{the_geom},param.sig_cd")
					
					.groupBy("sig_cd")
						.workerCount(nworkers)
						.storeEachGroup(RESULT)
					.build();
		marmot.deleteDir(RESULT);
		marmot.execute(plan);
		
		watch.stop();
		System.out.printf("total elapsed time=%s%n", watch.getElapsedMillisString());
	}
	
	private static DataSet getWgsPolitical(PBMarmotClient marmot, String outDsId) {
		GeometryColumnInfo info = new GeometryColumnInfo("the_geom", "EPSG:4326");
		
		Plan plan;
		plan = marmot.planBuilder("to_wgs84_political")
					.load(POLITICAL)
					.transformCrs("the_geom", "EPSG:5186", "EPSG:4326", "the_geom")
					.store(outDsId)
					.build();
		DataSet output = marmot.createDataSet(outDsId, info, plan, true);
		
		ClusterDataSetOptions opts = ClusterDataSetOptions.create().workerCount(1);
		output.cluster(opts);
		
		return output;
	}
}
