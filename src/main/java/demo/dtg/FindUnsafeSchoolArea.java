package demo.dtg;

import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.geo.CoordinateTransform;
import marmot.geo.GeoClientUtils;
import marmot.geo.command.ClusterDataSetOptions;
import static marmot.optor.AggregateFunction.*;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindUnsafeSchoolArea {
	private static final String SCHOOLS = "POI/전국초중등학교";
	private static final String TEMP_SCHOOLS = "tmp/dtg/temp_schools";
	private static final String DTG = "교통/dtg";
	private static final String RESULT = "tmp/dtg/unsafe_zone";
	
	private static final int WORKER_COUNT = 5;
	
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

		DataSet schools = findElementarySchoolBuffer(marmot, TEMP_SCHOOLS);
//		DataSet schools = marmot.getDataSet(TEMP_SCHOOLS);
		Polygon key = getValidWgsBounds(schools.getBounds());
		
		DataSet output;
		GeometryColumnInfo info = marmot.getDataSet(SCHOOLS).getGeometryColumnInfo();

		Plan plan;
		plan = marmot.planBuilder("find average speed around primary schools")
					.load(DTG)

					.filter("운행속도 > 1")
					.toPoint("x좌표", "y좌표", "the_geom")
					.intersects("the_geom", key)
					.transformCRS("the_geom", "EPSG:4326", "the_geom", "EPSG:5186")
					
					.spatialJoin("the_geom", TEMP_SCHOOLS, INTERSECTS, "param.*,운행속도")
					
					.groupBy("학교id")
						.tagWith("the_geom,학교명")
						.workerCount(WORKER_COUNT)
						.aggregate(AVG("운행속도"))
					
					.store(RESULT)
					.build();
		output = marmot.createDataSet(RESULT, info, plan, true);
		
		watch.stop();
		System.out.printf("count=%d, total elapsed time=%s%n",
							output.getRecordCount(), watch.getElapsedMillisString());
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(output, 5);
	}
	
	private static Polygon getValidWgsBounds(Envelope envl) {
		Envelope bounds = new Envelope(envl);
		bounds.expandBy(1);
		
		CoordinateTransform trans = CoordinateTransform.get("EPSG:5186", "EPSG:4326");
		Envelope wgs84Bounds = trans.transform(bounds);
		
		return GeoClientUtils.toPolygon(wgs84Bounds);
	}
	
	private static DataSet findElementarySchoolBuffer(PBMarmotClient marmot, String outDsId) {
		GeometryColumnInfo info = new GeometryColumnInfo("area", "EPSG:5186");
		
		Plan plan;
		plan = marmot.planBuilder("find elementary schools")
					.load(SCHOOLS)
					.filter("학교급구분 == '초등학교'")
					.buffer("the_geom", "area", 200)
					.project("the_geom,학교id,학교명,area")
					.store(outDsId)
					.build();
		DataSet output = marmot.createDataSet(outDsId, info, plan, true);
		
		ClusterDataSetOptions opts = ClusterDataSetOptions.create().workerCount(1);
		output.cluster(opts);
		
		return output;
	}
}
