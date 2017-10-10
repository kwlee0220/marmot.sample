package carloc;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Geometry;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindPassingStation {
	private static final String TAXI_TRJ = "로그/나비콜/택시경로";
	private static final String OUTPUT = "tmp/result";
	private static final String SRID = "EPSG:5186";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		StopWatch watch = StopWatch.start();
		
		Geometry key = getSubwayStations(marmot, "사당역");
		Plan plan = marmot.planBuilder("find_passing_station")
								.load(TAXI_TRJ)
								.filter("status == 3")
								.expand("the_geom:line_string",
											"the_geom = ST_TRLineString(trajectory)")
								.withinDistance("the_geom", key, 100)
								.project("*-{trajectory}")
								.store(OUTPUT)
								.build();
		
		RecordSchema schema = marmot.getOutputRecordSchema(plan);
		DataSet result = marmot.createDataSet(OUTPUT, schema, "the_geom", SRID, true);
		marmot.execute(plan);
		
		SampleUtils.printPrefix(result, 5);
		
		watch.stop();
		System.out.printf("elapsed time=%s%n", watch.getElapsedTimeString());
	}

	private static final String SUBWAY_STATIONS = "교통/지하철/서울역사";
	private static Geometry getSubwayStations(MarmotClient marmot, String stationName)
		throws Exception {
		String predicate = String.format("kor_sub_nm == '%s'", stationName);
		Plan plan = marmot.planBuilder("filter_subway_stations")
								.load(SUBWAY_STATIONS)
								.filter(predicate)
								.project("the_geom")
								.build();
		return marmot.executeLocally(plan)
						.stream()
						.map(rec -> rec.getGeometry(0))
						.findAny()
						.orElse(null);
	}
}
