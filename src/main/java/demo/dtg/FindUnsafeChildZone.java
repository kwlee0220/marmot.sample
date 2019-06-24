package demo.dtg;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.geo.CoordinateTransform;
import marmot.geo.GeoClientUtils;
import marmot.geo.command.ClusterDataSetOptions;
import marmot.plan.GeomOpOptions;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindUnsafeChildZone {
	private static final String CHILD_ZONE = "POI/어린이보호구역";
	private static final String TEMP_ZONE = "tmp/dtg/temp_child_zone";
	private static final String DTG = "교통/dtg";
	private static final String RESULT = "tmp/dtg/unsafe_zone";
	
	private static final int DISTANCE = 200;
	private static final int WORKER_COUNT = 5;
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();

		DataSet schools = bufferChildZone(marmot, TEMP_ZONE);
		Polygon key = getValidWgsBounds(schools.getBounds());
		
		DataSet output;

		Plan plan;
		plan = marmot.planBuilder("find average speed around primary schools")
					.load(DTG)

					.filter("운행속도 > 1")
					.toPoint("x좌표", "y좌표", "the_geom")
					.filterSpatially("the_geom", INTERSECTS, key)
					.transformCrs("the_geom", "EPSG:4326", "EPSG:5186")
					
					.spatialJoin("the_geom", TEMP_ZONE, "param.*,운행속도")

					.aggregateByGroup(Group.ofKeys("id").tags("the_geom,대상시설명")
											.workerCount(WORKER_COUNT), AVG("운행속도"),
										COUNT())
					.filter("count > 10000")	
						
					.defineColumn("speed:int", "avg")
					.project("the_geom,id,대상시설명 as name,speed,count")
					
					.store(RESULT)
					.build();
		GeometryColumnInfo gcInfo = marmot.getDataSet(CHILD_ZONE).getGeometryColumnInfo();
		output = marmot.createDataSet(RESULT, plan, StoreDataSetOptions.create().geometryColumnInfo(gcInfo).force(true));
		
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
	
	private static DataSet bufferChildZone(PBMarmotClient marmot, String outDsId) {
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("area", "EPSG:5186");
		
		Plan plan;
		plan = marmot.planBuilder("buffer child zones")
					.load(CHILD_ZONE)
					.buffer("the_geom", DISTANCE, GeomOpOptions.OUTPUT("area"))
					.project("the_geom,id,대상시설명,area")
					.store(outDsId)
					.build();
		DataSet output = marmot.createDataSet(outDsId, plan, StoreDataSetOptions.create().geometryColumnInfo(gcInfo).force(true));
		
		ClusterDataSetOptions opts = ClusterDataSetOptions.create().workerCount(1);
		output.cluster(opts);
		
		return output;
	}
}
