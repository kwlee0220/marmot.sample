package demo.dtg;

import static marmot.optor.StoreDataSetOptions.DEFAULT;
import static marmot.optor.StoreDataSetOptions.FORCE;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.locationtech.jts.geom.Polygon;

import utils.StopWatch;
import utils.UnitUtils;

import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.GeoClientUtils;
import marmot.geo.command.CreateSpatialIndexOptions;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;

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
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
//		DataSet political = getWgsPolitical(marmot, TEMP_POLITICAL);
		DataSet political = marmot.getDataSet(TEMP_POLITICAL);
		Polygon validBounds = GeoClientUtils.toPolygon(political.getBounds());
		
		DataSet ds = marmot.getDataSet(DTG);
		int nworkers = Math.max((int)(ds.length() / UnitUtils.parseByteSize("20gb")), 1);

		Plan plan;
		plan = Plan.builder("split_dtg")
					.load(DTG)

					.toPoint("x좌표", "y좌표", "the_geom")
					.filterSpatially("the_geom", INTERSECTS, validBounds)
					
					.spatialJoin("the_geom", TEMP_POLITICAL, "*-{the_geom},param.sig_cd")

					.storeByGroup(Group.ofKeys("sig_cd").workerCount(nworkers), RESULT, DEFAULT)
					.build();
		marmot.deleteDir(RESULT);
		marmot.execute(plan);
		
		watch.stop();
		System.out.printf("total elapsed time=%s%n", watch.getElapsedMillisString());
	}
	
	private static DataSet getWgsPolitical(PBMarmotClient marmot, String outDsId) {
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:4326");
		
		Plan plan;
		plan = Plan.builder("to_wgs84_political")
					.load(POLITICAL)
					.transformCrs("the_geom", "EPSG:5186", "EPSG:4326")
					.store(outDsId, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		DataSet output = marmot.getDataSet(outDsId);
		output.createSpatialIndex(CreateSpatialIndexOptions.WORKER_COUNT(1));
		
		return output;
	}
}
