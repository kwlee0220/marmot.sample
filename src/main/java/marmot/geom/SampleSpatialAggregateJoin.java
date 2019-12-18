package marmot.geom;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.MAX;
import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleSpatialAggregateJoin {
	private static final String GAS_STATIONS = "POI/주유소_가격";
	private static final String EMD = "구역/읍면동";
	private static final String RESULT = "tmp/result";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		Plan plan = Plan.builder("spatial_join")
								.load(EMD)
								.spatialAggregateJoin("the_geom", GAS_STATIONS, COUNT(), MAX("휘발유"))
								.store(RESULT, FORCE(gcInfo))
								.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 20);
	}
}
