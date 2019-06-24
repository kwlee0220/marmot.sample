package marmot.geom;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.plan.SpatialJoinOptions;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleIntersectionJoin {
	private static final String RESULT = "tmp/result";
	private static final String OUTER = "POI/주유소_가격";
	private static final String INNER = "tmp/서울특별시";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		SampleUtils.writeSeoul(marmot, INNER);
		
		Plan plan = marmot.planBuilder("sample_intersection_join")
							.load(OUTER)
							.buffer("the_geom", 50)
							.intersectionJoin("the_geom", INNER, SpatialJoinOptions.EMPTY)
							.build();
		DataSet result = marmot.createDataSet(RESULT, plan, StoreDataSetOptions.create().force(true));
		marmot.deleteDataSet(INNER);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
}
