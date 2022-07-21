package marmot;

import static marmot.optor.StoreDataSetOptions.FORCE;

import common.SampleUtils;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleCollectToArrayColumn {
	private static final String INPUT = "지오비전/유동인구/2015/월별_시간대";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet input = marmot.getDataSet(INPUT);
//		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("THE_GEOM", "EPSG:5186");

		Plan plan = Plan.builder("update")
							.load(INPUT)
							.collectToArrayColumn("avg:float[]", "column.startsWith('avg_')")
							.store(RESULT, FORCE(gcInfo))
							.build();
		marmot.execute(plan);
		
		SampleUtils.printPrefix(marmot.getDataSet(RESULT), 5);
	}
}
