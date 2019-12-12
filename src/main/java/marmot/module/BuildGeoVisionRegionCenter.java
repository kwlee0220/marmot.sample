package marmot.module;

import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildGeoVisionRegionCenter {
	private static final String BLOCKS = "구역/지오비전_집계구";
	private static final String BLOCK_CENTERS = "구역/지오비전_집계구_Point";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		DataSet blocks = marmot.getDataSet(BLOCKS);
		GeometryColumnInfo gcInfo = blocks.getGeometryColumnInfo();

		Plan plan = marmot.planBuilder("지오비전_집계구_중심점_추출")
								.load(BLOCKS)
								.centroid(gcInfo.name())
								.store(BLOCK_CENTERS, FORCE(gcInfo))
								.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(BLOCK_CENTERS);
		SampleUtils.printPrefix(result, 5);
		
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}
