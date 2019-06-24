package anyang.energe;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class A00_ExtractCadastral {
	private static final String INPUT = Globals.LAND_PRICES_2017;
	private static final String OUTPUT = Globals.CADASTRAL;
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();

		DataSet ds = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		long blockSize = UnitUtils.parseByteSize("128mb");

		Plan plan;
		plan = marmot.planBuilder("연속지적도 추출")
					.load(INPUT)
					.project("the_geom, pnu")
					.shard(1)
					.build();
		DataSet result = marmot.createDataSet(OUTPUT, plan, 
												StoreDataSetOptions.create()
																.geometryColumnInfo(gcInfo)
																.force(true)
																.blockSize(blockSize));
		
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
	}
}
