package marmot;

import static marmot.optor.StoreDataSetOptions.FORCE;

import java.util.UUID;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.optor.AggregateFunction;
import marmot.plan.LoadOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleLoadFixedMapper {
	private static final String INPUT = "구역/연속지적도_2019";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		String jobId = UUID.randomUUID().toString();
		
		Plan plan;
		plan = Plan.builder("load_text")
					.load(INPUT, LoadOptions.MAPPERS(31))
					.aggregate(AggregateFunction.COUNT())
					.store("tmp/result", FORCE)
					.build();
		marmot.execute(plan);
		watch.stop();
		
		DataSet result = marmot.getDataSet("tmp/result");
		SampleUtils.printPrefix(result, 5);
		
		System.out.println("elapsed: " + watch.getElapsedSecondString());
	}
}
