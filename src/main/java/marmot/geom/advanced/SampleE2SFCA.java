package marmot.geom.advanced;

import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.StoreDataSetOptions.FORCE;

import utils.StopWatch;

import common.SampleUtils;
import marmot.Plan;
import marmot.analysis.module.geo.E2SFCAParameters;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.geo.advanced.Power;
import marmot.optor.geo.advanced.WeightFunction;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleE2SFCA {
	private static final String BUS_SUPPLY = "연세대/강남구_버스";
	private static final String SUBWAY_SUPPLY = "연세대/강남구_지하철";
	private static final String FLOW_POP = "주민/유동인구/강남구/시간대/2015";
	private static final String RESULT_BUS = "tmp/E2SFCA/bus";
	private static final String RESULT_SUBWAY = "tmp/E2SFCA/subway";
	private static final String RESULT_CONCAT = "tmp/E2SFCA/concat";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		Plan plan;
		DataSet result;
		
		WeightFunction wfunc = Power.of(-0.1442);
		
		E2SFCAParameters params1 = new E2SFCAParameters();
		params1.setConsumerDataset(FLOW_POP);
		params1.setProviderDataset(BUS_SUPPLY);
		params1.setOutputDataset(RESULT_BUS);
		params1.setConsumerFeatureColumns("avg_08tmst,avg_15tmst");
		params1.setProviderFeatureColumns("slevel_08,slevel_15");
		params1.setOutputFeatureColumns("index_08,index_15");
		params1.setServiceDistance(400);
		params1.setWeightFunction(wfunc);
		marmot.executeProcess("e2sfca", params1.toMap());
	
		E2SFCAParameters params2 = new E2SFCAParameters();
		params2.setConsumerDataset(FLOW_POP);
		params2.setProviderDataset(SUBWAY_SUPPLY);
		params2.setOutputDataset(RESULT_SUBWAY);
		params2.setConsumerFeatureColumns("avg_08tmst,avg_15tmst");
		params2.setProviderFeatureColumns("slevel_08,slevel_15");
		params2.setOutputFeatureColumns("index_08,index_15");
		params2.setServiceDistance(800);
		params2.setWeightFunction(wfunc);
		marmot.executeProcess("e2sfca", params2.toMap());
		
		GeometryColumnInfo gcInfo = marmot.getDataSet(RESULT_BUS).getGeometryColumnInfo();
		plan = Plan.builder("append bus result")
					.load(RESULT_BUS)
					.project("the_geom,block_cd,index_08,index_15")
					.store(RESULT_CONCAT, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		result = marmot.getDataSet(RESULT_CONCAT);
		
		plan = Plan.builder("append subway result")
					.load(RESULT_SUBWAY)
					.project("the_geom,block_cd,index_08,index_15")
					.store(RESULT_CONCAT)
					.build();
		marmot.execute(plan);
		result = marmot.getDataSet(RESULT_CONCAT);
		
		plan = Plan.builder("combine two results")
					.load(RESULT_CONCAT)
					.aggregateByGroup(Group.ofKeys("block_cd").tags("the_geom"),
										SUM("index_08").as("index_08"),
										SUM("index_15").as("index_15"))
					.store(RESULT, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		result = marmot.getDataSet(RESULT);
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
		
		marmot.deleteDataSet(RESULT_CONCAT);
		marmot.deleteDataSet(RESULT_SUBWAY);
		marmot.deleteDataSet(RESULT_BUS);
	}
}
