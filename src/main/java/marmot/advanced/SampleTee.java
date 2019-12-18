package marmot.advanced;

import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.BindDataSetOptions;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.DataSetType;
import marmot.dataset.GeometryColumnInfo;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleTee {
	private static final String INPUT = "POI/주유소_가격";
	private static final String RESULT = "tmp/result";
	private static final String RESULT2 = "tmp/result2";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet input = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();

		marmot.deleteDataSet(RESULT);
		Plan plan = Plan.builder("filter")
							.load(INPUT)
							.filter("휘발유 > 2000")
							.tee("tmp/temp")
							.project("THE_GEOM,상호,휘발유")
							.store(RESULT, FORCE(gcInfo))
							.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);
		
		SampleUtils.printPrefix(result, 5);
		
		result = marmot.bindExternalDataSet(RESULT2, "tmp/temp", DataSetType.FILE,
											BindDataSetOptions.DEFAULT(gcInfo));
		SampleUtils.printPrefix(result, 5);
		marmot.deleteDataSet(RESULT2);
	}
}
