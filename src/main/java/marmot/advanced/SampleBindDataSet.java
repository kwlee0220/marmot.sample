package marmot.advanced;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.BindDataSetOptions;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.DataSetType;
import marmot.dataset.GeometryColumnInfo;
import marmot.io.MarmotFileNotFoundException;
import marmot.remote.protobuf.PBMarmotClient;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleBindDataSet {
	private static final String INPUT = "POI/주유소_가격";
	private static final String RESULT = "tmp/result";
	
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
							.project("THE_GEOM,상호,휘발유")
							.storeMarmotFile(RESULT)
							.build();
		marmot.execute(plan);
		try ( RecordSet rset = marmot.readMarmotFile(RESULT) ) {
			SampleUtils.printPrefix(rset, 5);
		}
		
		DataSet result;
		result = marmot.getDataSetOrNull(RESULT);
		Utilities.checkState(result == null, "result should be null");
		
		result = marmot.bindExternalDataSet(RESULT, RESULT, DataSetType.FILE,
											BindDataSetOptions.DEFAULT(gcInfo));
		SampleUtils.printPrefix(result, 5);
		
		marmot.deleteDataSet(RESULT);
		try {
			marmot.readMarmotFile(RESULT);
			throw new AssertionError();
		}
		catch ( MarmotFileNotFoundException expected ) { }
	}
}
