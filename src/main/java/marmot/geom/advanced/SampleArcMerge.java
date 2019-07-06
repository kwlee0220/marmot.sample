package marmot.geom.advanced;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.command.MarmotClientCommands;
import marmot.process.geo.arc.ArcMergeParameters;
import marmot.remote.protobuf.PBMarmotClient;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleArcMerge {
	private static final String INPUT = "tmp/gas_station_splits";
	private static final String RESULT = "tmp/gas_stations";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		String inputDsIdList = FStream.from(marmot.getDataSetAllInDir(INPUT, true))
										.map(DataSet::getId)
										.join(',');
		
		ArcMergeParameters params = new ArcMergeParameters();
		params.setInputDatasets(inputDsIdList);
		params.setOutputDataset(RESULT);
		params.setForce(true);
		
		marmot.executeProcess("arc_merge", params.toMap());
		
		DataSet result = marmot.getDataSet(RESULT);
		SampleUtils.printPrefix(result, 5);
	}
}
