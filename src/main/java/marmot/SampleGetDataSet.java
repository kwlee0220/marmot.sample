package marmot;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleGetDataSet {
	private static final String INPUT = "POI/주유소_가격";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet input = marmot.getDataSet(INPUT);
		SampleUtils.printPrefix(input, 50);
		
		for ( DataSet ds: marmot.getDataSetAllInDir("구역", true) ) {
			System.out.println(ds.getId());
		}
	}
}
