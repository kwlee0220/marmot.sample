package marmot.advanced;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.support.TextDataSetAdaptor;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleTextDataSetAdaptor {
	private static final String INPUT1 = "로그/나비콜/택시로그S";
	private static final String INPUT2 = "로그/나비콜/택시로그";
	private static final String INPUT3 = "주민/유동인구/월별_시간대/2015";
	private static final String INPUT4 = "교통/지하철/서울역사";
	private static final String INPUT5 = "건물/건물통합정보마스터/201809";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet ds;
		TextDataSetAdaptor adaptor = new TextDataSetAdaptor(marmot);
		
		ds = adaptor.adapt(marmot.getDataSet(INPUT1));
		System.out.printf("%s: type=%s count=%d mbr=%s%n",
							ds.getId(), ds.getType(), ds.getRecordCount(), ds.getBounds());
		
		ds = adaptor.adapt(marmot.getDataSet(INPUT2));
		System.out.printf("%s: type=%s count=%d mbr=%s%n",
							ds.getId(), ds.getType(), ds.getRecordCount(), ds.getBounds());
		
		ds = adaptor.adapt(marmot.getDataSet(INPUT3));
		System.out.printf("%s: type=%s count=%d mbr=%s%n",
							ds.getId(), ds.getType(), ds.getRecordCount(), ds.getBounds());
		
		ds = adaptor.adapt(marmot.getDataSet(INPUT4));
		System.out.printf("%s: type=%s count=%d mbr=%s%n",
							ds.getId(), ds.getType(), ds.getRecordCount(), ds.getBounds());
		
		ds = adaptor.adapt(marmot.getDataSet(INPUT5));
		System.out.printf("%s: type=%s count=%d mbr=%s%n",
							ds.getId(), ds.getType(), ds.getRecordCount(), ds.getBounds());
	}
}
