package marmot.geom.advanced;

import static marmot.StoreDataSetOptions.FORCE;
import static marmot.optor.JoinOptions.INNER_JOIN;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.process.geo.arc.ArcSplitParameters;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleArcSplit {
	private static final String GAS_STATIONS = "POI/주유소_가격";
	private static final String SGG = "구역/시군구";
	private static final String EMD = "구역/읍면동";
	private static final String ANYANG_EMD = "tmp/anyang_emd";
	private static final String RESULT = "tmp/gas_station_splits";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Plan plan;
		DataSet result;

		GeometryColumnInfo gcInfo = marmot.getDataSet(EMD).getGeometryColumnInfo();
		plan = marmot.planBuilder("test")
					.load(EMD)
					.defineColumn("SIG_CD:string", "EMD_CD.substring(0, 5)")
					.hashJoin("SIG_CD", SGG, "SIG_CD", "*,param.{SIG_KOR_NM}", INNER_JOIN)
					.filter("SIG_KOR_NM.contains('안양시')")
					.project("the_geom,EMD_KOR_NM")
					.build();
		result = marmot.createDataSet(ANYANG_EMD, plan, FORCE(gcInfo));
		result.cluster();
		
		ArcSplitParameters params = new ArcSplitParameters();
		params.setInputDataset(GAS_STATIONS);
		params.setSplitDataset(ANYANG_EMD);
		params.setSplitKey("EMD_KOR_NM");
		params.setOutputDataset(RESULT);
		params.setForce(true);
		marmot.executeProcess("arc_split", params.toMap());
		
		for ( DataSet ds: marmot.getDataSetAllInDir(RESULT, true) ) {
			System.out.println(ds.getId() + ":");
			
			// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
			SampleUtils.printPrefix(ds, 5);
		}
		
	}
}
