package marmot;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleDefineColumn {
	private static final String INPUT = "교통/지하철/서울역사";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet input = marmot.getDataSet(INPUT);
//		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("THE_GEOM", "EPSG:5186");

		Plan plan = marmot.planBuilder("update")
							.load(INPUT)
							.defineColumn("AREA:double", "ST_Area(the_geom);")
							.defineColumn("the_geom:point", "ST_Centroid(the_geom)")
							.defineColumn("sig_cd:int")
							.project("the_geom,area,SIG_CD")
							.build();
		DataSet result = marmot.createDataSet(RESULT, plan, GEOMETRY(gcInfo), FORCE);
		SampleUtils.printPrefix(result, 5);
	}
}
