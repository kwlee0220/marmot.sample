package marmot;

import java.text.DecimalFormat;

import org.apache.log4j.PropertyConfigurator;

import com.google.protobuf.util.JsonFormat;
import com.vividsolutions.jts.geom.Envelope;

import marmot.command.MarmotClientCommands;
import marmot.plan.ParseCsvOption;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CSV;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PrintPlanAsJson {
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotClientCommands.getMarmotHost(cl);
		int port = MarmotClientCommands.getMarmotPort(cl);
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		String outSchemaExpr = "the_geom:point,id:string,user_id:string,created_at:string,"
							+ "coordinates:point,text:string";
		RecordSchema schema = RecordSchema.parse(outSchemaExpr);
		String initExpr = "$format=ST_DTPattern('EEE MMM dd HH:mm:ss Z yyyy').withLocale(Locale.ENGLISH)";
		String transExpr = "local:_meta.mvel";
		Envelope envl = marmot.getDataSet("구역/시도").getBounds();
		
		String header = FStream.range(0, 13)
								.map(idx -> String.format("field_%02d", idx))
								.join(",");
		header = 
//				"date,owner,car_no,time,mileage,mileage_accum,velo,rpm,brake,xpos,ypos,heading,xacc,yacc")
//				"car_no,ts,month,sid_cd,besselX,besselY,status,company,driver_id,xpos,ypos")
				"운행일자,ts,운송사코드,차량번호";
//					"번호,사업자명,소재지전체주소,도로명주소,인허가일자,형태,경도,위도")
//					"시군구코드,출입구일련번호,법정동코드,시도명,시군구명,읍면동명,도로명코드,도로명,지하여부,건물본번,건물부번,건물명,우편번호,건물용도분류,건물군여부,관할행정동,xpos,ypos")
//					"시군구,번지,본번,부번,단지명,전용면적,계약년월,계약일,거래금액,층,건축년도,도로명")
//					"사용년월,대지위치,도로명_대지위치,시군구코드,법정동코드,대지구분코드,번,지,새주소_일련번호,새주소_도로코드,새주소_지상지하코드,새주소_본번,새주소_부번,사용량")
//					"시군구,번지,본번,부번,단지명,전월세구분,전용면적,계약년월,계약일,보증금,월세,층,건축년도,도로명")
//					"STD_YM,BLOCK_CD,X_COORD,Y_COORD,AVG_00TMST,AVG_01TMST,AVG_02TMST,AVG_03TMST,AVG_04TMST,AVG_05TMST,AVG_06TMST,AVG_07TMST,AVG_08TMST,AVG_09TMST,AVG_10TMST,AVG_11TMST,AVG_12TMST,AVG_13TMST,AVG_14TMST,AVG_15TMST,AVG_16TMST,AVG_17TMST,AVG_18TMST,AVG_19TMST,AVG_20TMST,AVG_21TMST,AVG_22TMST,AVG_23TMST";

		String colDecls = FStream.range(0, 24)
							.map(idx -> String.format("AVG_%02dTMST:float", idx))
							.join(",");
		
		RecordScript script = RecordScript.of("$money_formatter = new DecimalFormat('#,###,###')",
												"$money_formatter.parse(거래금액).intValue();")
											.importClass(DecimalFormat.class);

		RecordScript script2 = RecordScript.of("$pat = ST_DTPattern(\"yyyyMMddHHmmss\")",
										"ST_DTParseLE(운행일자 + 운행시분초.substring(0,6), $pat)");
		Plan plan;
		plan = marmot.planBuilder("import_plan")
//					.parseCsv(',', ParseCsvOption.HEADER(header))
					.parseCsv(',', ParseCsvOption.HEADER(header), ParseCsvOption.COMMENT('#'))
//					.parseCsv('|', HEADER(header))
//					.filter("sp != null && sn != null")
//					.expand1("ts:datetime", script2)
//					.project("the_geom,운송사코드,차량번호,일일주행거리,누적주행거리,운행속도,rpm,"
//							+ "브레이크신호,방위각,가속도x,가속도y")
//					.expand1("기준년도:short", "(기준년도.length() > 0) ? 기준년도 : '2017'")
//					.expand1("기준월:short", "(기준월.length() > 0) ? 기준월 : '01'")
//					.expand1("개별공시지가:long")
//					.project("고유번호,기준년도,기준월,개별공시지가")
//					.assignUid("id")
//					.sample(0.23)
//					.expand("일일주행거리:int,누적주행거리:int,운행속도:short,RPM:short,브레이크신호:boolean,방위각:short,가속도X:float,가속도Y:float")
//					.toPoint("x", "y", "the_geom")
//					.expand1("status:byte")
//					.parse("도로명코드,도로명,도로명로마자,읍면동_일련번호,시도명,시도명로마자,시군구명,시군구명로마자,읍면동명,읍면동명로마자,읍면동구분,읍면동코드,사용여부,변경사유,변경이력,고시일자,말소일자")
//					.expand1("the_geom:polygon", "ST_GeomFromGeoJSON(다발지역폴리곤)")
//					.toPoint("경도", "위도", "다발지점")
//					.transformCrs("다발지점", "EPSG:4326", "EPSG:5186")
//					.expand1("pnu:string", "시군구코드 + 법정동코드 + 대지구분코드 + 번 + 지")
//					.parseCsv('|', HEADER(header), TRIM_FIELD)
//					.update("$part=Lat_Lon.split(','); lat=$part[0]; lon=$part[1];")
//					.toPoint("lon", "lat", "the_geom")
//					.transformCrs("the_geom", "EPSG:4326", "EPSG:5186")
//					.project("ROW_ID,POI,ID,SP,SN,언급빈도수,선호도")
//					.expand("sp:double,sn:double,언급빈도수:int,선호도:double", "sn=sp + 1")
//					.expand("발생건수:int,사상자수:int,사망자수:int,중상자수:int,경상자수:int,부상신고자수:int")
//					.transformCrs("the_geom", "EPSG:5179", "aaa", "EPSG:5186")
//					.update("기준년도=(기준년도.length() > 0) ? 기준년도 : '2017'; 기준월=(기준월.length() > 0) ? 기준월 : '01'")
//					.expand1("거래금액:int",script)
//					.query("input_dsid", SpatialRelation.WITHIN_DISTANCE(30), "key_disId")
//					.spatialJoin("the_geom", "xxxxx", "output_cols")
//					.spatialOuterJoin("the_geom", "xxxxx", "output_cols")
//					.spatialSemiJoin("the_geom", "xxxxx",
//									SpatialJoinOption.NEGATED)
//					.loadSpatialIndexJoin("leftDsId", "rightDsId", "*", SpatialRelation.WITHIN_DISTANCE(30))
//					.intersection("the_geom", "the_geom2", "the_geom")
//					.intersects("the_geom", "param_dsid", PredicateOption.NEGATED)
//					.withinDistance("the_geom", "param_dsid", 500, PredicateOption.NEGATED)
//					.loadSquareGridFile(new SquareGrid(envl, new Size2d(100, 100)), 7)
//					.assignSquareGridCell("the_geom", new SquareGrid("dsid", new Size2d(100, 100)))
//					.join("col1,col2", "ds_id", "jcols", "out_cols", JoinOptions.SEMI_JOIN())
//					.join("emd_cd,name", "EMD", "emd_cd,age", "param.{the_geom,emd_kor_nm},count",
//							JoinOptions.FULL_OUTER_JOIN(1))
//					.groupBy("aaa,bbb")
//						.aggregate(AggregateFunction.SUM("cc"), AggregateFunction.COUNT())
//					.groupBy("block_cd")
//						.tagWith("geomCol")
//						.aggregate(AVG("day_total"))
//					.aggregate(AggregateFunction.SUM("aaa"), AggregateFunction.MAX("bbb").as("ccc"))
//					.sort("보관일수:A:F,카메라대수:A")
//					.distinct("c1,c2", 11)
//					.loadEquiJoin("left_dsId", "lc1,lc2", "right_dsId", "rc1,rc2",
//									"left.oc1,right.oc2", JoinOptions.INNER_JOIN(11))
//					.clipJoin("the_geom", "clipper_dsid")
//					.intersectionJoin("the_geom", "param_id", "out_cols")
//					.differenceJoin("the_geom", "clipper_dsid")
//					.spatialAggregateJoin("the_geom", "clipper_dsid",
//											AggregateFunction.COUNT(), AggregateFunction.MAX("c2"))
//					.pickTopK("col1:A:F", 5)
//					.toXYCoordinates("the_geom", "x", "y")
//					.matchSpatially("the_geom", SpatialRelation.CONTAINS, "param_ds_id")
//					.knnJoin("the_geom", "param_ds_id", 3, 100, "*,param.{xx}")
//					.shard(11)
					.build();
		
		System.out.println(JsonFormat.printer().print(plan.toProto()));
	}
}
