package basic;

import java.util.Arrays;

import org.apache.log4j.PropertyConfigurator;

import com.google.protobuf.util.JsonFormat;

import io.vavr.control.Option;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.proto.optor.LoadTextFileProto;
import marmot.proto.optor.OperatorProto;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;

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

		String host = MarmotCommands.getMarmotHost(cl);
		int port = MarmotCommands.getMarmotPort(cl);
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);

		LoadTextFileProto load = LoadTextFileProto.newBuilder()
													.addAllPaths(Arrays.asList("."))
													.setCharset("UTF-8")
													.setCommentPrefix("#")
													.setSplitCountPerBlock(1)
													.build();
		
		String outSchemaExpr = "the_geom:point,id:string,user_id:string,created_at:string,"
							+ "coordinates:point,text:string";
		String initExpr = "$format=ST_DTPattern('EEE MMM dd HH:mm:ss Z yyyy').withLocale(Locale.ENGLISH)";
		String transExpr = "local:_meta.mvel";

		Plan plan;
		plan = marmot.planBuilder("meta_data")
					.add(OperatorProto.newBuilder().setLoadTextfile(load).build())
					.update("land_type = land_type + 1")
					.expand("pnu:string", "pnu = sgg_cd + emdl_cd + land_type + bon_bun + bu_bun")
					.project("date,pnu,usage")
					.build();
		
		System.out.println(JsonFormat.printer().print(plan.toProto()));
	}
}
