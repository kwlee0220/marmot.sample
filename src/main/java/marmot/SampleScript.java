package marmot;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.PropertyConfigurator;

import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.script.MarmotScriptEngine;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleScript {
	private static final String SCRIPT_00 = "scripts/dataset.mcs";
	private static final String SCRIPT_01 = "scripts/run_plan.mcs";
	private static final String SCRIPT_02 = "scripts/import_shapefile.mcs";
	private static final String SCRIPT_03 = "scripts/import_csv.mcs";
	private static final String SCRIPT_04 = "scripts/anyang_minwon.mcs";
	private static final String SCRIPT_05 = "scripts/biz_area.mcs";
	private static final String SCRIPT_06 = "scripts/10minutes_policy.mcs";
	private static final String SCRIPT_07 = "scripts/anyang_energe.mcs";
	
	public static final void main(String... args) throws Exception {
//		PropertyConfigurator.configure("log4j.properties");
//		MarmotClientCommands.configureLog4j();
		MarmotClientCommands.disableLog4j();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		String script = loadScript(SCRIPT_07);
//		System.out.println(script);
		
		MarmotScriptEngine engine = new MarmotScriptEngine(marmot).setVerbose(true);
		engine.evaluate(script);
	}
	
	private static String loadScript(String path) throws IOException {
		return IOUtils.toString(new File(path));
	}
}
