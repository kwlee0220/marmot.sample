package marmot.geom;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;

import com.google.common.collect.Maps;

import utils.StopWatch;

import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleGeoServerPlugin {
//	private static final String INPUT = "POI/주유소_가격";
	private static final String INPUT = "기타/안양대/도봉구/민원";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		String host = MarmotClientCommands.getMarmotHost();
		int port = MarmotClientCommands.getMarmotPort();
		
		Map<String,Serializable> params = Maps.newHashMap();
		params.put("host", host);
		params.put("port", port);
		params.put("dataset", INPUT);
		
		DataStore store = DataStoreFinder.getDataStore(params);
		String[] names = store.getTypeNames();
		
		System.out.println(Arrays.toString(names));
		
		SimpleFeatureType type = store.getSchema(names[0]);
        System.out.println("featureType  name: " + type.getName());
        System.out.println("featureType count: " + type.getAttributeCount());
        
        SimpleFeatureSource src = store.getFeatureSource(names[0]);
        
        for (AttributeDescriptor descriptor : type.getAttributeDescriptors()) {
            System.out.print("  " + descriptor.getName());
            System.out.print(" (" + descriptor.getMinOccurs() + "," + descriptor.getMaxOccurs()
                    + ",");
            System.out.print((descriptor.isNillable() ? "nillable" : "manditory") + ")");
            System.out.print(" type: " + descriptor.getType().getName());
            System.out.println(" binding: " + descriptor.getType().getBinding().getSimpleName());
        }

        AttributeDescriptor attributeDescriptor = type.getDescriptor(0);
        System.out.println("attribute 0    name: " + attributeDescriptor.getName());
        System.out.println("attribute 0    type: " + attributeDescriptor.getType().toString());
        System.out.println("attribute 0 binding: " + attributeDescriptor.getType().getBinding());
        
//        AttributeDescriptor cityDescriptor = type.getDescriptor("휘발유");
//        System.out.println("attribute '휘발유'    name: " + cityDescriptor.getName());
//        System.out.println("attribute '휘발유'    type: " + cityDescriptor.getType().toString());
//        System.out.println("attribute '휘발유' binding: " + cityDescriptor.getType().getBinding());

        // default geometry
        GeometryDescriptor geometryDescriptor = type.getGeometryDescriptor();
        System.out.println("default geom    name: " + geometryDescriptor.getName());
        System.out.println("default geom    type: " + geometryDescriptor.getType().toString());
        System.out.println("default geom binding: " + geometryDescriptor.getType().getBinding());
        System.out.println("default geom     crs: "
                + CRS.toSRS(geometryDescriptor.getCoordinateReferenceSystem()));
	}
}
