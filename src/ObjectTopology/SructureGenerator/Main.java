package ObjectTopology.SructureGenerator;

import java.util.LinkedHashMap;
import java.util.List;

import org.w3c.dom.NamedNodeMap;

public class Main {

	private static String _configFile = "./ConfigFiles/config_restaurants.xml";

	public static void main(String[] args) {
	
		try{
		
			MatrixToConf mtfe = new MatrixToConf();
			LinkedHashMap<String, NamedNodeMap> ci = mtfe.loadConfigFile(_configFile);
			Generator gn = new Generator(ci);
			gn.generatePossibleStates(10000/*Integer.MAX_VALUE*/);
			
			List<int[][]> sl = gn.getStatesList();
			for(int i = 0 ; sl.size() > i ; i++){
			
			MatrixToConf mtf = new MatrixToConf(sl.get(i), ci);
			mtf.writeConfigurationFile(mtf.buildConfiguration(), "./ConfigFiles/config"+(i+1)+".xml");

		}

		//System.out.println(gn.getStates() + " generated States");
		
		}catch(Exception e){e.printStackTrace();}
	}

}
