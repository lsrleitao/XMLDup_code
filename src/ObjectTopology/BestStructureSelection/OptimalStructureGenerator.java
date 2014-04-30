//Selecciona a melhor estrutura através de uma procura aleatoria

package ObjectTopology.BestStructureSelection;

import java.io.File;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;

import DuplicateDetection.ConfigGeneral;
import DuplicateDetection.XMLDup;
import ObjectTopology.SructureGenerator.Generator;
import ObjectTopology.SructureGenerator.MatrixToConf;
import ObjectTopology.XMLTransformer.DocumentTransformer;
import RDB.DataBase;
import RDB.XMLToTable;

public class OptimalStructureGenerator {

    private static String _configFile = "./XMLDup Data/DBConfigFile/config_restaurants.xml";
    private static String _dbPath = "./XMLDup Data/DB/restaurants.xml";
    private static int _maxNumberTopology = 100; /*Integer.MAX_VALUE*/

    public static void main(String[] args) {
        
        try{
            
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(new File(_configFile));
            String objectName = document.getDocumentElement().getNodeName();
            
            DocumentTransformer dt = new DocumentTransformer();
            document = dt.transformConfigFileUniqueLeaves(document);
            Map<String,List<String>> paths = dt.getPaths();
            String configFileEdited = _configFile.replace(".xml", "_edited.xml");
            dt.createFile(document, configFileEdited);
            
                String f_aux = _dbPath.replace(".xml", "_transformed.xml");
                double rPrecision;
                double rPrecisionMax = 0;
                int rPrecisionMaxIndex = -1;
                
                MatrixToConf mtfe = new MatrixToConf();
                LinkedHashMap<String, NamedNodeMap> ci = mtfe.loadConfigFile(configFileEdited);
                Generator gn = new Generator(ci);
                System.out.print("Generating Structure Topologies...");
                gn.generatePossibleStates(_maxNumberTopology);
                System.out.println("FINISHED!");

                MatrixToConf mtf = null;
                
                ConfigGeneral cg = new ConfigGeneral(document);
                
                DataBase rdb = DataBase.getSingletonObject(true);
                
                XMLToTable xmltt = new XMLToTable(document, cg.getDbObjectName(), rdb, dt.getUniqueAttributes(paths));
                xmltt.loadXMLDocToTable();
                
                List<int[][]> sl = gn.getStatesList();
                System.out.println(sl.size() + " possible Topologies");
                for(int j = 0 ; sl.size() > j ; j++){
                    System.out.println("Structure being processed: " + (j+1));
                
                    mtf = new MatrixToConf(sl.get(j), ci);
                    document = mtf.buildConfiguration();
                    //mtf.writeConfigurationFile(document, f2[i].getPath().replace(".xml", "_config.xml"));
                
                    dt.transformDBToConfigStructure(document, _dbPath, f_aux, paths, objectName, true);

                    cg = new ConfigGeneral(document);
                    
                    XMLDup xmldup = new XMLDup(document, f_aux, cg);
                    xmldup.run();
                    rPrecision = xmldup.getScores().get(0);
                    if(rPrecision > rPrecisionMax){
                        rPrecisionMax = rPrecision;
                        rPrecisionMaxIndex = j;
                        System.out.println("rPrecision Updated: " + rPrecisionMax);
                    }
                    
                    File f_temp = new File(f_aux);
                    f_temp.delete();
                }
                
                mtf = new MatrixToConf(sl.get(rPrecisionMaxIndex), ci);
                document = mtf.buildConfiguration();
                
                dt.transformDBToConfigStructure(document, _dbPath, f_aux, paths, objectName, true);
                
                cg = new ConfigGeneral(document);
                
                XMLDup xmldup = new XMLDup(document, f_aux, cg);
                xmldup.run();
                
                File f_temp = new File(f_aux);
                f_temp.delete();
                f_temp = new File(configFileEdited);
                f_temp.delete();
               
     
           
        }catch(Exception e){e.printStackTrace();}
    }

}
