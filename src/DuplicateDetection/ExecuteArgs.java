package DuplicateDetection;

import java.io.File;
import java.util.List;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import ObjectTopology.XMLTransformer.DocumentTransformer;
import RDB.DataBase;
import RDB.XMLToTable;

public class ExecuteArgs {

    private static boolean _ISORIGINALSTRUCTURE = true;
    private static String _OriginalConfigFile;
    private static String _configFile;

    public static void main(String[] args){
        
        if(args.length > 2){
            _ISORIGINALSTRUCTURE = false;
            _OriginalConfigFile = args[1];
            _configFile = args[2];
        }
        else{
            _OriginalConfigFile = args[1];
        }
        String fPath = args[0];
        
        try{
            
            if(_ISORIGINALSTRUCTURE){
                _configFile = _OriginalConfigFile;
            }
            
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document configDocument = builder.parse(new File(_configFile));
            String objectName = configDocument.getDocumentElement().getNodeName();
            
            Document originalConfigDocument = builder.parse(new File(_OriginalConfigFile));
                  
            ConfigGeneral cg = new ConfigGeneral(configDocument);
            

            String f_aux = fPath.replace(".xml", "_transformed.xml");
            DocumentTransformer dt = new DocumentTransformer();
            Document dbDoc = builder.parse(new File(fPath));

            System.out.print("Pre-processing strings...");
            Document transformedDb = dt.preProcessXMLStrings(dbDoc, cg.getDbObjectName());
            System.out.println("FINISH!");


            //transforma BD para formato do ficheiro de configuracao
            originalConfigDocument = dt.transformConfigFileUniqueLeaves(originalConfigDocument);
            Map<String,List<String>> paths = dt.getPaths();

            if(_ISORIGINALSTRUCTURE){
                configDocument = originalConfigDocument;
                cg = new ConfigGeneral(configDocument);
            }

            transformedDb = dt.transformDBToConfigStructureToDoc(configDocument, transformedDb, paths, objectName, false);


            dt.createFile(transformedDb, f_aux);
            //dt.createFile(configDocument, f2[i].getPath().replace(".xml", "_config.xml"));

            DataBase rdb = DataBase.getSingletonObject(true);

            XMLToTable xmltt = new XMLToTable(transformedDb, cg.getDbObjectName(), rdb, dt.getUniqueAttributes(paths));
            xmltt.loadXMLDocToTable();

            XMLDup xmldup = new XMLDup(configDocument, f_aux, cg);
            xmldup.run();

            File f_temp = new File(f_aux);
            f_temp.delete();

            
         
        }catch(Exception e){e.printStackTrace();}
    }

}
