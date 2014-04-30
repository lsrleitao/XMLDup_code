package DuplicateDetection;

import java.util.ArrayList;
import java.util.List;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import RDB.DataBase;

public class LoadStringSimilarityScores {

    public LoadStringSimilarityScores(String dbPath, DataBase rdb, Document configFile, String objectName){
        
        List<String> selectedAttributes = buildAttributesList(configFile.getChildNodes(), new ArrayList<String>());
        System.out.println("Loading similarity scores from attributes: " + selectedAttributes);
        //rdb.loadStringSimilarityScoresToMemory(selectedAttributes);
        rdb.loadStringSimilarityScoresToDB(selectedAttributes);
        
    }
    
    private List<String> buildAttributesList(NodeList nl, List<String> attributes){

        String nodeName;
        Node node;
        int cnSize;
        NodeList cn;
        
        NamedNodeMap nnm;
        for(int i = 0; nl.getLength() > i; i++){
            
            node = nl.item(i);
            cn = node.getChildNodes();
            cnSize = cn.getLength();
            
            if(node.getNodeType() == Node.ELEMENT_NODE){
                
                nnm = node.getAttributes();
                
                nodeName = node.getNodeName();
                
                if(cnSize == 0 && nnm.getNamedItem("useFlag")!= null && nnm.getNamedItem("useFlag").getTextContent().equals("1")){
                    attributes.add(nodeName);
                }
                else if(cnSize > 0){
                    attributes = buildAttributesList(node.getChildNodes(), attributes);
                }
                    
            }

        }
        
        return attributes;
    }
    
    
}
