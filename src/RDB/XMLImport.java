package RDB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import DuplicateDetection.StringMatching;

public class XMLImport {

    private Document _dataset;
    private String _objectName;
    private DataBase _db;
    private Set<String> _attributes;

    public XMLImport(String objectName, DataBase db, Set<String> attributes) {
        this._db = db;
        this._objectName = objectName;
        this._attributes = attributes;
    }

    public void setDocument(Document doc) {
        this._dataset = doc;
    }

    public void loadDatabase(int windowSize, int block) {

        Node el = _dataset.getDocumentElement();
        Element nd = (Element) el;

        NodeList nl = nd.getElementsByTagName(_objectName);

        int element;
        Node node;
        Map<String, List<String>> objectInfo;
        int size = nl.getLength();
        for (int i = 0; i < size; i++) {

            element = windowSize * block - (windowSize - i);

            // System.out.println("Loading Element: " + (element+1) + " ...");

            node = nl.item(i);

            objectInfo = transverseObject(node, new HashMap<String, List<String>>());
            
            objectInfo = completeObjectInfo(objectInfo, _attributes);
            
            String id = node.getAttributes().item(0).getTextContent();

            _db.insertObject(objectInfo, id, element);

        }

    }
    
    public void loadDatabaseFromDoc() {

        Node el = _dataset.getDocumentElement();
        Element nd = (Element) el;

        NodeList nl = nd.getElementsByTagName(_objectName);

        Node node;
        Map<String, List<String>> objectInfo;
        int size = nl.getLength();
        for (int i = 0; i < size; i++) {

            node = nl.item(i);

            objectInfo = transverseObject(node, new HashMap<String, List<String>>());
            
            objectInfo = completeObjectInfo(objectInfo, _attributes);

            String id = node.getAttributes().item(0).getTextContent();
            
            _db.insertObject(objectInfo,id, i);

        }

    }
    
    private Map<String,List<String>> completeObjectInfo(Map<String,List<String>> objInfo, Set<String> attributes){
        Iterator<String> it = attributes.iterator();
        List<String> lst_aux = new ArrayList<String>();
        lst_aux.add("");
        while(it.hasNext()){
            String attr = it.next();
            if(!objInfo.containsKey(attr)){
                objInfo.put(attr, lst_aux);
            }
        }
        return objInfo;
    }

    public Map<String, List<String>> transverseObject(Node n, Map<String, List<String>> row) {

        StringMatching sm = new StringMatching(); 
        NodeList nl = n.getChildNodes();
        List<String> lst;

        int size = nl.getLength();
        String nodeName;
        String nodeContent;

        for (int i = 0; size > i; i++) {

            Node node = nl.item(i);

            if (node.getNodeType() == Node.ELEMENT_NODE && 
                node.getChildNodes().getLength() == 1 &&
                !node.getChildNodes().item(0).hasChildNodes()) {

                nodeName = node.getNodeName();
                nodeContent = node.getTextContent();
                lst = new ArrayList<String>();

                if(!sm.isEmptyString(nodeContent)){

                    lst = row.get(nodeName);
                    if (lst!=null) {
                        lst.add(nodeContent);
                        row.put(nodeName, lst);
                    } else {
                        lst = new ArrayList<String>();
                        lst.add(nodeContent);
                        row.put(nodeName, lst);
                    }
                }
                
            }

            else {
                transverseObject(node, row);
            }

        }

        return row;
    }

}
