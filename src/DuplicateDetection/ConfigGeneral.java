package DuplicateDetection;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * @author Lu�s Leit�o
 */

public class ConfigGeneral {

    private ConfigObj _configStructure;
    private float _threshold;

    // 0 - relational database
    // 1 - main memory
    private int _storageType;
    private Document _docStructure;
    private boolean _blocking;
    private String[] _key;
    private float[] _blockingParams;
    private float _uniquePF;
    private String _blockingAlgo;

    public ConfigGeneral(Document configFile) {

        Document document = configFile;
        
        try {
            
            this._docStructure = document;
            
            buildConfigStructureFromXML(document);
            
        } catch (Exception e) {
            System.out.println("ERRO NO FICHEIRO DE CONFIGURACAO!!!");
            e.printStackTrace();
        }

    }

    /**
     * Builds the configuration structure provided by the user
     * 
     * @param filePath The file path of the configuration file
     */
    public void buildConfigStructureFromXML(Document document) {

            Node rootNode = document.getDocumentElement();
            NodeList nl = rootNode.getChildNodes();

            ConfigObj co = new ConfigObj();

            _threshold = Float.parseFloat(rootNode.getAttributes().getNamedItem("threshold")
                    .getTextContent());

            _storageType = Integer.parseInt(rootNode.getAttributes().getNamedItem("mainMemory")
                    .getTextContent());
            
            _blocking = Boolean.parseBoolean(rootNode.getAttributes().getNamedItem("blocking").getTextContent());
            
            _key = rootNode.getAttributes().getNamedItem("key").getTextContent().split(",");
            
            _uniquePF = Float.parseFloat(rootNode.getAttributes().getNamedItem("uniquePF").getTextContent());
            
            _blockingAlgo = rootNode.getAttributes().getNamedItem("blockingAlgo").getTextContent();
            
            String[] params = rootNode.getAttributes().getNamedItem("blockingParams").getTextContent().split(",");
            _blockingParams = new float[params.length];
            for(int i = 0; params.length > i; i++){
                _blockingParams[i] = Float.parseFloat(params[i]);
            }
             
            
            String nodeName = rootNode.getNodeName();
            co.setNodeName(nodeName);

            _configStructure = buildConfigStructureAux(co, nl);

            // percorreEstruturaXMLTeste(_configStructure.getChildrenList());
            // System.out.println(_configStructure.getChildrenList());


        // System.out.println(co.getChildrenList().size());
        // System.out.println(co.getChildrenList().get(1).getChildrenList().get(1).getFormula());
    }

    /**
     * Used to build recursively the configuration structure
     * 
     * @param co The configuration object that will represent the node configuration
     * @param nl All nodes that belong to a given level of the tree
     * @return The configuration root object which represents the root node
     */
    public ConfigObj buildConfigStructureAux(ConfigObj co, NodeList nl) {

        List<ConfigObj> lst = new ArrayList<ConfigObj>();

        String nodeName;
        int useFlag;
        float defaultProb;
        String simMeasure;
        float simCut;
        boolean onlyAttributes;
        double pruningFactor;

        for (int i = 0; nl.getLength() > i; i++) {

            if (nl.item(i).getNodeType() == Node.ELEMENT_NODE) {

                useFlag = Integer.parseInt(nl.item(i).getAttributes().getNamedItem("useFlag")
                        .getTextContent());

                if (useFlag == 0)
                    continue;

                ConfigObj co_aux = new ConfigObj();              

                // comuns a folhas e nos internos
                nodeName = nl.item(i).getNodeName();
                simMeasure = nl.item(i).getAttributes().getNamedItem("simMeasure").getTextContent();
                simCut = Float.parseFloat(nl.item(i).getAttributes().getNamedItem("simCut")
                        .getTextContent());
                defaultProb = Float.parseFloat(nl.item(i).getAttributes().getNamedItem(
                        "defaultProb").getTextContent());
                onlyAttributes = Boolean.parseBoolean(nl.item(i).getAttributes().getNamedItem(
                        "onlyAttributes").getTextContent());
                co_aux.setNodeName(nodeName);
                co_aux.setSimMeasure(simMeasure);
                co_aux.setSimCut(simCut);
                co_aux.setDefaultProb(defaultProb);
                co_aux.setOnlyAttributes(onlyAttributes);

                if (nl.item(i).getChildNodes().getLength() == 0) {

                    co_aux.setNodeType(0);// 0 means the node is a leaf
                    pruningFactor = Double.parseDouble(nl.item(i).getAttributes().getNamedItem("pf").getTextContent());
                    co_aux.setPruningFactor(pruningFactor);
                }

                if (nl.item(i).getChildNodes().getLength() > 0) {

                    co_aux.setNodeType(1);// 1 means the node is an inner node

                    buildConfigStructureAux(co_aux, nl.item(i).getChildNodes());

                }

                lst.add(co_aux);
                co.setChildrenList(lst);

                Element e = (Element) nl.item(i);

                if (e.hasAttribute("attributes")) {
                    String attrib = e.getAttribute("attributes").replaceAll(" ", "");
                    if (attrib.isEmpty()) {
                        co_aux.setAttributes(new String[0]);
                    } else {
                        String[] attr = attrib.split(",");
                        co_aux.setAttributes(attr);
                    }
                } else
                    co_aux.setAttributes(new String[0]);

                // System.out.println("Name: "+nl.item(i).getNodeName());
                // System.out.println("Type: "+nl.item(i).getNodeType());
                // System.out.println("Value: "+nl.item(i).getNodeValue());
                // System.out.println("Child: "+nl.item(i).getChildNodes().getLength());
            }
        }

        return co;
    }

    public float getThreshold() {
        return _threshold;
    }

    public ConfigObj getConfigStructure() {
        return _configStructure;
    }
    
    public Document getStructureDoc(){
        return this._docStructure;
    }

    public String getDbObjectName() {
        return _configStructure.getNodeName();
    }

    public int getStorageType() {
        return _storageType;
    }
    
    public boolean useBlocking(){
        return _blocking;
    }
    
    public float getUniquePF(){
        return _uniquePF;
    }
    
    public String[] getkey() {
        return _key;
    }
    
    public String getBlockingAlgo(){
        return _blockingAlgo;
    }
    
    public float[] getBlockingParams() {
        return _blockingParams;
    }

    public void setConfigStructure(ConfigObj cs) {
        this._configStructure = cs;
    }

}
