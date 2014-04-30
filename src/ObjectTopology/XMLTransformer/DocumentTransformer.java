package ObjectTopology.XMLTransformer;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

import DuplicateDetection.StringMatching;
import antlr.collections.Enumerator;

public class DocumentTransformer {

    private Map<String,List<String>> _paths = new HashMap<String,List<String>>();
    private List<String> _nodes = new ArrayList<String>();
    private Document _configDocument;
    private StringMatching _sm = new StringMatching();

    public DocumentTransformer(){}

    public Map<String,List<String>> getPaths(){
        return _paths;
    }

    public Document transformConfigFileUniqueLeaves(Document configFile){

        Node n = configFile.getDocumentElement();
        NodeList nl = n.getChildNodes();
        this._configDocument = configFile;

        walkConfigStructure(nl, "");

        return _configDocument;
    }

    /*private void changeNodeNameIndex(){
	    for(int i = 0; _nodes.size() > i ; i++){
	        _configDocument.renameNode(n, namespaceURI, qualifiedName)
	    }
	}*/

    public Set<String> getUniqueAttributes(Map<String,List<String>> attrPaths){
        
        Set<String> attrSet = new HashSet<String>(); 
        
        for(Map.Entry<String, List<String>> e: attrPaths.entrySet()){
            for(int i = 0; e.getValue().size() > i; i++){
                String idNum = "";
                if(i > 0){
                    idNum = Integer.toString(i+1);                    
                }
                attrSet.add(e.getKey() + idNum);
            }            
        }
        
        return attrSet;
    }
    
    private String walkConfigStructure(NodeList nl, String path){

        String path_aux = path;

        for(int i=0; nl.getLength() > i; i++){

            path = path_aux;

            Node n = nl.item(i);
            NodeList childNodes = n.getChildNodes();
            int cnSize = childNodes.getLength();
            int pathsNumber;
            String nodeName = n.getNodeName();
            List<String> nodeNamePaths;
            List<String> lst;
            if(n.getNodeType() == Node.ELEMENT_NODE){
                if(cnSize == 0){
                    if(_paths.containsKey(nodeName)){
                        nodeNamePaths = _paths.get(nodeName);
                        nodeNamePaths.add(path);
                        pathsNumber = nodeNamePaths.size();
                        _nodes.add(nodeName);
                        if(pathsNumber > 1){
                            _configDocument.renameNode(n, "", nodeName+pathsNumber);
                        }
                    }
                    else{
                        lst = new ArrayList<String>();
                        lst.add(path);
                        _paths.put(nodeName, lst);
                    }

                }
                else if(cnSize > 0){
                    path = walkConfigStructure(childNodes, path + n.getNodeName() + "\\");
                }
            }
        }
        return path;
    }

    public void transformDBToConfigStructure(Document configFile, String dbFile, String outputFile, Map<String,List<String>> paths, String objectName, boolean uniqueLeavesAlreadyTagged) throws Exception{

        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
        Document documentOutput = documentBuilder.newDocument();

        DocumentBuilderFactory factory2 = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder2 = factory2.newDocumentBuilder();
        Document documentDB = builder2.parse(new File(dbFile));

        Element el = documentOutput.createElement("db");
        documentOutput.appendChild(el);

        NodeList nl = documentDB.getElementsByTagName(objectName);

        XMLToXML trf;
        Node n_aux;
        for(int i = 0; nl.getLength() > i; i++){
            trf = new XMLToXML(configFile, paths);

            if(uniqueLeavesAlreadyTagged)
                n_aux = documentOutput.adoptNode(trf.transformNodeAssigned(nl.item(i)));
            else
                n_aux = documentOutput.adoptNode(trf.transformNode(nl.item(i)));

            documentOutput.getDocumentElement().appendChild(n_aux);
        }

        createFile(documentOutput, outputFile);
    }

    public Document transformDBToConfigStructureToDoc(Document configFile, Document dbFile, Map<String,List<String>> paths, String objectName, boolean uniqueLeavesAlreadyTagged) throws Exception{

        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
        Document documentOutput = documentBuilder.newDocument();

        Element el = documentOutput.createElement("db");
        documentOutput.appendChild(el);

        NodeList nl = dbFile.getElementsByTagName(objectName);

        XMLToXML trf;
        Node n_aux;
        for(int i = 0; nl.getLength() > i; i++){
            trf = new XMLToXML(configFile, paths);

            if(uniqueLeavesAlreadyTagged){
                n_aux = documentOutput.adoptNode(trf.transformNodeAssigned(nl.item(i)));
            }
            else{
                n_aux = documentOutput.adoptNode(trf.transformNode(nl.item(i)));
            }
            //System.out.println(nl.item(i).getTextContent());
            if(n_aux == null){
                System.out.println("Nó devolvido a Null!!!!!");
                System.exit(0);
            }
            documentOutput.getDocumentElement().appendChild(n_aux);
        }

        return documentOutput;
    }

    public Document preProcessXMLStrings(Document dbFile, String objectName) throws Exception{

        NodeList nl = dbFile.getElementsByTagName(objectName);
        for(int i = 0; nl.getLength() > i; i++){
            Node n = nl.item(i);
            preProcessAttributes(n);
            transverseObject(n);
        }
        return dbFile;
    }

    private void transverseObject(Node el) throws Exception{

        NodeList nl = el.getChildNodes();

        for(int j = 0; nl.getLength() > j; j++){
            Node n = nl.item(j);

            if(n.getNodeType() == Node.ELEMENT_NODE && 
               n.getChildNodes().getLength() == 1 &&
               !n.getChildNodes().item(0).hasChildNodes()){

                //se uma folha estiver vazia e não tiver informaçao nos atributos
                //nao adicionar o no
                String content = _sm.preProcessString(n.getTextContent());
                
                /*if(content.isEmpty() && n.getAttributes().getLength() == 0){
                    continue;
                }
                else{*/
                    preProcessAttributes(n);
                    Text textNode = (Text)n.getFirstChild();
                    textNode.setData(content);
                //}

            }
            else if(n.getNodeType() == Node.ELEMENT_NODE){
                preProcessAttributes(n);
                transverseObject(n); 
            }
        }

    }

    private void preProcessAttributes(Node n){
        NamedNodeMap nnm = n.getAttributes();
        for(int i = 0; nnm.getLength() > i; i++){
            Node n_aux = nnm.item(i);
            n_aux.setTextContent(_sm.preProcessString(n_aux.getTextContent()));
        }
    }

    public void extractTestSet(String outputFile, String dbFile, String objectName, int testSize) throws Exception{

        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
        Document documentOutput = documentBuilder.newDocument();

        DocumentBuilderFactory factory2 = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder2 = factory2.newDocumentBuilder();
        Document documentDB = builder2.parse(new File(dbFile));

        Element el = documentOutput.createElement("db");
        documentOutput.appendChild(el);

        NodeList nl = documentDB.getElementsByTagName(objectName);

        List<Integer> lst = new ArrayList<Integer>();
        Random r = new Random();
        int index;
        int objectNumber = Math.round(((float)nl.getLength())*((float)testSize/100f));

        while(lst.size() < objectNumber){
            index = r.nextInt(nl.getLength());

            if(lst.contains(index))
                continue;
            else
                lst.add(index);

        }

        Node n_aux;
        for(int i = 0; lst.size() > i; i++){
            n_aux = documentOutput.importNode(nl.item(i), true);
            documentOutput.getDocumentElement().appendChild(n_aux);
        }

        createFile(documentOutput, outputFile);
    }

    public void extractTestSetBalanced(String outputFile, String dbFile, String objectName, int testSizeDuplicates, int testSizeNonDuplicates) throws Exception{

        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
        Document documentOutput = documentBuilder.newDocument();

        DocumentBuilderFactory factory2 = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder2 = factory2.newDocumentBuilder();
        Document documentDB = builder2.parse(new File(dbFile));

        Element el = documentOutput.createElement("db");
        documentOutput.appendChild(el);

        NodeList nl = documentDB.getElementsByTagName(objectName);

        Hashtable<String,List<List<Integer>>> pairs = getPairsByType(nl);
        List<List<Integer>> duplicates = pairs.get("duplicates");
        List<List<Integer>> non_duplicates = pairs.get("non_duplicates");

        List<Integer> selectedDups = selectDupObjectsPercentage(testSizeDuplicates, duplicates);
        List<Integer> selectedNonDups = selectNonDupObjectsPercentage(testSizeNonDuplicates, non_duplicates);

        Node n_aux;
        int index;
        for(int i = 0; selectedDups.size() > i; i++){
            index = selectedDups.get(i);
            n_aux = documentOutput.importNode(nl.item(index), true);
            documentOutput.getDocumentElement().appendChild(n_aux);
        }
        for(int i = 0; selectedNonDups.size() > i; i++){
            index = selectedNonDups.get(i);
            n_aux = documentOutput.importNode(nl.item(index), true);
            documentOutput.getDocumentElement().appendChild(n_aux);
        }

        createFile(documentOutput, outputFile);
    }

    public Document extractTestSetCriticalElementsToDoc(String resultsFile, Document dbFile, String objectName,
                                                        int citicalPairsSampleRateTP,
                                                        int citicalPairsSampleRateFP,
                                                        int citicalPairsSampleRateTN,
                                                        int citicalPairsSampleRateFN) throws Exception{

        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
        Document documentOutput = documentBuilder.newDocument();

        Element el = documentOutput.createElement("db");
        documentOutput.appendChild(el);

        NodeList nl = dbFile.getElementsByTagName(objectName);

        List<HashSet<Integer>> samples = new ArrayList<HashSet<Integer>>(); 

        HashSet<Integer> cumulatedObjIndex = new HashSet<Integer>();

        Hashtable<Integer,List<Integer>> criticalObjectsIndexFP = getCriticalObjectsIndex(resultsFile, "FP", cumulatedObjIndex);
        System.out.println("FP: " + criticalObjectsIndexFP.size());
        HashSet<Integer> criticalObjectsIndexFPSample = selectSample(criticalObjectsIndexFP, citicalPairsSampleRateFP);
        samples.add(criticalObjectsIndexFPSample);
        System.out.println("FP Sample: " + criticalObjectsIndexFPSample.size());

        cumulatedObjIndex.addAll(criticalObjectsIndexFPSample);
        System.out.println(cumulatedObjIndex.size());

        Hashtable<Integer,List<Integer>> criticalObjectsIndexFN = getCriticalObjectsIndex(resultsFile, "FN", cumulatedObjIndex);
        System.out.println("FN: " + criticalObjectsIndexFN.size());
        HashSet<Integer> criticalObjectsIndexFNSample = selectSample(criticalObjectsIndexFN, citicalPairsSampleRateFN);
        samples.add(criticalObjectsIndexFNSample);
        System.out.println("FN Sample: " + criticalObjectsIndexFNSample.size());

        cumulatedObjIndex.addAll(criticalObjectsIndexFNSample);
        System.out.println(cumulatedObjIndex.size());

        Hashtable<Integer,List<Integer>> criticalObjectsIndexTP = getCriticalObjectsIndex(resultsFile, "TP", cumulatedObjIndex);
        System.out.println("TP: " + criticalObjectsIndexTP.size());
        HashSet<Integer> criticalObjectsIndexTPSample = selectSample(criticalObjectsIndexTP, citicalPairsSampleRateTP);
        samples.add(criticalObjectsIndexTPSample);
        System.out.println("TP Sample: " + criticalObjectsIndexTPSample.size());

        cumulatedObjIndex.addAll(criticalObjectsIndexTPSample);
        System.out.println(cumulatedObjIndex.size());

        Hashtable<Integer,List<Integer>> criticalObjectsIndexTN = getCriticalObjectsIndex(resultsFile, "TN", cumulatedObjIndex);
        System.out.println("TN: " + criticalObjectsIndexTN.size());
        HashSet<Integer> criticalObjectsIndexTNSample = selectSample(criticalObjectsIndexTN, citicalPairsSampleRateTN);
        samples.add(criticalObjectsIndexTNSample);
        System.out.println("TN Sample: " + criticalObjectsIndexTNSample.size());

        cumulatedObjIndex.addAll(criticalObjectsIndexTNSample);
        System.out.println(cumulatedObjIndex.size());

        HashSet<Integer> uniqueness = new HashSet<Integer>();
        for(int i = 0; samples.size() > i; i++){

            Enumeration<Integer> it = Collections.enumeration(samples.get(i));
            //System.out.println(samples.get(i).size());
            int index;
            Node n_aux;
            while(it.hasMoreElements()){
                index = it.nextElement();

                if(uniqueness.contains(index)){
                    continue;
                }
                else{    
                    n_aux = documentOutput.importNode(nl.item(index), true);
                    documentOutput.getDocumentElement().appendChild(n_aux);
                    uniqueness.add(index);
                }
            }

        }

        return documentOutput;
    }

    private HashSet<Integer> selectSample(Hashtable<Integer,List<Integer>> coi, int sampleRate){

        HashSet<Integer> res = new HashSet<Integer>();

        List<Integer> lst_aux = new ArrayList<Integer>();

        Enumeration<Integer> e = coi.keys();
        while(e.hasMoreElements()){
            lst_aux.add(e.nextElement());
        }

        int size = lst_aux.size();
        float sample = (float)size*(float)sampleRate/(float)100;

        Random r = new Random();

        int index;
        while(res.size() < sample && lst_aux.size() != 0){
            index = r.nextInt(lst_aux.size());
            int objIndex = lst_aux.get(index);

            int containsPairedIndex = containsPairedIndex(res, coi.get(objIndex));

            if(!res.contains(objIndex) && containsPairedIndex != -1){
                res.add(objIndex);
                res.add(containsPairedIndex);
                lst_aux.remove(index);
            }
            else if(res.contains(objIndex) && containsPairedIndex != -1){
                res.add(containsPairedIndex);
            }
            else if(!res.contains(objIndex) && containsPairedIndex == -1){
                res.add(objIndex);
                lst_aux.remove(index);
            }

        }

        return res;
    }

    private int containsPairedIndex(HashSet<Integer> res, List<Integer> lst){

        List<Integer> lst_aux = new ArrayList<Integer>();

        int index;
        for(int i = 0; lst.size() > i; i++){
            index = lst.get(i);
            if(!res.contains(index)){
                lst_aux.add(index);
            }
        }

        if(lst_aux.size() > 0){
            Random r = new Random();
            return r.nextInt(lst_aux.size());
        }
        else{
            return -1;
        }
    }

    private Hashtable<Integer,List<Integer>> getCriticalObjectsIndex(String resultsFile, String pairType, HashSet<Integer> cumulatedObjIndex){

        Hashtable<Integer,List<Integer>> coi = new  Hashtable<Integer,List<Integer>>();

        try{

            FileInputStream fin = new FileInputStream(resultsFile);
            BufferedInputStream bis = new BufferedInputStream(fin);
            BufferedReader in = new BufferedReader(new InputStreamReader(bis));
            boolean more = true;
            String aux = null;

            more = true;
            while(more){
                aux = in.readLine();
                if (aux == null) {
                    more = false;
                } else {
                    String[] line = aux.split("=");
                    String[] line_aux = line[1].split("and");

                    int obj1 = Integer.parseInt(line_aux[0].trim());
                    int obj2 = Integer.parseInt(line_aux[1].trim());

                    aux = in.readLine();
                    line = aux.split("=");
                    double similarity = Double.parseDouble(line[1].trim());

                    aux = in.readLine();
                    boolean isDup;
                    if(aux.equals("DUP? = true"))
                        isDup = true;
                    else
                        isDup = false;

                    //True Positives
                    if(pairType.equals("TP") && similarity >= 0.75 && isDup){
                        if(!cumulatedObjIndex.contains(obj1) || !cumulatedObjIndex.contains(obj2)){
                            coi = updateCriticalObjects(coi, obj1, obj2);
                        }
                    }

                    //False Positives
                    if(pairType.equals("FP") && similarity >= 0.75 && !isDup){
                        if(!cumulatedObjIndex.contains(obj1) || !cumulatedObjIndex.contains(obj2)){
                            coi = updateCriticalObjects(coi, obj1, obj2);
                        }
                    }

                    //True Negatives
                    if(pairType.equals("TN") && similarity < 0.75 && !isDup){
                        if(!cumulatedObjIndex.contains(obj1) || !cumulatedObjIndex.contains(obj2)){
                            coi = updateCriticalObjects(coi, obj1, obj2);
                        }
                    }

                    //False Negatives
                    if(pairType.equals("FN") && similarity < 0.75 && isDup){
                        if(!cumulatedObjIndex.contains(obj1) || !cumulatedObjIndex.contains(obj2)){
                            coi = updateCriticalObjects(coi, obj1, obj2);
                        }
                    }

                    in.readLine();
                }
            }

        }catch(Exception e){e.printStackTrace();}

        return coi;
    }

    private Hashtable<Integer,List<Integer>> updateCriticalObjects(Hashtable<Integer,List<Integer>> coi, int obj1, int obj2){

        List<Integer> lst = new ArrayList<Integer>();

        if(coi.containsKey(obj1) && coi.containsKey(obj2)){
            coi.get(obj1).add(obj2);
            coi.get(obj2).add(obj1);
        }
        else if(coi.containsKey(obj1) && !coi.containsKey(obj2)){
            coi.get(obj1).add(obj2);
            lst.add(obj1);
            coi.put(obj2, lst);
        }
        else if(!coi.containsKey(obj1) && coi.containsKey(obj2)){
            coi.get(obj2).add(obj1);
            lst.add(obj2);
            coi.put(obj1, lst);
        }
        else if(!coi.containsKey(obj1) && !coi.containsKey(obj2)){
            lst.add(obj2);
            coi.put(obj1, lst);
            lst = new ArrayList<Integer>();
            lst.add(obj1);
            coi.put(obj2, lst);
        }

        return coi;
    }

    public Document extractTestSetBalancedToDoc(Document dbFile, String objectName, int testSizeDuplicates, int testSizeNonDuplicates) throws Exception{

        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
        Document documentOutput = documentBuilder.newDocument();

        Element el = documentOutput.createElement("db");
        documentOutput.appendChild(el);

        NodeList nl = dbFile.getElementsByTagName(objectName);

        Hashtable<String,List<List<Integer>>> pairs = getPairsByType(nl);
        List<List<Integer>> duplicates = pairs.get("duplicates");
        List<List<Integer>> non_duplicates = pairs.get("non_duplicates");

        List<Integer> selectedDups = selectDupObjectsPercentage(testSizeDuplicates, duplicates);
        List<Integer> selectedNonDups = selectNonDupObjectsPercentage(testSizeNonDuplicates, non_duplicates);

        Node n_aux;
        int index;
        for(int i = 0; selectedDups.size() > i; i++){
            index = selectedDups.get(i);
            n_aux = documentOutput.importNode(nl.item(index), true);
            documentOutput.getDocumentElement().appendChild(n_aux);
        }
        for(int i = 0; selectedNonDups.size() > i; i++){
            index = selectedNonDups.get(i);
            n_aux = documentOutput.importNode(nl.item(index), true);
            documentOutput.getDocumentElement().appendChild(n_aux);
        }

        return documentOutput;
    }

    private List<Integer> selectDupObjectsPercentage(float percentage, List<List<Integer>> lst){

        List<Integer> res = new ArrayList<Integer>();
        int objects = countObjects(lst);
        int objectNumber = Math.round(((float)objects)*(percentage/100f));

        Random r = new Random();
        int index;
        int index2;
        int index3;

        while(res.size() < objectNumber){

            index = r.nextInt(lst.size());

            //se so tem um é porque os restantes ja foram consumidos.
            if(lst.get(index).size() == 1){
                res.add(lst.get(index).get(0));
                lst.remove(index);
                continue;
            }

            index2 = r.nextInt(lst.get(index).size());
            res.add(lst.get(index).get(index2));
            lst.get(index).remove(index2);

            index3 = r.nextInt(lst.get(index).size());
            res.add(lst.get(index).get(index3));
            lst.get(index).remove(index3);

            if(lst.get(index).size() == 0){
                lst.remove(index);
            }

        }

        return res;
    }

    private List<Integer> selectNonDupObjectsPercentage(float percentage, List<List<Integer>> lst){

        List<Integer> res = new ArrayList<Integer>();
        int objects = countObjects(lst);
        int objectNumber = Math.round(((float)objects)*(percentage/100f));

        Random r = new Random();
        int index;

        while(res.size() < objectNumber){

            index = r.nextInt(lst.size());

            res.add(lst.get(index).get(0));
            lst.remove(index);

        }

        return res;
    }

    private int countObjects(List<List<Integer>> lst){
        int res = 0;
        for(int i = 0; lst.size() > i; i++){//System.out.println(lst.get(i));
            res = res + lst.get(i).size();
        }
        return res;
    }

    private Hashtable<String,List<List<Integer>>> getPairsByType(NodeList nl){

        List<List<Integer>> duplicates = new ArrayList<List<Integer>>();
        List<List<Integer>> non_duplicates = new ArrayList<List<Integer>>();

        Node n;
        String id;
        int size = nl.getLength();
        Hashtable<String,List<Integer>> lst = new Hashtable<String,List<Integer>>();
        for(int i = 0; size > i; i++){
            n = nl.item(i);
            id = n.getAttributes().item(0).getTextContent();
            if(lst.containsKey(id)){
                lst.get(id).add(i);
            }
            else{
                List<Integer> l_temp = new ArrayList<Integer>();
                l_temp.add(i);
                lst.put(id, l_temp);
            }
        }

        Enumeration<List<Integer>> e = lst.elements();
        List<Integer> l;
        while(e.hasMoreElements()){
            l = e.nextElement();
            if(l.size() > 1){
                duplicates.add(l);
            }
            else{
                non_duplicates.add(l);
            }
        }

        Hashtable<String,List<List<Integer>>> res = new Hashtable<String,List<List<Integer>>>();
        res.put("duplicates", duplicates);
        res.put("non_duplicates", non_duplicates);

        return res;

    }

    public void createFile(Document outputDoc, String outputFile) throws Exception{
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
        transformer.transform(new DOMSource(outputDoc), new StreamResult(new FileOutputStream(outputFile)));
    }
}
