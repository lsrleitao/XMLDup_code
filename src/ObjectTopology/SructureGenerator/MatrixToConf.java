package ObjectTopology.SructureGenerator;

import java.io.File;
import java.io.IOException;

import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class MatrixToConf {
	
	private int[][] _structure;
	private LinkedHashMap<String, NamedNodeMap> _attributes;
	private int _attributesSize;
	private int _groupNumber = 0;

	public MatrixToConf(){}
	
	public MatrixToConf(int[][] structure, LinkedHashMap<String, NamedNodeMap> configInfo){
		this._structure = structure;
		this._attributes = configInfo;
		this._attributesSize = configInfo.size()-1;
	}
	
	public Document buildConfiguration() throws ParserConfigurationException{
		
		DocumentBuilderFactory documentBuilderFactory = 
        DocumentBuilderFactory.newInstance();
		DocumentBuilder documentBuilder = 
        documentBuilderFactory.newDocumentBuilder();
		Document document = documentBuilder.newDocument();
		String elementName = getLinkedHashMapPosition(_attributes,0);
		Element rootElement = document.createElement(elementName);
		rootElement = addAttributes(rootElement, _attributes.get(elementName));
		
		document.appendChild(rootElement);
		
		Element em = null;
		for(int j = 0; _attributesSize+(_attributesSize-2) > j; j++){
			if(_structure[_attributesSize+(_attributesSize-2)][j] == 1){
				if(j < _attributesSize){
					elementName = getLinkedHashMapPosition(_attributes,j+1);
					em = document.createElement(elementName);
					em = addAttributes(em, _attributes.get(elementName));
				}else{
					_groupNumber++;
					Element groupElement = document.createElement("group"+_groupNumber);
					em = transverseMatrix(groupElement, j, document);
					em = addAttributesToGroupNode(em);
				}
				rootElement.appendChild(em);
				
			}
		}
		
		
		return document;
	}
	
	private Element transverseMatrix(Element el, int nodeIndex, Document document){
		
		Element em = null;
		
		for(int j = 0; _attributesSize+(_attributesSize-2) > j; j++){
			
			if(_structure[nodeIndex][j] == 1){
				if(j < _attributesSize){
					String elementName = getLinkedHashMapPosition(_attributes,j+1);
					em = document.createElement(elementName);
					em = addAttributes(em, _attributes.get(elementName));
				}else{
					_groupNumber++;
					Element groupElement = document.createElement("group"+_groupNumber);
					em = transverseMatrix(groupElement, j, document);
					em = addAttributesToGroupNode(em);
				}
				
				el.appendChild(em);
			}
			
		}
		
		return el;
	}
	
	public void writeConfigurationFile(Document document, String file) throws TransformerException{
	    TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        DOMSource source = new DOMSource(document);
        StreamResult result =  new StreamResult(file);
        
        transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
        
        
        transformer.transform(source, result);
	}
	
	public LinkedHashMap<String,NamedNodeMap> loadConfigFile(String filePath) throws ParserConfigurationException, IOException, SAXException{
	
		LinkedHashMap<String,NamedNodeMap> res = new LinkedHashMap<String,NamedNodeMap>();
		
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(new File(filePath));
        
        Node rootNode = document.getDocumentElement();
        res.put(rootNode.getNodeName(), rootNode.getAttributes());
        
        res = loadConfigFileAux(rootNode.getChildNodes(), res);
             
        return res;
	}
	
	public LinkedHashMap<String,NamedNodeMap> loadConfigFileAux(NodeList nl, LinkedHashMap<String,NamedNodeMap> elements){
		
		LinkedHashMap<String,NamedNodeMap> res = elements;
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
	    	    
	    		if(!considerElement(nnm)){
	    			continue;
	    		}
	    		
	    		nodeName = node.getNodeName();
	    		
	    		if(cnSize == 0){
	    			res.put(nodeName, nnm);
	    			continue;
	    		}
	    		
	    		if(cnSize > 0){
	    			res = loadConfigFileAux(node.getChildNodes(), res);
	    		}
	    			
	    	}

	    }
	    
	    return res;
	}
	
	private String getLinkedHashMapPosition(LinkedHashMap<String, NamedNodeMap> lhm, int i){

		Enumeration<String> it = Collections.enumeration(lhm.keySet());

		String key;
		int cnt = 0;
		while(it.hasMoreElements()){
			key = it.nextElement();
			if(cnt == i){
				return key;
			}
			cnt++;
		}	
		
		return null;	
	}
	
	private Element addAttributes(Element elm, NamedNodeMap attributes){
	    String name;
	    String value;
	    Node n;
		for(int i = 0; attributes.getLength() > i; i++){
		    n = attributes.item(i);
			name = n.getNodeName();
			value = n.getNodeValue();
			elm.setAttribute(name, value);
		}
		
		return elm;
	}
	
	private boolean considerElement(NamedNodeMap attributes){
		Node n = attributes.getNamedItem("useFlag");
		if(n.getTextContent().equals("1"))
			return true;
		else
			return false;
	}
	
	private Element addAttributesToGroupNode(Element elm){
		
		elm.setAttribute("attributes", "");
		elm.setAttribute("onlyAttributes", "false");
		elm.setAttribute("simCut", "0");
		elm.setAttribute("defaultProb", "0.5");
		elm.setAttribute("useFlag", "1");
		elm.setAttribute("formula", "MEDIA");
		elm.setAttribute("simMeasure", "levenstein");
		
		return elm;
	}
	
}
