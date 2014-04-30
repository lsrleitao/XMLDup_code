package ObjectTopology.XMLTransformer;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

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

import DuplicateDetection.StringMatching;

public class XMLToXML {

    private Document _OutputXMLDoc;
    private Hashtable<String,List<Node>> _leafs = new Hashtable<String,List<Node>>();
    private Document _XMLStructureDoc;
    private Node _newObject;
    private Map<String,List<String>> _paths;
    private StringMatching _sm = new StringMatching();

	public XMLToXML(Document structureDoc, Map<String,List<String>> paths){
		
	    try{
	    
    	    DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
    	    DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
    	    Document document = documentBuilder.newDocument();
    		
    		this._OutputXMLDoc = document;
    		this._XMLStructureDoc = structureDoc;
    		this._paths = paths;
		
        } catch (Exception e) {
            System.out.println("ERRO NO FICHEIRO DE CONFIGURACAO!!!");
            e.printStackTrace();
        }
	}
	
	public Node transformNode(Node originalNode){
	
		Node nf = null;
		
		try{

		    loadNodes(originalNode.getChildNodes(), "");

		    nf = transferDataToNewNode(_leafs, (Element) originalNode);
		    
		    }catch (Exception e){
		      System.err.println("Error: " + e.getMessage());
		    }
		    
		    return nf;
	}
	
	   public Node transformNodeAssigned(Node originalNode){
	       
	        Node nf = null;
	        
	        try{

	            loadNodesAssigned(originalNode.getChildNodes());

	            nf = transferDataToNewNodeAssigned(_leafs, (Element) originalNode);
	            
	            }catch (Exception e){
	              System.err.println("Error: " + e.getMessage());
	            }
	            
	            return nf;
	    }
	
	private Element transferDataToNewNode(Hashtable<String,List<Node>> contents, Element el){
		
		Element register = this._OutputXMLDoc.createElement(this._XMLStructureDoc.getDocumentElement().getNodeName());
		
		NamedNodeMap attrs = el.getAttributes();
		Node n;
		for (int j=0; j<attrs.getLength(); j++) {
		    n = attrs.item(j);
			register.setAttribute(n.getNodeName(), n.getTextContent());
		}
		
		buildConfigStructureAux(this._XMLStructureDoc.getDocumentElement().getChildNodes(), register, "");
		
		return register;
	}
	
	   private Element transferDataToNewNodeAssigned(Hashtable<String,List<Node>> contents, Element el){
	        
	        Element register = this._OutputXMLDoc.createElement(this._XMLStructureDoc.getDocumentElement().getNodeName());
	        
	        NamedNodeMap attrs = el.getAttributes();
	        Node n;
	        for (int j=0; j<attrs.getLength(); j++) {
	            n = attrs.item(j);
	            register.setAttribute(n.getNodeName(), n.getTextContent());
	        }
	        
	        buildConfigStructureAuxAssigned(this._XMLStructureDoc.getDocumentElement().getChildNodes(), register);
	        
	        return register;
	    }
	
	public String buildConfigStructureAux(NodeList nl, Element element, String path){
	    	
	    String path_aux = path;
		String nodeName;
		Node node;
		NodeList cn;
		int cnSize;
		List<Node> lst;
	    int i = 0;	
	    while(nl.getLength() > i){
	    		
	        path=path_aux;
	    	node = nl.item(i);
	    	cn = node.getChildNodes();
	    	cnSize = cn.getLength();
	    	
	    	if(node.getNodeType() == Node.ELEMENT_NODE){
	    			
	    		nodeName = node.getNodeName();
	    		
	    		if(cnSize == 0){
	    			
	    		    List<String> nodeNamePaths = _paths.get(nodeName);
	    		    
	    			//testar se a folha ja existe noutro caminho
	    			if(_paths.containsKey(nodeName) && nodeNamePaths.size() > 1){
	    			    nodeName = changeNodeName(nodeNamePaths, path, nodeName);
	    			}
	    			
                    if(!_leafs.containsKey(nodeName)){
                        i++;
                        continue;
                    }
                        
                    lst = _leafs.get(nodeName);
                    
                    //System.out.println("Depois: " + nodeName);
	    			
                    Node n;
	    			for(int k = 0; lst.size() > k ; k++){
	    				Element el = this._OutputXMLDoc.createElement(nodeName);
	    				n = lst.get(k);
	    				el.appendChild(this._OutputXMLDoc.createTextNode(/*_sm.preProcessString(*/n.getTextContent()/*)*/));
	    							
	    				NamedNodeMap attrs = n.getAttributes();
	    				Node attr;
	    				for (int j=0; j<attrs.getLength(); j++) {
	    				    attr = attrs.item(j);
	    					el.setAttribute(attr.getNodeName(),/* _sm.preProcessString(*/attr.getTextContent()/*)*/);
	    				}
	    				
	    				element.appendChild(el);
	    			}

	    		}
	    			
	    		if(cnSize > 0){		
	    			
		    			Element el = this._OutputXMLDoc.createElement(nodeName);
			    		
			    		/*NamedNodeMap attrs = getGroupNodeAttributes(nodeName, _XMLStructureDoc.getDocumentElement());
			    		Node attr;
		    			for (int k=0; k<attrs.getLength(); k++) {
		    			    attr = attrs.item(k);
		    				el.setAttribute(attr.getNodeName(), attr.getTextContent());
		    			}*/
		    			
		    			element.appendChild(el);
	
		    			path = buildConfigStructureAux(cn, el, path + nodeName + "\\");
	    		}
	    			
	    	}
	    			
	    	i++;
	    }

	    return path;
	}
	
	public void buildConfigStructureAuxAssigned(NodeList nl, Element element){
        
        String nodeName;
        Node node;
        NodeList cn;
        int cnSize;
        List<Node> lst;
        int i = 0;  
        while(nl.getLength() > i){
                
            node = nl.item(i);
            cn = node.getChildNodes();
            cnSize = cn.getLength();
            
            if(node.getNodeType() == Node.ELEMENT_NODE){
                    
                nodeName = node.getNodeName();
                
                if(cnSize <= 1){//System.out.println("No do ficheiro de conf: " + nodeName);
                    
                    if(!_leafs.containsKey(nodeName)){//System.out.println(_leafs);
                        i++;
                        continue;
                    }
                        
                    lst = _leafs.get(nodeName);
                    
                    Node n;
                    for(int k = 0; lst.size() > k ; k++){
                        Element el = this._OutputXMLDoc.createElement(nodeName);
                        n = lst.get(k);
                        el.appendChild(this._OutputXMLDoc.createTextNode(n.getTextContent()));
                                    
                        NamedNodeMap attrs = n.getAttributes();
                        Node attr;
                        for (int j=0; j<attrs.getLength(); j++) {
                            attr = attrs.item(j);
                            el.setAttribute(attr.getNodeName(), attr.getTextContent());
                        }
                        
                        element.appendChild(el);
                    }

                }
                    
                if(cnSize > 0){     
                    
                        Element el = this._OutputXMLDoc.createElement(nodeName);
                        
                        /*NamedNodeMap attrs = getGroupNodeAttributes(nodeName, _XMLStructureDoc.getDocumentElement());
                        Node attr;
                        for (int k=0; k<attrs.getLength(); k++) {
                            attr = attrs.item(k);
                            el.setAttribute(attr.getNodeName(), attr.getTextContent());
                        }*/
                        
                        element.appendChild(el);
    
                        buildConfigStructureAuxAssigned(cn, el);                 
                        
                }
                    
            }
                    
            i++;
        }

    }
	
	private String changeNodeName(List<String> elementPaths, String path, String nodeName){

	    int index = 0;
	    
	    for(int i = 0; elementPaths.size() > i ; i++){
	        if(elementPaths.get(i).equals(path)){
	            index = i;
	            break;
	        }
	    }
	    
	    if(index == 0){
	        return nodeName;
	    }
	    else{
	        index+=1;
	        return nodeName + index;
	    }
	}
	
	   private String loadNodes(NodeList nl, String path){
	       
	        String path_aux = path;
	        String nodeName;
	        Node node;
	        NodeList cn;
	        int cnSize;
	            
	        int i = 0;
	        while(nl.getLength() > i){
	                
	            path = path_aux;
	            
	            node = nl.item(i);
	            
	            if(node.getNodeType() == Node.ELEMENT_NODE){
	                
                    cn = node.getChildNodes();
                    cnSize = cn.getLength();
                    nodeName = node.getNodeName();
	                if(cnSize <= 1){
	                    
	                    List<String> nodeNamePaths = _paths.get(nodeName);
	                    
	                    if(_paths.containsKey(nodeName) && nodeNamePaths.size() > 1){
                            nodeName = changeNodeName(nodeNamePaths, path, nodeName);
	                    }
	                    
                        if(!_leafs.containsKey(nodeName)){
                            List<Node> lst = new ArrayList<Node>();
                            lst.add(node);
                            _leafs.put(nodeName, lst);
                        }
                        else{
                            _leafs.get(nodeName).add(node);
                        }
	                }
	                if(cnSize > 1){
	                    path = loadNodes(node.getChildNodes(), path + nodeName + "\\");
	                }
	                    
	            }
	                    
	            i++;
	        }
	        return path;
	    }
	   
	   private void loadNodesAssigned(NodeList nl){
           
           String nodeName;
           Node node;
           NodeList cn;
           int cnSize;
           List<Node> lst;    
           int i = 0;
           while(nl.getLength() > i){
     
               node = nl.item(i);
               
               if(node.getNodeType() == Node.ELEMENT_NODE){
                   
                   cn = node.getChildNodes();
                   cnSize = cn.getLength();
                   nodeName = node.getNodeName(); //System.out.println("Objectnode: "+nodeName);
                   //System.out.println("cnSize: "+cnSize);
                   if(cnSize == 1 && !cn.item(0).hasChildNodes()){
                       
                       if(!_leafs.containsKey(nodeName)){
                           lst = new ArrayList<Node>();
                           lst.add(node);
                           _leafs.put(nodeName, lst); //System.out.println("put _leafs: "+nodeName);
                       }
                       else{
                           _leafs.get(nodeName).add(node);
                       }
                   }
                   else{
                       loadNodesAssigned(node.getChildNodes());
                   }
                       
               }
                       
               i++;
           }
       }
	
	public Node getObject(){
		return _newObject;
	}
	
	public void printNodeToFile(Node n, String filePath) throws Exception{
		
		_OutputXMLDoc.appendChild(n);
		
		File f = new File(filePath);
		if(f.exists())
			f.delete();
		f.createNewFile();
		
    	Transformer transformer = TransformerFactory.newInstance().newTransformer();
        
        transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
        
        transformer.transform(new DOMSource(_OutputXMLDoc), new StreamResult(new FileOutputStream(filePath)));
	}

}