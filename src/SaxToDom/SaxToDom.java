package SaxToDom;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.*;
import org.xml.sax.helpers.XMLReaderFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class SaxToDom {
    private String _sourcePath;
    private String _objectName;
    private long _dbSize = 0;

    private XMLReader parser;

    public SaxToDom(String sourcePath, String objName) throws SAXException {
        _sourcePath = sourcePath;
        _objectName = objName;
        parser = XMLReaderFactory.createXMLReader();
    }

    public Document makeDom(long i, long j) {

        Document doc = null;

        try {

            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            DOMImplementation impl = builder.getDOMImplementation();

            // Create the document
            doc = impl.createDocument(null, null, null);

            ContentHandler handler = new SaxToDomHandler(doc, _objectName, i, j);
            parser.setContentHandler(handler);

            parser.parse(_sourcePath);

        } catch (DOMException e) {
            System.err.println(e);
            e.printStackTrace();
            System.exit(0);
        } catch (ParserConfigurationException e) {
            System.err.println(e);
        } catch (SAXException e) {
            if (!e.getMessage().equals("Document parsing stopped"))
                System.err.println(e);

        } catch (IOException e) {
            System.err.println(e);
        }
        return doc;
    }

    public long countObj() throws SAXException, IOException {

        if(_dbSize == 0){

            long [] vec = {0};

            ContentHandler handler = new CountObjHandler(vec, _objectName);
            parser.setContentHandler(handler);

            parser.parse(_sourcePath);

            return vec[0];
        } else {
            return _dbSize;
        }
    }

    public List<Node> convertDocToNodeList(Document doc) {

        List<Node> lst = new ArrayList<Node>();
        NodeList nl = doc.getElementsByTagName(_objectName);

        for (int i = 0; nl.getLength() > i; i++) {
            lst.add(nl.item(i));
        }

        return lst;
    }

    public void setDBSize(long size) {

        _dbSize = size;
    }

}
