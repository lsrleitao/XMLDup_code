package SaxToDom;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.Text;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

public class SaxToDomHandler
        implements ContentHandler {

    private long cnt_ini = 0;
    private long cnt_fin = 3;
    private long cnt = -1;
    private String objectName;
    private Document myDoc;
    private Node myCurrentNode;

    private boolean charFlag = false; // usado para quando existem caracteres
    // especiais o texto ficar todo no mesmo
    // n� texto
    private Node charNode; // usado para quando existem caracteres especiais o

    // texto ficar todo no mesmo n� texto

    public SaxToDomHandler(Document doc, String objName, long iniObj, long finObj) {
        myDoc = doc;
        myCurrentNode = myDoc;
        cnt_ini = iniObj;
        cnt_fin = finObj;
        objectName = objName;
    }

    public void characters(char[] text, int start, int length) throws SAXException {

        if (cnt >= cnt_ini && cnt <= cnt_fin || cnt == -1) {

            String str = new String(text, start, length);
            Text t = myDoc.createTextNode(str);

            if (charFlag)
                charNode.getLastChild().setTextContent(
                        charNode.getLastChild().getTextContent() + str);
            else
                myCurrentNode.appendChild(t);

        }

        // System.out.println("char1: " + new String(text, start, length));
        // System.out.println("char2: " + new String(text, start,
        // length).length());
        charFlag = true;
        charNode = myCurrentNode;

    }

    public void startElement(String namespaceURI,
                             String localName,
                             String qualifiedName,
                             Attributes atts) {

        if (localName.equals(objectName))
            cnt++;

        if (cnt >= cnt_ini && cnt <= cnt_fin || cnt == -1) {

            Element elem = myDoc.createElement(localName);

            for (int i = 0; i < atts.getLength(); ++i) {
                String qname = atts.getQName(i);
                String value = atts.getValue(i);
                Attr attr = myDoc.createAttribute(qname);
                attr.setValue(value);
                elem.setAttributeNodeNS(attr);

            }

            myCurrentNode.appendChild(elem);
            myCurrentNode = elem;

        }

        // System.out.println("se: " + localName);

    }

    public void endElement(String namespaceURI, String localName, String qualifiedName) throws SAXException {

        if (cnt >= cnt_ini && cnt <= cnt_fin) {

            myCurrentNode = myCurrentNode.getParentNode();

            if (localName.equals(objectName) && cnt == cnt_fin) {

                Text t = myDoc.createTextNode("");
                myCurrentNode.appendChild(t);

                throw new SAXException("Document parsing stopped");
            }
        }

        // System.out.println("ee: " + localName);
        charFlag = false;
    }

    // do-nothing methods
    public void processingInstruction(String target, String data) {
    }

    public void ignorableWhitespace(char[] text, int start, int length) {
    }

    public void setDocumentLocator(Locator locator) {
    }

    public void startDocument() {
    }

    public void endDocument() {
    }

    public void startPrefixMapping(String prefix, String uri) {
    }

    public void endPrefixMapping(String prefix) {
    }

    public void skippedEntity(String name) {
    }
} // end TextExtractor
