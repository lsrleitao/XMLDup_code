package SaxToDom;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;

public class CountObjHandler
        implements ContentHandler {

    private long[] objCnt;
    private String objectName;

    public CountObjHandler(long[] vec, String objName) {

        objCnt = vec;
        objectName = objName;
    }

    public void startElement(String namespaceURI,
                             String localName,
                             String qualifiedName,
                             Attributes atts) {

        if (localName.equals(objectName)) {
            objCnt[0]++;
        }

    }

    // do-nothing methods
    public void endElement(String namespaceURI, String localName, String qualifiedName) {
    }

    public void characters(char[] text, int start, int length) {
    }

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

