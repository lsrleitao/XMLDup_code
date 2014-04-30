package RDB;

import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import SaxToDom.SaxToDom;

public class XMLToTable {

    private String _XMLDataset;
    private Document _XMLDatasetDoc;
    private String _objectName;
    private int windowSize = 1000;
    private DataBase _db;
    private Set<String> _attributes;

    public XMLToTable(String XMLDataset, String objectName, DataBase rdb, Set<String> attributes) {
        this._XMLDataset = XMLDataset;
        this._objectName = objectName;
        this._db = rdb;
        this._attributes = attributes;
    }
    
    public XMLToTable(Document XMLDataset, String objectName, DataBase rdb, Set<String> attributes) {
        this._XMLDatasetDoc = XMLDataset;
        this._objectName = objectName;
        this._db = rdb;
        this._attributes = attributes;
    }

    public void loadXMLToTable() {

        try {

            /*
             * DocumentBuilderFactory factory =
             * DocumentBuilderFactory.newInstance(); DocumentBuilder builder =
             * factory.newDocumentBuilder(); Document document =
             * builder.parse(new File(XMLDataset));
             * 
             * 
             * XMLImport xmli = new XMLImport(objectName, db);
             * xmli.setDocument(document); xmli.loadDatabase();
             */

            SaxToDom std = new SaxToDom(_XMLDataset, _objectName);

            Document document;

            XMLImport xmli = new XMLImport(_objectName, _db, _attributes);

            long _dbSize = std.countObj();
            // System.out.println("DB size: " + _dbSize);

            int block = 1;
            int object = 0;
            System.out.print("Loading XML to Table...");
            for (int i = 0; object < _dbSize; i = i + windowSize) {
                document = std.makeDom(i, i + windowSize - 1);
                xmli.setDocument(document);
                xmli.loadDatabase(windowSize, block);
                object = i + windowSize - 1;
                block++;
            }

            System.out.println("FINISHED!");

        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
    
    public void loadXMLDocToTable() {

        try {

            XMLImport xmli = new XMLImport(_objectName, _db, _attributes);

            System.out.print("Loading XML to Table...");
            xmli.setDocument(_XMLDatasetDoc);
            xmli.loadDatabaseFromDoc();

            System.out.println("FINISHED!");

        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

}
