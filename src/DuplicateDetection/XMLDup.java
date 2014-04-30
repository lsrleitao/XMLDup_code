package DuplicateDetection;

import SaxToDom.SaxToDom;
import java.util.*;
import java.io.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException; /*
                                                        * import
                                                        * javax.xml.transform.OutputKeys;
                                                        * import
                                                        * javax.xml.transform.Transformer;
                                                        * importjavax.xml.transform.
                                                        * TransformerFactory; import
                                                        * javax.
                                                        * xml.transform.dom.DOMSource;
                                                        * import
                                                        * javax.xml.transform.stream
                                                        * .StreamResult;
                                                        */

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import CandidateDefinition.Candidates;
import CandidateDefinition.Canopy;
import CandidateDefinition.NoBlocking;
import CandidateDefinition.SNM;
import CandidateDefinition.SNMII;
import CandidateDefinition.SuffixArrays;
import RDB.DataBase;
import RDB.XMLToTable;

import java.lang.instrument.Instrumentation;

/**
 * @author Lu�s Leit�o
 */
public class XMLDup {
    // Global value so it can be ref'd by the tree-adapter
    Document document;
    String _resultsFolder = "./XMLDup Data/Results/";
    Document _configFile;
    Map<String,Double> _scores;
    
    /*
     * static String configGraph1 =
     * "./XMLDup Data/ConfigGraphs/PrecisionRecallSimilarityVsNrPairs.plt";
     * static String configGraph2 =
     * "./XMLDup Data/ConfigGraphs/PrecisonVsRecall.plt"; static String
     * configGraph3 =
     * "./XMLDup Data/ConfigGraphs/PrecisionRecallVsSimilarity.plt"; static
     * String configGraph4 =
     * "./XMLDup Data/ConfigGraphs/PrecisionRecallSimilarityVsPercPairs.plt";
     */
    String _bdFile;
    static String _resultsPath;
    float _threshold;
    FileHandler FH = new FileHandler();
    long _dbSize;

    static long _duplicatesTotal;
    static long _duplicatesFound;
    
    long _candidates;
    long _pairsTotal;
    
    ConfigGeneral _cg;

    public XMLDup() {
    }

    public XMLDup(Document configFile, String bdFile, ConfigGeneral cg) throws ParserConfigurationException,
            IOException,
            SAXException {

        _configFile = configFile;
        _bdFile = bdFile;

        _duplicatesTotal = 0;
        _duplicatesFound = 0;
        
        _cg = cg;
        
    }
    
    public Map<String,Double> getScores(){
        return _scores;
    }

    public void processBDSax(int windowSize, ConfigGeneral cg, String DBPath) throws SAXException,
            IOException {

        System.out.println("Parser: SAX");

        _threshold = cg.getThreshold();
        String objName = cg.getDbObjectName();

        System.out.print("Starting XML reader...");
        SaxToDom std = new SaxToDom(_bdFile, objName);
        System.out.println("FINISHED!");

        //DataBase db = DataBase.getSingletonObject(true);

        //XMLToTable xmltt = new XMLToTable(DBPath, objName);
        //xmltt.loadXMLToTable();
        
        //usar apenas para procura da melhor estrutura.
        DataBase.getSingletonObject(false).cleanDuplicatesTable();

        StructureSort ss = new StructureSort();
        cg.setConfigStructure(ss.sortStructure(cg.getConfigStructure()));

        _dbSize = std.countObj();

        System.out.println("Elements in DB : " + _dbSize);

        Compare comp = new Compare();

        int numComp = 0;

        double DdbSize = _dbSize;
        double DwSize = windowSize;
        double blockNum = 0;

        if (Math.round(DdbSize / DwSize) < DdbSize / DwSize)
            blockNum = Math.round(DdbSize / DwSize) + 1;
        else
            blockNum = Math.round(DdbSize / DwSize);

        System.out.println("Blocks: " + Math.round(blockNum));
        int block = -1;

        int wSize = windowSize;

        int window1 = 0;// janela em que est� a iteracao (1� grupo de blocos)
        int window2 = 0;// janela em que est� a iteracao (2� grupo de blocos)

        int object1_Index;// indice do objecto1 do par que est� a ser comparado.
        int object2_Index;// indice do objecto2 do par que est� a ser comparado.

        document = std.makeDom(window1 * wSize, window1 * wSize + wSize - 1);
        List<Node> l1 = std.convertDocToNodeList(document);

        document = std.makeDom(window2 * wSize, window2 * wSize + wSize - 1);
        List<Node> l2 = std.convertDocToNodeList(document);

        while (blockNum > window1) {
            // System.out.println("inicio do ciclo w1 " + window1 + " w2 " +
            // window2);

            if (window1 != block) {
                block = window1;
                System.out
                        .println("BLOCK " + new Integer(window1 + 1) + "/" + Math.round(blockNum));
            }

            int l1Size = l1.size();
            int l2Size = l2.size();
            for (int k = 0; l1Size > k; k++) {

                for (int l = 0; l2Size > l; l++) {

                    if (k >= l && window1 == window2) {
                        continue;
                    } else {
                        object1_Index = window1 * windowSize + k;
                        object2_Index = window2 * windowSize + l;
                        comp.compareNodes(object1_Index, object2_Index, cg, l1.get(k), l2.get(l));
                        numComp++;
                    }
                }
            }

            if (blockNum - 1 == window1)
                break;

            if (window2 == blockNum - 1) {

                window1++;// System.out.println("troca de bloco1 " + window1);
                document = std.makeDom(window1 * wSize, window1 * wSize + wSize - 1);
                l1 = std.convertDocToNodeList(document);
                l2 = l1;
                window2 = window1;// System.out.println("troca de bloco2 " +
                // window2);

            } else {

                window2++;// System.out.println("troca de bloco2 " + window2);
                document = std.makeDom(window2 * wSize, window2 * wSize + wSize - 1);
                l2 = std.convertDocToNodeList(document);

            }

        }

        System.out.print("Printing results to file...");
        // escrever o ficheiro
        BufferedWriter outRP = new BufferedWriter(new FileWriter(_resultsPath
                + "/results_pairs.txt", true));
        FH.writeSortedFile(comp.getDup(), outRP, cg.getStorageType());
        System.out.println("FINISHED!");

        System.out.println("Pairs Compared: " + numComp);

        outRP.close();

        //db.closeConnection();
    }

    /**
     * Processes the database.
     * 
     * @param node The root node of the database
     */
    public void processBDDom(ConfigGeneral cg, String DBPath, Document dbDocument) throws SAXException,
    IOException, ParserConfigurationException {

        try {

            System.out.println("Parser: DOM");

            document = dbDocument;

            Node node = document.getDocumentElement();

            _threshold = cg.getThreshold();

            //DataBase db = DataBase.getSingletonObject(true);

            Compare comp = new Compare();

            String objName = cg.getDbObjectName();

            //usar apenas para procura da melhor estrutura.
            //DataBase.getSingletonObject(false).cleanDuplicatesTable();
            //DataBase.getSingletonObject(false).cleanBlockingKeysTable();

            // carrega os dados para uma tabela
            //XMLToTable xmltt = new XMLToTable(dbDocument, objName);
            //xmltt.loadXMLDocToTable();

            // ordena a estrutura dos dados que vai ser aplicada
            StructureSort ss = new StructureSort();
            cg.setConfigStructure(ss.sortStructure(cg.getConfigStructure()));

            Element nd = (Element) node;
            NodeList nl = nd.getElementsByTagName(objName);

            _dbSize = nl.getLength();

            int num_comp = 0; 

            System.out.println("Elements in DB: " + _dbSize);
            
            Candidates cnd = null;
            if(cg.useBlocking()){
                if(cg.getBlockingAlgo().equals("SuffixA")){
                    cnd = new SuffixArrays((int)cg.getBlockingParams()[0],(int)cg.getBlockingParams()[1],(int)cg.getBlockingParams()[2], cg.getBlockingParams()[3]); //SuffixSize(lms), MaxBlockSize(lbs), keyRuns, simthreshold 
                }
                else if(cg.getBlockingAlgo().equals("SNM")){
                    cnd = new SNM((int)cg.getBlockingParams()[0], (int)cg.getBlockingParams()[1]); // Window,keyRuns
                }
                else if(cg.getBlockingAlgo().equals("SNMII")){
                    cnd = new SNMII((int)cg.getBlockingParams()[0], (int)cg.getBlockingParams()[1]); // Window,keyRuns
                }
                else if(cg.getBlockingAlgo().equals("Canopy")){
                    cnd = new Canopy(cg.getBlockingParams()[0], cg.getBlockingParams()[1], (int)cg.getBlockingParams()[2]); // T1,T2,gramSize - T1 > T2
                }
                
                cnd.buildKeys(cg.getkey());


                System.out.print("Obtaining Candidates...");
                Map<Integer,Set<Integer>> cndPairs = cnd.getCandidates();
                //System.out.println(cndPairs);
                System.out.println("FINISH!"); 
           
                int hashSize = cndPairs.size();           

                DataBase db = DataBase.getSingletonObject(false);
                Set<String> dups = db.getDuplicatePairs(cg.getkey()[0].split("#")[0]);
                //System.out.println(dups);
                //System.out.println(dups.size());
                
                int b = 1;
                for(Map.Entry<Integer, Set<Integer>> e : cndPairs.entrySet()){
                    System.out.println("Block " + b + "/" + hashSize);
                    int index1 = e.getKey();
                    Enumeration<Integer> e2 = Collections.enumeration(e.getValue());
                    for(int i = 0; e.getValue().size() > i; i++){
                    //while(e2.hasMoreElements()){
                        int index2 = e2.nextElement();
                        num_comp++;
                        //System.out.println("Pair: " + index1 + "," + index2);
                        
                        if(cndPairs.containsKey(index1) && cndPairs.get(index1).contains(index2) && 
                                cndPairs.containsKey(index2) && cndPairs.get(index2).contains(index1) ){
                          System.out.println("Pair: " + index1 + "," + index2 + " REPETIDO");
                          System.exit(0);
                        }
                        
                        //Remove os duplicados que são avaliados. Apenas ficam em "dups" aqueles que são verdadeiros duplicados mas não foram comparados
                        if(index1 > index2){
                            dups.remove(new String(index2 + "#" + index1));
                        }
                        else{
                            dups.remove(new String(index1 + "#" + index2));
                        }
                        
                        comp.compareNodes(index1, index2, cg, nl.item(index1), nl.item(index2));
                    }
                    b++;
                }
                
                System.out.println(dups);
                System.out.println(dups.size());
                
                //compara as chaves vazias
                Set<Integer> emptyKeys = cnd.getEmptyKeys();
                System.out.println("Comparing Objects for " + emptyKeys.size() + " Empty Keys");
                Enumeration<Integer> ek = Collections.enumeration(emptyKeys);
                
                b = 1;
                for(int i = 0; i < _dbSize && emptyKeys != null; i++){
                    System.out.println("Block " + b + "/" + _dbSize);
                    ek = Collections.enumeration(emptyKeys);
                    //if(emptyKeys.contains(i)){
                    //    continue;
                    //}
                    for(int j = 0; emptyKeys.size() > j; j++){
                    //while(ek.hasMoreElements()){
                        int key = ek.nextElement();
                        if(emptyKeys.contains(i) && emptyKeys.contains(key) && i > key){
                            continue;
                        } 
                        else if(i != key){
                            num_comp++;
                            //System.out.println("Pair: " + i + "," + key);
                            comp.compareNodes(i,key, cg, nl.item(i),nl.item(key));
                        }
                        
                    }
                    b++;
                }

            }
            else{
                //cnd = new NoBlocking(_dbSize);
                
                for(int i = 0; i < _dbSize; i++){
                    System.out.println("ELEMENT " + i);
                    for(int j = i+1; j < _dbSize; j++){
                        num_comp++;
                        comp.compareNodes(i,j, cg, nl.item(i),nl.item(j));
                    }
                }
            }

            _candidates =  num_comp;
            _pairsTotal = (_dbSize*(_dbSize-1))/2;

            System.out.println("Sorting Pairs...");

            SortingAlgos sa = new SortingAlgos(comp.getDup());
            // sa.quicksort(0,comp.getDup().size()-1);
            // sa.insertionSort();
            sa.heapSort(comp.getDup());

            System.out.print("Printing results to file...");
            // escrever o ficheiro
            BufferedWriter outRP = new BufferedWriter(new FileWriter(_resultsPath
                    + "/results_pairs.txt", true));
            FH.writeSortedFile(comp.getDup(), outRP, cg.getStorageType()); // comentar
            // se
            // ja
            // se
            // possui
            // o
            // ficheiro
            // com
            // a
            // totalidade
            // das
            // similaridades
            // dos
            // pares
            // de
            // objectos
            System.out.println("FINISHED!");

            System.out.println("Pairs Compared: " + num_comp);

            outRP.close();
            //db.closeConnection();

        } catch (IOException e) {
            System.err.println("Erro no processa BD!");
            // System.exit(-1);
        }

    }

    // funcao de teste
    public List<String> testObjectStructure(NodeList nl, List<String> lst) {

        Node n;
        int i = 0;
        String str_aux;
        while (i < nl.getLength()) {
            n = nl.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE && n != null) {
                str_aux = n.getNodeName();
                if (!lst.contains(str_aux))
                    lst.add(str_aux);
                lst = testObjectStructure(n.getChildNodes(), lst);
            }
            i++;

        }

        return lst;
    }

    public void run() {

        System.out.println("Process started at " + Calendar.getInstance().getTime());
        System.out.println("OS: " + System.getProperty("os.name"));

        try {

            // factory.setValidating(true);
            // factory.setNamespaceAware(true);

            // definir e criar, com as repectivas BDs, a pasta onde estas ser
            // colocadas

            File f = new File(_bdFile);
            
            String bdFileName = "";
            // String fConfigGraphSaida = "";

            TimeProfiler tp = new TimeProfiler();
            tp.start();

            bdFileName = f.getName().replaceAll(".xml", "");
            // definir e criar a pasta onde guardar os resultados
            String rsdir = _resultsFolder + bdFileName;
            this._resultsPath = rsdir;

            File f3 = new File(rsdir);
            f3.mkdir();
            FH.deleteFolderContent(f3);
            FH.docToFile(_configFile, rsdir + "/config.xml");

            System.out.print("Loading configuration file...");

            System.out.println("FINISHED!");
            if (_cg.getStorageType() == 1){
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = factory.newDocumentBuilder();
                Document dbDoc = builder.parse(new File(_bdFile));
                
                processBDDom(_cg, _bdFile, dbDoc);
            }
            else{
                processBDSax(100, _cg, _bdFile);
            }

            _scores = FH.printPrecisionRecall(_threshold, _resultsPath,
                                                    _duplicatesTotal,_duplicatesFound, _pairsTotal, _candidates);
            

                // GNUPLOT
                /*
                 * fConfigGraphSaida =
                 * FH.escreveFicheiroConfigGraficoAlterado(configGraph1,
                 * bdFileName, "PRSvsNP", threshold, resultsPath);
                 * Runtime.getRuntime().exec("pgnuplot " + "\"" + new
                 * File(fConfigGraphSaida).getCanonicalPath() + "\"");
                 * 
                 * fConfigGraphSaida =
                 * FH.escreveFicheiroConfigGraficoAlterado(configGraph2,
                 * bdFileName, "PvsR", threshold, resultsPath);
                 * Runtime.getRuntime().exec("pgnuplot " + "\"" + new
                 * File(fConfigGraphSaida).getCanonicalPath() + "\"");
                 * 
                 * fConfigGraphSaida =
                 * FH.escreveFicheiroConfigGraficoAlterado(configGraph3,
                 * bdFileName, "PRvsS", threshold, resultsPath);
                 * Runtime.getRuntime().exec("pgnuplot " + "\"" + new
                 * File(fConfigGraphSaida).getCanonicalPath() + "\"");
                 * 
                 * fConfigGraphSaida =
                 * FH.escreveFicheiroConfigGraficoAlterado(configGraph4,
                 * bdFileName, "PRSvsPP", threshold, resultsPath);
                 * Runtime.getRuntime().exec("pgnuplot " + "\"" + new
                 * File(fConfigGraphSaida).getCanonicalPath() + "\"");
                 */

            Gnuplot gp = new Gnuplot("PRSvsNP", bdFileName, _threshold, _resultsPath, _dbSize,
                    _duplicatesFound);
            gp.buildGraphConfigurationFile();
            gp.writeGraph();

            gp = new Gnuplot("PvsR", bdFileName, _threshold, _resultsPath, _dbSize,
                    _duplicatesFound);
            gp.buildGraphConfigurationFile();
            gp.writeGraph();

            gp = new Gnuplot("PRvsS", bdFileName, _threshold, _resultsPath, _dbSize,
                    _duplicatesFound);
            gp.buildGraphConfigurationFile();
            gp.writeGraph();

            gp = new Gnuplot("PRSvsPP", bdFileName, _threshold, _resultsPath, _dbSize,
                    _duplicatesFound);
            gp.buildGraphConfigurationFile();
            gp.writeGraph();

            tp.stop();
            tp.writeTimeToFile(_resultsPath);
            
            _scores.put("Time",(double) tp.getCpuTime());//index 5
            _scores.put("Comparisons",(double) Singleton.getSingletonObject().getComparisons());//index 6

            FH.writeComparisonsToFile(Singleton.getSingletonObject().getComparisons(),
                    _resultsPath);
            System.out.println("String Comparisons: "
                    + Singleton.getSingletonObject().getComparisons());
            
            Singleton.getSingletonObject().resetComparisons();

            System.out.println("Process ended at " + Calendar.getInstance().getTime());

                // A utiliza��o do Transformer (javax.xml.transform) permite
                // grande flexibilidade na leitura e escrita
                // de XML. Neste caso, especifica-se a indenta��o autom�tica e a
                // utiliza��o de um encoding adequado
                // atrav�s de op��es de sa�da.

                // para ficheiros grandes atrasa o programa
                /*
                 * Transformer transformer =
                 * TransformerFactory.newInstance().newTransformer();
                 * transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                 * transformer.setOutputProperty(OutputKeys.ENCODING,
                 * "ISO-8859-1"); transformer.transform(new DOMSource(document),
                 * new StreamResult(new FileOutputStream(bdFile)));
                 */

        } catch (SAXParseException spe) {
            // Error generated by the parser
            System.out.println("\n** Parsing error" + ", line " + spe.getLineNumber() + ", uri "
                    + spe.getSystemId());
            System.out.println("   " + spe.getMessage());
            // Use the contained exception, if any
            Exception x = spe;
            if (spe.getException() != null)
                x = spe.getException();
            x.printStackTrace();
        } catch (SAXException sxe) {
            // Error generated during parsing)
            // O DOM utiliza o SAX para efectuar a interpreta��o (parse) ao
            // documento XML.
            // O tratamento de eventos definido pelo DOM efectua a constru��o da
            // �rvore.
            Exception x = sxe;
            if (sxe.getException() != null)
                x = sxe.getException();
            x.printStackTrace();
        } catch (ParserConfigurationException pce) {
            // Parser with specified options can't be built
            pce.printStackTrace();
        } catch (IOException ioe) {
            // I/O error
            ioe.printStackTrace();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
    
    public void run(Document db) {

        System.out.println("Process started at " + Calendar.getInstance().getTime());
        System.out.println("OS: " + System.getProperty("os.name"));
        
        try {

            // factory.setValidating(true);
            // factory.setNamespaceAware(true);

            // definir e criar, com as repectivas BDs, a pasta onde estas ser
            // colocadas

            File f = new File(_bdFile);
            
            String bdFileName = "";
            // String fConfigGraphSaida = "";

            TimeProfiler tp = new TimeProfiler();
            tp.start();

            bdFileName = f.getName().replaceAll(".xml", "");
            // definir e criar a pasta onde guardar os resultados
            String rsdir = _resultsFolder + bdFileName;
            this._resultsPath = rsdir;

            File f3 = new File(rsdir);
            f3.mkdir();
            FH.deleteFolderContent(f3);
            FH.docToFile(_configFile, rsdir + "/config.xml");

            System.out.print("Loading configuration file...");
            ConfigGeneral cg = new ConfigGeneral(_configFile);
            System.out.println("FINISHED!");
            
            processBDDom(cg, _bdFile, db);


            _scores = FH.printPrecisionRecall(_threshold, _resultsPath, _duplicatesTotal,
                        _duplicatesFound, _pairsTotal, _candidates);
            

                // GNUPLOT
                /*
                 * fConfigGraphSaida =
                 * FH.escreveFicheiroConfigGraficoAlterado(configGraph1,
                 * bdFileName, "PRSvsNP", threshold, resultsPath);
                 * Runtime.getRuntime().exec("pgnuplot " + "\"" + new
                 * File(fConfigGraphSaida).getCanonicalPath() + "\"");
                 * 
                 * fConfigGraphSaida =
                 * FH.escreveFicheiroConfigGraficoAlterado(configGraph2,
                 * bdFileName, "PvsR", threshold, resultsPath);
                 * Runtime.getRuntime().exec("pgnuplot " + "\"" + new
                 * File(fConfigGraphSaida).getCanonicalPath() + "\"");
                 * 
                 * fConfigGraphSaida =
                 * FH.escreveFicheiroConfigGraficoAlterado(configGraph3,
                 * bdFileName, "PRvsS", threshold, resultsPath);
                 * Runtime.getRuntime().exec("pgnuplot " + "\"" + new
                 * File(fConfigGraphSaida).getCanonicalPath() + "\"");
                 * 
                 * fConfigGraphSaida =
                 * FH.escreveFicheiroConfigGraficoAlterado(configGraph4,
                 * bdFileName, "PRSvsPP", threshold, resultsPath);
                 * Runtime.getRuntime().exec("pgnuplot " + "\"" + new
                 * File(fConfigGraphSaida).getCanonicalPath() + "\"");
                 */

            Gnuplot gp = new Gnuplot("PRSvsNP", bdFileName, _threshold, _resultsPath, _dbSize,
                    _duplicatesFound);
            gp.buildGraphConfigurationFile();
            gp.writeGraph();

            gp = new Gnuplot("PvsR", bdFileName, _threshold, _resultsPath, _dbSize,
                    _duplicatesFound);
            gp.buildGraphConfigurationFile();
            gp.writeGraph();

            gp = new Gnuplot("PRvsS", bdFileName, _threshold, _resultsPath, _dbSize,
                    _duplicatesFound);
            gp.buildGraphConfigurationFile();
            gp.writeGraph();

            gp = new Gnuplot("PRSvsPP", bdFileName, _threshold, _resultsPath, _dbSize,
                    _duplicatesFound);
            gp.buildGraphConfigurationFile();
            gp.writeGraph();

            tp.stop();
            tp.writeTimeToFile(_resultsPath);
            
            _scores.put("Time",(double) tp.getCpuTime());
            _scores.put("Comparisons",(double) Singleton.getSingletonObject().getComparisons());

            FH.writeComparisonsToFile(Singleton.getSingletonObject().getComparisons(),
                    _resultsPath);
            System.out.println("String Comparisons: "
                    + Singleton.getSingletonObject().getComparisons());
            
            Singleton.getSingletonObject().resetComparisons();

            System.out.println("Process ended at " + Calendar.getInstance().getTime());

                // A utiliza��o do Transformer (javax.xml.transform) permite
                // grande flexibilidade na leitura e escrita
                // de XML. Neste caso, especifica-se a indenta��o autom�tica e a
                // utiliza��o de um encoding adequado
                // atrav�s de op��es de sa�da.

                // para ficheiros grandes atrasa o programa
                /*
                 * Transformer transformer =
                 * TransformerFactory.newInstance().newTransformer();
                 * transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                 * transformer.setOutputProperty(OutputKeys.ENCODING,
                 * "ISO-8859-1"); transformer.transform(new DOMSource(document),
                 * new StreamResult(new FileOutputStream(bdFile)));
                 */
            
        } catch (SAXParseException spe) {
            // Error generated by the parser
            System.out.println("\n** Parsing error" + ", line " + spe.getLineNumber() + ", uri "
                    + spe.getSystemId());
            System.out.println("   " + spe.getMessage());
            // Use the contained exception, if any
            Exception x = spe;
            if (spe.getException() != null)
                x = spe.getException();
            x.printStackTrace();
        } catch (SAXException sxe) {
            // Error generated during parsing)
            // O DOM utiliza o SAX para efectuar a interpreta��o (parse) ao
            // documento XML.
            // O tratamento de eventos definido pelo DOM efectua a constru��o da
            // �rvore.
            Exception x = sxe;
            if (sxe.getException() != null)
                x = sxe.getException();
            x.printStackTrace();
        } catch (ParserConfigurationException pce) {
            // Parser with specified options can't be built
            pce.printStackTrace();
        } catch (IOException ioe) {
            // I/O error
            ioe.printStackTrace();
        } catch (Throwable t) {
            t.printStackTrace();
        }
        
    }
    
}
