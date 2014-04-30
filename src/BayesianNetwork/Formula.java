package BayesianNetwork;

import java.util.List;
import DuplicateDetection.StringMatching;

/**
 * @author Lu�s Leit�o
 */
public class Formula {

    private StringMatching _sm = new StringMatching();
    private double _threshold;

    // define se sao usadas as heuristicas de corte para averiguar quando o par
    // ja nao pode atingir o threshold definido.
    private static final boolean _heuristics = true;
    // valor estabelecido para as probabilidades que não se conhecem
    private static double _maxVal = 1;//pruning factor    
    private static final boolean _uniquePF = true;//para usar os pfs do ficheiro de configuracao, colocar a false

    public Formula(double threshold, float uniquePF) {
        _threshold = threshold;
        _maxVal = uniquePF;
    }

    /**
     * Computes the bayesian network score
     * 
     * @param redeBayes The structure that represents the bayesian network
     * @param metodoRaiz The formula to be used in the root of the network
     * @return The similarity score produced by the bayesian network
     */
    public double getNetworkScore(ValueNode network) {
        
        ObjBayes networkPF;
        if(_uniquePF)
            networkPF = network;
        else           
            networkPF = setNetworkPruningFactors(network);
        
        double res = walkNetwork(networkPF, _threshold);
        return res;
    }

    private double formula(String formula, double[] scores) {
        if (formula.equals("AVG"))
            return average(scores);
        else if (formula.equals("OR"))
            return or(scores);
        else if (formula.equals("AND"))
            return and(scores);
        else if (formula.equals("MAX"))
            return max(scores);
        else
            return -1;
    }

    private double walkNetwork(ObjBayes node, double threshold) {
        
        double skipFormula;
        String formula = node.getFormula();
        double score;
        List<ObjBayes> l = node.getChildNodes();
        int size = l.size();
        
        double [] scores;
        double formulaScore = 1;
        
        //pruning factor unico definido em _maxVal
        if(_uniquePF){
            scores = fillMaxScore(size, _maxVal); //usar para pruning factors com o valor _maxVal definido para todos os atributos
            //formulaScore = _maxVal; //usar para pruning factors com o valor _maxVal definido para todos os atributos
        }
        //pruning factor definido para cada atributo no ficheiro de configuracao
        else{
            scores = fillMaxScorePF(l,size);//System.out.println("Scores antes");listScores(scores);
            //formulaScore = formula(formula, scores);
        }
        
        ObjBayes currentNode;
        
        for (int i = 0; size > i; i++) {
            currentNode = l.get(i);
            if (currentNode.isFinalNode()) {
                score = getLeafScore(currentNode);
                //System.out.println("leaf score: " + score);
            } else {
                double newThreshold = getNewThreshold(formula, threshold, scores, size, i);
                score = walkNetwork(currentNode, newThreshold);
                if (score == -1){
                    return -1;
                }
            }

            skipFormula = skipFunction(formula, score);

            if (skipFormula > -1) {
                formulaScore = skipFormula;
                break;
            }

            scores[i] = score; //System.out.println("Scores depois");listScores(scores);
            formulaScore = formula(formula, scores);//System.out.println("FS: " + formulaScore);
            //System.out.println("T: " + threshold);

            if (_heuristics && threshold > formulaScore) {
                return -1;
            }
            
        }//System.out.println("object score: " + formulaScore);
        return formulaScore;
    }
    
    private ObjBayes setNetworkPruningFactors(ObjBayes node) {

        String formula = node.getFormula();
        double formulaScore = 1;
        double score;
        List<ObjBayes> l = node.getChildNodes();
        int size = l.size();
        double [] scores = fillMaxScore(size, 0);
        ObjBayes currentNode;
        for (int i = 0; size > i; i++) {
            currentNode = l.get(i);
            if (currentNode.isFinalNode()) {
                score = getLeafScorePF(currentNode);
            } else {
                score = setNetworkPruningFactors(currentNode).getPruningFactor();
            }
            scores[i] = score;           
        }
        formulaScore = formula(formula, scores);        
        node.setPruningFactor(formulaScore);
        //System.out.println("PF_set = "+ formulaScore);
        
        
        return node;
    }
    
    //efectua uma query a um dado no para saber a probabilidade desse no ser duplicado considerando os pruning factors como probabilidades a priori
    private double queryPruningFactor(ObjBayes node) {

        String formula = node.getFormula();
        double formulaScore = 0;
        double score;
        List<ObjBayes> l = node.getChildNodes();
        int size = l.size();
        double [] scores = fillMaxScorePF(l, size);
        ObjBayes currentNode;
        // System.out.println(size);
        for (int i = 0; size > i; i++) {
            currentNode = l.get(i);
            if (currentNode.isFinalNode()) {
                score = getLeafScorePF(currentNode);
            } else {
                score = queryPruningFactor(currentNode);
            }
            scores[i] = score;
            formulaScore = formula(formula, scores);

        }//System.out.println("query pf = " + formulaScore);
        return formulaScore;
    }

    private double skipFunction(String formula, double score) {
        if (score == 1 && (formula.equals("OR") || formula.equals("MAX")))
            return 1;
        else if (score == 0 && formula.equals("AND"))
            return 0;

        return -1;
    }

    private double getNewThreshold(String formula,
                                   double oldThreshold,
                                   double[] scores,
                                   int size, int index) {
        if (formula.equals("AVG"))
            return averageThreshold(oldThreshold, scores, size, index);
        else if (formula.equals("OR"))
            return orThreshold(oldThreshold, scores, index);
        else if (formula.equals("AND"))
            return andThreshold(oldThreshold, scores, size, index);
        else if (formula.equals("MAX"))
            return maxThreshold();
        else
            return -1;
    }

    private double getLeafScore(ObjBayes leaf) {

        double score = -2;

        if (leaf instanceof ValueLeafNode) {
            ValueLeafNode vln = (ValueLeafNode) leaf;
            score = _sm.stringMatching(vln.getSimMeasure(), vln.getContent1(), vln.getContent2());
            if (score < vln.getSimCut())
                score = 0;
        } else /* if(leaf instanceof HiddenValueLeafNode) */{
            HiddenValueLeafNode hvln = (HiddenValueLeafNode) leaf;
            score = hvln.getDefaultProb();
            // As 2 linhas que se seguem estao comentadas para o corte de
            // similaridade nao actuar sobre a probabilidade de ausencia.
            /*
             * if(score < hvln.getSimCut()) score = 0;
             */
        }// System.out.println("score: "+score);
        return score;
    }
    
    //devolve o valor do pruning factor num dado no folha ou a probabilidade de ausencia se este for vazio
    private double getLeafScorePF(ObjBayes leaf) {

        double score = -2;

        if (leaf instanceof ValueLeafNode) {
            ValueLeafNode vln = (ValueLeafNode) leaf;//System.out.println("PF = " + vln.getPruningFactor());
            score = vln.getPruningFactor();
            /*if (score < vln.getSimCut())
                score = 0;*/
        } else /* if(leaf instanceof HiddenValueLeafNode) */{
            HiddenValueLeafNode hvln = (HiddenValueLeafNode) leaf;
            score = hvln.getDefaultProb();
            // As 2 linhas que se seguem estao comentadas para o corte de
            // similaridade nao actuar sobre a probabilidade de ausencia.
            /*
             * if(score < hvln.getSimCut()) score = 0;
             */
        }// System.out.println("score: "+score);
        return score;
    }

    private double average(double[] scores) {

        double res = 0;
        double size = scores.length;

        for (int i = 0; size > i; i++) {
            res += scores[i];
        }
        res = res / size;

        return res;
    }

    private double averageThreshold(double oldThreshold, double[] scores, double size, int index) {

        //System.out.println("oldThreshold: " + oldThreshold);
        
        double remainingSum = 0;
        
        for(int i = 0; scores.length > i; i++){//System.out.println("scores: "+ scores[i]);
            if(index != i){
                remainingSum = remainingSum + scores[i];
            }
        }
        
        //esta formula era aplicada quando era passado o score do nivel ja calculado
        //Tinha problemas com a precisao no arredondamento dos doubles
        //double res = (size * (oldThreshold - score)) + nodePF;
        
        double res = (size * oldThreshold) - remainingSum;//System.out.println("newthreshold: " + res);

        return res;
    }

    private double or(double[] scores) {

        double res = 1;
        double size = scores.length;
        double score;

        for (int i = 0; size > i; i++) {
            score = scores[i];
            if (score == 1d)
                return 1;
            else
                res = res * (1d - score);
        }

        return 1 - res;
    }

    private double orThreshold(double oldThreshold, double[] scores, int index) {

        double remainingProd = 1;
        
        for(int i = 0; scores.length > i; i++){
            if(index != i){
                remainingProd = remainingProd * (1d-scores[i]);
            }
        }
        double res = (oldThreshold - 1d + remainingProd) / remainingProd;
        
        return res;
        
        //return Double.NEGATIVE_INFINITY;
    }

    private double max(double[] scores) {

        double res = Double.NEGATIVE_INFINITY;
        double size = scores.length;
        double score;

        for (int i = 0; size > i; i++) {
            score = scores[i];
            if (score == 1)
                return 1;
            else if (score > res)
                res = score;
        }

        return res;
    }

    private double maxThreshold() {

        return Double.NEGATIVE_INFINITY;
    }

    private double and(double[] scores) {

        double res = 1;
        double size = scores.length;

        for (int i = 0; size > i; i++) {
            res = res * scores[i];
        }

        return res;
    }

    private double andThreshold(double oldThreshold, double[] scores, int size, int index) {

        double remainingProd = 1;
        
        for(int i = 0; scores.length > i; i++){
            if(index != i){
                remainingProd = remainingProd * scores[i];
            }
        }
              
        if(size == 1){
            return oldThreshold;
        }
        else{
            //esta formula era aplicada quando era passado o score do nivel ja calculado
            //Tinha problemas com a precisao no arredondamento dos doubles
            //return oldThreshold / (score/nodePF);
            
            return oldThreshold / remainingProd;
        }
    }

    private double[] fillMaxScore(int size, double maxVal) {
        double [] maxScore = new double[size];
        for (int i = 0; size > i; i++) {
            maxScore[i] = maxVal;
        }
        return maxScore;
    }
    
/*    private double[] fillMaxScorePF(List<ObjBayes> l) {
        int size = l.size();
        double [] maxScore = new double[size];
        for (int i = 0; size > i; i++) {
            ObjBayes node = l.get(i);
            if(node.isFinalNode()){
                maxScore[i] = getLeafScorePF(node);
            }
            else{
                maxScore[i] = queryPruningFactor(node);
            }
        }
        return maxScore;
    }*/
    
    private double[] fillMaxScorePF(List<ObjBayes> l, int size) {
        double [] maxScore = new double[size];
        ObjBayes node;
        for (int i = 0; size > i; i++) {
            node = l.get(i);
            maxScore[i] = node.getPruningFactor();
        }
        return maxScore;
    }

    private void listScores(double[] scores) {
        System.out.println("Inicio de scores");
        for (int i = 0; scores.length > i; i++) {
            System.out.println(scores[i]);
        }
        System.out.println("Fim de scores");
    }

}
