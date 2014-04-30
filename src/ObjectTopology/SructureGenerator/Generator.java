package ObjectTopology.SructureGenerator;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;

import com.hp.hpl.jena.query.engineHTTP.Params.Pair;

public class Generator {


    int _leafNodes;
    int _matrixSize;
    int _groupNodes;//including the root node
    Hashtable<BigInteger, int[][]> _hashStates = new Hashtable<BigInteger, int[][]>();
    int _initialStructureState [][];
    List<String> _nodeTypes = new ArrayList<String>();

    List<Integer> _assignedNodes = new ArrayList<Integer>();
    List<Integer> _assignedLeaves = new ArrayList<Integer>();
    List<int[][]> _states = new ArrayList<int[][]>();

    private int _maxHits = 1000000;


    public Generator(LinkedHashMap<String, NamedNodeMap> nodes){
        _leafNodes = nodes.size()-1;
        _matrixSize = _leafNodes + (_leafNodes - 2 ) + 1;
        _groupNodes = _matrixSize - _leafNodes;

        _initialStructureState = initializeStateMatrix(_matrixSize);


    }

    public Generator(){}

    private int[][] getOneNodeMatrix(){
        int[][] res = new int [2][2];
        res[0][0] = 9;
        res[0][1] = 9;
        res[1][0] = 1;
        res[1][1] = 9;

        return res;

    }

    public int[][] initializeStateMatrix(int nodes){

        int[][] matrix = new int [nodes][nodes];

        for(int i = 0; i < nodes; i++){
            for(int j = 0; j < nodes; j++){
                if(i==j || j == nodes-1 || (i < _leafNodes /*&& _leafNodes > j*/))
                    matrix[i][j] = 9;
                else
                    matrix[i][j] = 0;
            }
        }
        return matrix;

    }

    //gera um novo estado proximo do recebido. Uma troca, uma inserçao ou uma eliminaçao.
    public int[][] generateNeighborState(int[][] state){

        int[][] newState = null;

        Random r = new Random();
        //int operation = r.nextInt(3);//0 - eliminaçao; 1 - inserçao; 2 - troca; 3 - recolocaçao
        int operation = generateRandomNumberRange(1, 3);//nao esta a permitir eliminacoes

        if(operation == 0){
            newState = delete(state);System.out.println("DELETE");
        }

        if(operation == 1){
            newState = insert(state);System.out.println("INSERT");
        }

        if(operation == 2){
            newState = swap(state);System.out.println("SWAP");
        }

        if(operation == 3){
            newState = move(state);System.out.println("MOVE");
        }

        return newState;

    }

    private int[][] swap(int[][] state){

        int[][] stateRes = copyMatrix(state);
        //int leaves = (state.length-1+2)/2;
        List<List<Integer>> availableLeafNodes = new ArrayList<List<Integer>>();

        Random r = new Random();

        for(int j = 0; _leafNodes > j; j++){
            for(int i = _leafNodes; state.length > i; i++){
                if(state[i][j] == 1){
                    List<Integer> pair = new ArrayList<Integer>();
                    pair.add(i);
                    pair.add(j);
                    availableLeafNodes.add(pair);
                }
            }
        }
        if(availableLeafNodes.size() < 2){
            return stateRes;
        }

        int pairIndex = r.nextInt(availableLeafNodes.size());
        List<Integer> pair = availableLeafNodes.get(pairIndex);
        availableLeafNodes.remove(pairIndex);

        pairIndex = r.nextInt(availableLeafNodes.size());
        List<Integer> pair2 = availableLeafNodes.get(pairIndex);

        /*while(pair.get(0) == pair2.get(0) && availableLeafNodes.size() > 1){
            availableLeafNodes.remove(pairIndex);
            pairIndex = r.nextInt(availableLeafNodes.size());
            pair2 = availableLeafNodes.get(pairIndex);
        }*/

        stateRes[pair.get(0)][pair.get(1)] = 0;
        stateRes[pair2.get(0)][pair2.get(1)] = 0;
        stateRes[pair.get(0)][pair2.get(1)] = 1;
        stateRes[pair2.get(0)][pair.get(1)] = 1;

        return stateRes;
    }

    private int[][] move(int[][] state){

        int[][] stateRes = copyMatrix(state);
        //int leaves = (state.length-1+2)/2;
        List<List<Integer>> availableLeafNodes = new ArrayList<List<Integer>>();

        Random r = new Random();

        //recolher folhas com mais de 1 irmao
        //escolher folha aleatoriamente
        //recolher os nos agregadores que estao disponiveis para adicionar a folha escolhida
        //escolher um no agregador aleatoriamente
        //retirar folha da posiçao inicial e colocar como no filho do no agregador seleccionado

        int cnt = 0;
        for(int i = _leafNodes; state.length > i; i++){
            List<List<Integer>> leavesColumn = new ArrayList<List<Integer>>();
            for(int j = 0; state.length-1 > j; j++){
                if(state[i][j] == 1){
                    if(j < _leafNodes){
                        List<Integer> pair = new ArrayList<Integer>();
                        pair.add(i);
                        pair.add(j);
                        leavesColumn.add(pair);
                    }
                    cnt++;
                }
            }
            if(cnt > 2){
                availableLeafNodes.addAll(leavesColumn);
            }
            cnt = 0;
        }

        if(availableLeafNodes.size() == 0){
            return stateRes;
        }
        else{
            List<Integer> connectedGroupNodes = new ArrayList<Integer>();
            for(int j = _leafNodes; state.length-1 > j; j++){
                for(int i = _leafNodes; state.length > i; i++){
                    if(state[i][j] == 1){
                        connectedGroupNodes.add(j);
                        break;
                    }
                }
            }

            if(connectedGroupNodes.size() == 0){
                return stateRes;
            }
            else{
                int pairIndex = r.nextInt(availableLeafNodes.size());
                List<Integer> pair = availableLeafNodes.get(pairIndex);
                pairIndex = r.nextInt(connectedGroupNodes.size());
                if(connectedGroupNodes.get(pairIndex) == pair.get(0)){
                    if(connectedGroupNodes.size() < 2){
                        return stateRes;
                    }
                    else{
                        connectedGroupNodes.remove(pairIndex);
                        pairIndex = r.nextInt(connectedGroupNodes.size());
                    }
                }
                stateRes[pair.get(0)][pair.get(1)] = 0;
                stateRes[connectedGroupNodes.get(pairIndex)][pair.get(1)] = 1;
            }

        }

        return stateRes;

    }

    private int[][] insert(int[][] state){

        int[][] stateRes = copyMatrix(state);
        //int leaves = (state.length-1+2)/2;
        boolean unassigned = true;
        List<Integer> unassignedLeaves = new ArrayList<Integer>();
        List<Integer> unassignedGroupNodes = new ArrayList<Integer>();
        List<List<List<Integer>>> groupableNodes = new ArrayList<List<List<Integer>>>();

        //recolhe folhas que estão de fora
        //usar apenas se for permitida a ausencia de atributos
        /*for(int j = 0; _leafNodes > j; j++){
	        for(int i = _leafNodes; state.length > i; i++){
	            if(state[i][j] == 1){
	                unassigned = false;
	               break;
	            }
	        }
	        if(unassigned){
	            unassignedLeaves.add(j);
	        }
	        unassigned = true;
	    }*/

        //recolhe nos agregadores que estão de fora
        for(int j = _leafNodes; state.length-1 > j; j++){
            for(int i = _leafNodes; state.length > i; i++){
                if(state[i][j] == 1){
                    unassigned = false;
                    break;
                }
            }
            if(unassigned){
                unassignedGroupNodes.add(j);
            }
            unassigned = true;    
        }

        //recolhe os nos que podem ser agrupados
        for(int i = _leafNodes; state.length > i; i++){
            List<List<Integer>> gn = new ArrayList<List<Integer>>();
            for(int j = 0; state.length-1 > j; j++){
                if(state[i][j] == 1){

                    //Esta condição evita que seja colocado mais do que um no agregador no mesmo nivel.
                    //Esta relacionado como a tarefa de classificaçao vai ser feita, onde cada atributo vai corresponder a um nivel.
                    //Como tal, deste modo não faz sentido ter atributos no mesmo nivel e em caminhos diferentes.
                    //Para permitir basta comentar este if. 
                    if(j >= _leafNodes){
                        gn = new ArrayList<List<Integer>>();
                        break;
                    }

                    List<Integer> pair = new ArrayList<Integer>();
                    pair.add(i);
                    pair.add(j);
                    gn.add(pair);
                }
            }

            if(gn.size() > 2){
                groupableNodes.add(gn);
            }
        }	    

        Random r = new Random();
        int nodeIndex;
        int placementIndex;

        int action = 1; //r.nextInt(2);//0 - inserir um no nao atribuido; 1 - inserir um no agregador

        //usar apenas a action 0 se for permitida a ausencia de atributos
        /*if(action == 0 && unassignedLeaves.size() > 0){
	        nodeIndex = r.nextInt(unassignedLeaves.size());	        
	        placementIndex = generateRandomNumberRange(_leafNodes, state.length-1);
            while(!connectedNode(state, placementIndex)){
                placementIndex = generateRandomNumberRange(_leafNodes, state.length-1);
            }
            stateRes[placementIndex][unassignedLeaves.get(nodeIndex)] = 1;
	    }*/
        /*else*/ if(action == 1 && groupableNodes.size() > 0 && unassignedGroupNodes.size() > 0){
            int groupNodeIndex = r.nextInt(unassignedGroupNodes.size());

            int columnIndex = r.nextInt(groupableNodes.size());
            List<List<Integer>> l = groupableNodes.get(columnIndex);
            /*int groupNodeIndex = getNextGroupIndex(unassignedGroupNodes, l.get(0).get(0));
	        if(groupNodeIndex == -1){
	            return stateRes;
	        }*/

            int groupedNodes = generateRandomNumberRange(2, l.size()-1);

            while(groupedNodes > 0){
                groupedNodes--;

                nodeIndex = r.nextInt(l.size());

                /*if(l.get(nodeIndex).get(1) > groupNodeIndex){
    	                return state;
    	                //O no introduzido tem de ter um indice menor do que o no ao qual se vai ligar e um indice menor
    	                //do que o no(s) agregador que vai ficar como seu filho. Isto no caso de o no introduzido nao agregar apenas folhas.
    	            }*/

                stateRes[l.get(nodeIndex).get(0)][l.get(nodeIndex).get(1)] = 0;
                if(stateRes[unassignedGroupNodes.get(groupNodeIndex)][l.get(nodeIndex).get(1)] == 9
                        || stateRes[l.get(nodeIndex).get(0)][unassignedGroupNodes.get(groupNodeIndex)] == 9){
                    System.out.println("Erro na geraçao da estrutura!");
                    System.exit(0);
                }

                stateRes[unassignedGroupNodes.get(groupNodeIndex)][l.get(nodeIndex).get(1)] = 1;

                stateRes[l.get(nodeIndex).get(0)][unassignedGroupNodes.get(groupNodeIndex)] = 1;
                l.remove(nodeIndex);
            }

        }

        return stateRes;  
    }
    
    //remove apenas nos agregadores
    private int[][] delete(int[][] state){
        int[][] stateRes = copyMatrix(state);
        
        List<List<List<Integer>>> lst_groupNodes = new ArrayList<List<List<Integer>>>();
        for(int i = _leafNodes; state.length-1 > i; i++){
            List<List<Integer>> lst_nodes = new ArrayList<List<Integer>>();
            for(int j = 0; state.length-1 > j; j++){
                if(state[i][j]==1){
                    List<Integer> pair = new ArrayList<Integer>();
                    pair.add(i);
                    pair.add(j);
                    lst_nodes.add(pair);
                }
            }
            if(lst_nodes.size() > 1){
                lst_groupNodes.add(lst_nodes);
            }
        }
        
        if(lst_groupNodes.size() == 0){
            return stateRes;
        }
        
        Random r = new Random();
        int groupIndex = r.nextInt(lst_groupNodes.size());
        List<List<Integer>> nodesToReconect = lst_groupNodes.get(groupIndex);
        int groupNodeIndex = nodesToReconect.get(0).get(0);
        int newGroupNodeIndex = -1;
        
        for(int i = _leafNodes; state.length > i; i++){
            if(state[i][groupNodeIndex] == 1){
                newGroupNodeIndex = i;
                break;
            }
        }
        
        for(int k = 0; nodesToReconect.size() > k; k++){
            int iCoord = nodesToReconect.get(k).get(0);
            int jCoord = nodesToReconect.get(k).get(0);
            state[iCoord][jCoord] = 0;
            state[newGroupNodeIndex][jCoord] = 1;
        }
        
        return stateRes;
    }

    private int getNextGroupIndex(List<Integer> lst, int columnIndex){
        int res = -1;
        for(int i = 0; lst.size() > i; i++){ 
            if(lst.get(i)>columnIndex){
                return res;
            }
            res = i;
        }

        return res;
    }

    private boolean connectedNode(int[][] state, int column){
        for(int j = 0; state.length-1 > j; j++){
            if(state[column][j] == 1){
                return true;
            }
        }
        return false;
    }

    private int[][] deleteWithNodeRemoval(int[][] state){

        int[][] stateRes = copyMatrix(state);	    
        //int leaves = (state.length-1+2)/2;    
        List<List<List<Integer>>> candidateNodes = new ArrayList<List<List<Integer>>>();
        List<List<List<Integer>>> candidateNodesGroup = new ArrayList<List<List<Integer>>>();

        for(int i = _leafNodes; state.length > i; i++){
            List<List<Integer>> lst_leaves = new ArrayList<List<Integer>>();
            List<List<Integer>> lst_groupNodes = new ArrayList<List<Integer>>();
            for(int j = 0; state.length-1 > j; j++){
                if(state[i][j]==1){
                    List<Integer> pair = new ArrayList<Integer>();
                    if(j<_leafNodes){
                        pair.add(i);
                        pair.add(j);
                        lst_leaves.add(pair);
                        break;
                    }
                    else{
                        pair.add(i);
                        pair.add(j);
                        lst_groupNodes.add(pair);
                    }
                }
            }

            //testa se pode apagar nos e quais. Armazena os indices dos que pode.
            if(lst_leaves.size() + lst_groupNodes.size() > 2 && lst_leaves.size() > 0){
                candidateNodes.add(lst_leaves);
                candidateNodesGroup.add(lst_groupNodes);       
            }

        }

        if(candidateNodes.size() == 0){
            return stateRes;
        }	    
        else{
            Random r = new Random();
            int index = r.nextInt(candidateNodes.size());
            int index2;

            if(candidateNodes.get(index).size() + candidateNodesGroup.get(index).size() > 2){
                index2 = r.nextInt(candidateNodes.get(index).size());
                List<Integer> p = candidateNodes.get(index).get(index2);
                stateRes[p.get(0)][p.get(1)] = 0;
            }
            else{
                //se possui 2 folhas
                if(candidateNodesGroup.get(index).size() == 0){
                    index2 = r.nextInt(2);
                    List<Integer> p = candidateNodes.get(index).get(index2);
                    stateRes[p.get(0)][p.get(1)] = 0;

                    candidateNodes.get(index).remove(index2);
                    p = candidateNodes.get(index).get(0);
                    stateRes[p.get(0)][p.get(1)] = 0;

                    //passa para o nivel de cima apenas se o pai nao for a raiz
                    if(p.get(0) != state.length-1){
                        int newIndex = moveLeafToUpperLevel(stateRes, p.get(0));
                        stateRes[newIndex][p.get(0)] = 0;
                        stateRes[newIndex][p.get(1)] = 1;
                    }
                }
                //se possui uma folha e um no agregador
                else{
                    List<Integer> p = candidateNodes.get(index).get(0);
                    stateRes[p.get(0)][p.get(1)] = 0;

                    List<Integer> p2 = candidateNodesGroup.get(index).get(0);
                    stateRes = moveNodeToUpperLevel(stateRes, p.get(0), p2.get(0));

                }
            }


        }

        return stateRes;
    }

    private int[][] moveNodeToUpperLevel(int[][] state, int upperGroupNode, int lowerGroupNode){

        for(int j = 0; state.length-1 > j; j++){
            if(state[lowerGroupNode][j] == 1){
                state[lowerGroupNode][j] = 0;
                state[upperGroupNode][j] = 1;
            }
        }

        state[upperGroupNode][lowerGroupNode] = 0;

        return state;
    }	

    private int moveLeafToUpperLevel(int[][] state, int index){
        int res = -1;

        for(int i = 0; state.length > i; i++){
            if(state[i][index] == 1){
                res = i;
                break;
            }
        }

        return res;
    }

    private int[][] eraseGroupNode(int[][] state, int column){
        for(int j = 0; state.length-1 > j; j++){
            if(state[column][j]==1){
                state[column][j] = 0;
            }
        }

        for(int k = 0; state.length > k; k++){
            if(state[k][column] == 1){
                state[k][column] = 0;
                return state;
            }
        }

        return state;
    }

    private boolean isErasableGroupNode(int[][] state, int index, int leaves){

        int cnt=0;

        for(int i = leaves; state.length > i; i++){
            if(state[i][index]==1){
                for(int l = 0; state.length-1 > l; l++){
                    if(state[i][l]==1){
                        cnt++;
                    }
                }
                if(cnt > 2){
                    return true;
                }
            }
        }
        return false;
    }

    public int[][] generateOnlyLeafNodesMatrix(){

        int [][] matrix = initializeStateMatrix(_matrixSize);

        for(int j = 0; _leafNodes > j; j++){
            matrix[_matrixSize-1][j] = 1;
        }

        return matrix;
    }

    //É assumido que cada folha apenas é pai de um unico no. A matriz é gerada segundo essa permissa
    //O resto é preenchido aleatoriamente tendo depois de ser verificadas as regras posteriormente
    public int[][] generateNewMatrix(int[][] state){

        int gi;
        int[][] matrix = copyMatrix(state);

        for(int j = 0; _leafNodes > j; j++){
            gi = generateLineIndex();
            matrix[gi][j] = 1;
        }

        int groupNodesLimit = _leafNodes + (_leafNodes - 2);
        for(int j = _leafNodes; groupNodesLimit > j; j++){
            gi = generateLineIndex();
            while(matrix[gi][j] == 9 || gi <= j){
                gi = generateLineIndex();
            }
            if(makeConnection())
                matrix[gi][j] = 1;
            else
                matrix[gi][j] = 0;
        }

        return matrix;

    }

    public String generateStringKey(String key, int nodeIndex, int[][] state, int groupCnt){

        for(int j = 0; _leafNodes + (_leafNodes - 2) > j; j++){          
            if(state[nodeIndex][j] == 1){
                if(j < _leafNodes){
                    key = key + "g" + groupCnt + "l" + j + "$";
                }else{
                    key = key + "g" + groupCnt;
                    groupCnt++;
                    key = key + "g" + groupCnt + "$";
                    key = generateStringKey(key, j, state, groupCnt);
                }
            }       
        }     
        return key;
    }	    

    public BigInteger generateKey(int[][] state){

        BigInteger key = BigInteger.valueOf(state[_leafNodes][0]);

        int keySize = 0;

        for(int j = 0; _leafNodes + (_leafNodes - 2) > j; j++){
            for(int i = _leafNodes; _leafNodes + (_leafNodes - 2 + 1) > i; i++){
                if(i == j || (i == _leafNodes && j == 0)){
                    continue;
                }
                else{
                    key = (key.multiply(BigInteger.valueOf(10L))).add(BigInteger.valueOf(state[i][j]));
                    keySize++;
                }
            }
        }

        return key;
    }

    private int generateLineIndex(){

        Random r = new Random();
        int i = r.nextInt(_leafNodes-2+1);

        return _leafNodes + i;
    }

    private boolean makeConnection(){
        Random r = new Random();
        int i = r.nextInt(2);

        if(i == 1)
            return true;
        else
            return false;
    }

    private boolean hashStateExists(BigInteger stateKey){

        if(_hashStates.containsKey(stateKey))
            return true;
        else
            return false;
    }

    private int[][] copyMatrix(int[][] source){

        int[][] matrix = new int[source.length][source.length];

        for(int i = 0; source.length > i; i++){
            for(int j = 0; source.length > j; j++){
                matrix[i][j] = source[i][j];

            }
        }
        return matrix;
    }

    public void printMatrix(int[][] matrix){
        for(int i = 0; i < matrix.length; i++){
            System.out.println();
            for(int j = 0; j < matrix.length; j++){
                System.out.print(matrix[j][i] + " ; ");
            }
        }
        System.out.println();
    }

    public void printStates(){
        for(int i = 0; i < _hashStates.size(); i++){
            printMatrix(_hashStates.get(i));
            System.out.println("NEW STATE");
        }
    }

    public void generatePossibleStates(int maxStates){

        if(_leafNodes == 1){
            _hashStates.put(BigInteger.ONE, getOneNodeMatrix());
            return;
        }

        StructureValidator sv = new StructureValidator(_leafNodes);

        int[][] matrix;
        BigInteger key;
        int hits=0;
        int nrStates = 0;

        boolean hashStateExists;

        while((hits < _maxHits || _hashStates.size()== 0) && maxStates > nrStates){
            matrix = generateNewMatrix2(_initialStructureState);
            key = generateKey(matrix);

            sv = new StructureValidator(_leafNodes);

            try{

                hashStateExists = hashStateExists(key);
                if(!hashStateExists /*&& sv.isValidStructure(matrix) /*&& !stateExists(matrix)*/){
                    _hashStates.put(key, matrix);
                    _states.add(matrix);
                    nrStates++;
                    hits = 0;
                }
                else if(hashStateExists)
                    hits++;

            }catch(java.lang.StackOverflowError e){
                printMatrix(matrix);
                System.out.println("saiu");
                System.exit(0);}
        }

    }

    public int[][] getInitialStructureState(){
        return _initialStructureState;
    }

    public int getStates(){
        return _hashStates.size();
    }

    public List<int[][]> getStatesList(){
        return _states;
    }

    //gera um novo estado a partir da raiz respeitando as regras da estrutura
    public int[][] generateNewMatrix2(int[][] state){
        _assignedNodes = new ArrayList<Integer>();
        _assignedLeaves = new ArrayList<Integer>();
        int[][] matrix = copyMatrix(state);
        return generateStructureFromRoot(matrix, _leafNodes + (_leafNodes-2), _assignedLeaves, _assignedNodes, _leafNodes);
    }

    //obriga a que os nós estejam todos presentes na estrutura gerada
    private int[][] generateStructureFromRoot(int[][] structure, int indexI, List<Integer> assignedLeaves, List<Integer> assignedNodes, int nrUpperNodes){

        int[][] st = structure;
        int cnt = 0;

        if(_leafNodes-assignedLeaves.size() == 0)
            return st;

        int maxLeaves = nrUpperNodes;

        int nrConnectedLeaves = selectNumberOfConnectedLeaves(maxLeaves, assignedLeaves, assignedNodes, indexI);

        List<Integer> connectedLeaves = selectConnectedLeaves(_leafNodes, nrConnectedLeaves, assignedLeaves);
        st = markConnectedNodes(st, connectedLeaves, indexI);
        assignedLeaves = updateAssignedNodes(assignedLeaves, connectedLeaves);


        int nrConnectedNodes = selectNumberOfConnectedNodes(getAvailableNodesToAssign(assignedNodes, indexI, maxLeaves-nrConnectedLeaves), nrConnectedLeaves, maxLeaves);

        List<Integer> connectedNodes = selectConnectedNodes(indexI - _leafNodes, nrConnectedNodes, assignedNodes);
        st = markConnectedNodes(st, connectedNodes, indexI);

        assignedNodes = updateAssignedNodes(assignedNodes, connectedNodes);

        int[] nodeDistribution = new int[nrConnectedNodes];
        if(nrConnectedNodes >= 1){
            nodeDistribution = getParentDistribution(nrConnectedNodes, nrConnectedLeaves, maxLeaves);

        }

        for(int j =_leafNodes; j < _leafNodes + (_leafNodes-2); j++){
            if(structure[indexI][j] == 1){
                st = generateStructureFromRoot(st, j, assignedLeaves, assignedNodes, nodeDistribution[cnt]);
                cnt++;
            }
        }

        //adiciona os nós que não ficaram atribuidos
        //comentar if se se pretender que haja a possibilidade de gerar estruturas em que faltem atributos
        if(cnt == nrConnectedNodes && _leafNodes != assignedLeaves.size() && indexI == _leafNodes + (_leafNodes-2)){
            List<Integer> fill = completeRemainingLeaves(assignedLeaves, indexI, st);
            assignedLeaves = updateAssignedNodes(assignedLeaves, fill);
            st = markConnectedNodes(st, fill, indexI);
        }

        return st;
    }

    private int[] getParentDistribution(int nrConnectedNodes, int nrConnectedLeaves, int maxLeaves){

        int [] dist = new int[nrConnectedNodes];

        for(int i = 0; nrConnectedNodes > i; i++){
            dist[i] = 2;
        }

        int remainingNodes = (maxLeaves-nrConnectedLeaves)-(2*nrConnectedNodes);

        while(remainingNodes != 0){
            int selectedNode = generateRandomNumberRange(0, nrConnectedNodes-1);
            remainingNodes--;
            dist[selectedNode] = dist[selectedNode] + 1;
        }

        return dist;

    }

    public int generateRandomNumberRange(int min, int max){
        Random r = new Random();
        return r.nextInt((max-min)+1) + min;
    }

    private List<Integer> completeRemainingLeaves(List<Integer> assignedLeaves, int indexI, int[][] structure){

        List<Integer> lst = new ArrayList<Integer>();

        for(int j = 0; j < _leafNodes; j++){
            if(!nodeAlreadyAssigned(j, assignedLeaves) && structure[indexI][j] == 0){
                lst.add(j);
            }
        }

        return lst;
    }

    private int availableNodes(List<Integer> assignedNodes, int indexI){

        int cnt = 0;

        for(int j = _leafNodes; _leafNodes + (indexI -_leafNodes) + 1 > j; j++){
            if(!nodeAlreadyAssigned(j, assignedNodes))
                cnt++;
        }

        return cnt;
    }

    private int selectNumberOfConnectedLeaves(int alternatives, List<Integer> assignedLeaves,List<Integer> assignedNodes, int indexI){

        if(alternatives <= 2)
            return alternatives;

        int availableNodes = availableNodes(assignedNodes, indexI);

        if(availableNodes == 0){
            return generateRandomNumberRange(2, alternatives);
        }

        else if(availableNodes == 1){
            if ((alternatives-1) >= 2){
                return generateRandomNumberRange(1, alternatives);
            }
            else{
                return generateRandomNumberRange(2, alternatives);
            }
        }

        else{
            if(availableNodes >= 2){

                if(alternatives >= 4){
                    return generateRandomNumberRange(0, alternatives);
                }
                else if(alternatives == 2 || alternatives == 3){
                    return generateRandomNumberRange(1, alternatives);
                }
                else
                    return generateRandomNumberRange(2, alternatives);
            }
        }


        Random r = new Random();
        return r.nextInt(alternatives);
    }

    private int selectNumberOfConnectedNodes(int alternatives, int nrConnectedLeaves, int maxLeaves){

        int res;

        if(alternatives == 0)
            return 0;

        if(nrConnectedLeaves == 0)
            return generateRandomNumberRange(2,alternatives);

        if(nrConnectedLeaves == 1)
            return generateRandomNumberRange(1,alternatives);

        res = generateRandomNumberRange(0,alternatives);

        return res;
    }

    private List<Integer> selectConnectedNodes(int max, int leavesToConnect, List<Integer> unavailableNodes){

        List<Integer> lst = new ArrayList<Integer>();

        if(max == 0)
            return lst;

        Random r = new Random();
        int leafIndex;
        while(lst.size() < leavesToConnect){
            leafIndex = r.nextInt(max);
            if(!nodeAlreadyAssigned(_leafNodes + leafIndex, unavailableNodes) && !nodeAlreadyAssigned(_leafNodes + leafIndex, lst))
                lst.add(_leafNodes + leafIndex);
        }

        return lst;
    }

    private List<Integer> selectConnectedLeaves(int max, int leavesToConnect, List<Integer> unavailableLeaves){

        List<Integer> lst = new ArrayList<Integer>();

        if(max == 0)
            return lst;

        Random r = new Random();
        int leafIndex;
        while(lst.size() < leavesToConnect){
            leafIndex = r.nextInt(max);
            if(!nodeAlreadyAssigned(leafIndex, unavailableLeaves) && !nodeAlreadyAssigned(leafIndex, lst))
                lst.add(leafIndex);		
        }

        return lst;
    }

    private boolean nodeAlreadyAssigned(int leafIndex, List<Integer> lst){
        for(int i = 0; i < lst.size(); i++){
            if(lst.get(i) == leafIndex)
                return true;
        }
        return false;
    }

    private int[][] markConnectedNodes(int[][] structure, List<Integer> connectedLeaves, int indexI){

        for(int j = 0; j < connectedLeaves.size(); j++){
            structure[indexI][connectedLeaves.get(j)] = 1;
        }

        return structure;
    }

    private List<Integer> updateAssignedNodes(List<Integer> assignedLeaves, List<Integer> connectedLeaves){

        for(int i = 0; i < connectedLeaves.size(); i++){
            assignedLeaves.add(connectedLeaves.get(i));
        }

        return assignedLeaves;
    }

    private int getAvailableNodesToAssign(List<Integer> assignedNodes, int indexI, int leavesToAddress){

        int availableNodes = availableNodes(assignedNodes, indexI);

        if(availableNodes == 0)
            return 0;

        if(leavesToAddress/availableNodes >= 2)
            return availableNodes;

        while(availableNodes != 0 && leavesToAddress/availableNodes < 2){
            availableNodes--;
        }

        return availableNodes;
    }

}
