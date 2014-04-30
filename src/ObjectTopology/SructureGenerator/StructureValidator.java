package ObjectTopology.SructureGenerator;

import java.util.ArrayList;
import java.util.List;

public class StructureValidator {

	private int _leafNodes;
	private List<Integer> _visitedNodes = new ArrayList<Integer>();
	
	public StructureValidator(int leafNodes){
		this._leafNodes = leafNodes;
	}
	
	public boolean isValidStructure(int[][] structure){
		
		_visitedNodes = new ArrayList<Integer>();
		
		if(rootNodeHasChilds(structure) 
				&& groupNodeConnection(structure) 
				&& noSingleChildConnection(structure)
				
				&& singleParentConnection(structure)
				//&& noNodeCiclesExist(structure, 0, _leafNodes, new ArrayList<Integer>()) //Desnecessario com a nova estrutura da matriz julgo eu
				
				&& rootReachableFromAllLeaves(structure, 0, _leafNodes)
				&& areUnvisitedNodesConnected(structure))
			return true;
		else
			return false;
		
	}
	
	//Verifica que as todas as folhas possuem uma e só uma ligação
	private boolean  singleParentConnection(int[][] structure){
		
		int conn = 0;
		
		for(int j = 0; _leafNodes > j; j++){
			for(int i = _leafNodes; _leafNodes + (_leafNodes - 2) + 1 > i; i++){
				conn = conn + structure[i][j];
			}	
			if(conn == 0 || conn > 1)
				return false;
			
			conn = 0;
		}
		return true;
	}
	
	//Verifica que os nos agregadores nao possuem apenas uma ligacao a um no pai. 
	private boolean noSingleChildConnection(int[][] structure){
		
		int conn = 0;
		
		for(int i = _leafNodes; _leafNodes + (_leafNodes - 2) > i; i++){
			
			conn = 0;
			for(int j = 0; _leafNodes + (_leafNodes - 2) > j; j++){
				if(i > j){
					conn = conn + structure[i][j];
				}
			}
			
			if(conn == 1){
				return false;
			}
			
		}

		return true;
	}
	
	//verifica se o nó raiz tem pelo menos uma ligação a um nó pai
	private boolean rootNodeHasChilds(int[][] structure){
		
		int conn = 0;
		int rootColumnIndex = structure.length-1;
		
		for(int j = 0; _leafNodes + (_leafNodes - 2) > j; j++){

				conn = conn + structure[rootColumnIndex][j];
		}

			if(conn == 0 || (conn == 1 && _leafNodes > 1))
				return false;
			else
				return true;
	}
	
	//Verifica se um no agregador nao se encontra ligado a mais de um no agregador
	private boolean groupNodeConnection(int[][] structure){
		
		int conn = 0;
		
		for(int j = _leafNodes; _leafNodes + (_leafNodes - 2) > j; j++){
			
			conn = 0;
			for(int i = _leafNodes; _leafNodes + (_leafNodes - 2 + 1) > i; i++){
				if(i > j){
					conn = conn + structure[i][j];
				}
			}
			
			if(conn > 1){
				return false;
			}
			
		}
		
		return true;
	}
	
	//Verifica se existem ciclos entre os nos
	private boolean noNodeCiclesExist(int [][] structure, int indexj, int jumpIndex, List<Integer> visitedNodes){	
			
		for(int j = indexj; jumpIndex > j; j++){
			
			for(int i = _leafNodes; _leafNodes + (_leafNodes - 2 + 1) > i; i++){
				if(structure[i][j] == 1){
					if(i == (_leafNodes + (_leafNodes - 2))){
						continue;
					}
					else{
						List<Integer> lst = updateVisitedNodes(visitedNodes,i);
						if(lst.size() == 0)
							return false;
						else
							return noNodeCiclesExist(structure, i, i+1, lst);	
					}
				}
				else{
					continue;
				}

			}
			
		}
			
		return true;
	}
	
	public List<Integer> updateVisitedNodes(List<Integer> lst, int nodeIndex){
		
		List<Integer> res = new ArrayList<Integer>();
		
		for(int i = 0; lst.size() > i ; i++){
			
			if(lst.get(i) == nodeIndex){
				return res;
			}
		}
		
		res = lst;
		res.add(nodeIndex);
		
		return res;

	}
	
	//Verifica se se consegue atingir a raiz a partir de qualquer folha
	public boolean rootReachableFromAllLeaves(int[][] structure, int indexj, int jumpIndex){
		
		for(int j = indexj; jumpIndex > j; j++){
					
			for(int i = _leafNodes; _leafNodes + (_leafNodes - 2 + 1) > i; i++){
				if(i <= j || (i == (_leafNodes + (_leafNodes - 2)) && structure[i][j] == 1)){
					continue;
				}
				if(i < (_leafNodes + (_leafNodes - 2)) && structure[i][j] == 1){
					addVisitedNode(i);//System.out.println(_visitedNodes);
					boolean res = rootReachableFromAllLeaves(structure, i, i+1);
					if(!res)
						return false;
					else 
						break;
				}
				else if(j >= _leafNodes && i == (_leafNodes + (_leafNodes - 2)) && structure[i][j] == 0){
					return false;
				}

			}
			
		}
		
		return true;
	}
	
	
	//Verifica se nos que nao fazem parte dos caminhos das folhas até à raiz se encontram ligados a algum nó (agregador ou raiz)
	private boolean areUnvisitedNodesConnected(int[][] structure){
		
		int conn;
		
		for(int j = _leafNodes; _leafNodes + (_leafNodes - 2) > j; j++){
			
			if(isVisitedNode(j))
				continue;
			
			conn = 0;
			for(int i = _leafNodes; _leafNodes + (_leafNodes - 2 + 1) > i; i++){
				if(i <= j)
					continue;
				else
					conn = conn + structure[i][j];
				
				if(conn > 0)
					return false;
			}
		}

		return true;
		
	}
	
	private boolean isVisitedNode(int nodeIndex){
		for(int i = 0; _visitedNodes.size() > i ; i++){
			if(_visitedNodes.get(i) == nodeIndex)
				return true;
		}
		
		return false;
	}
	
	private boolean addVisitedNode(int nodeIndex){
		
		boolean exists = false;
		for(int i = 0; _visitedNodes.size() > i ; i++){
			
			if(_visitedNodes.get(i) == nodeIndex){
				exists = true;
				break;
			}
		}
		
		if(!exists || _visitedNodes.size() == 0){
			_visitedNodes.add(nodeIndex);
			exists = true;
		}
		
		return exists;
	}
	
}
