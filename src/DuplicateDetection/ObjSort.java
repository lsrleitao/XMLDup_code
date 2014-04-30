package DuplicateDetection;

/**
 * @author Lu�s Leit�o
 */
public class ObjSort {

    private float _similaridade;//double para os valores serem mais descriminativos
    private short _indexNode1;//int ou long para grandes bases de dados
    private short _indexNode2;
    private boolean _dup;

    public ObjSort() {

    }

    public void setSimilaridade(float sim) {
        _similaridade = sim;
    }

    public void setIndexNode1(short in1) {
        _indexNode1 = in1;
    }

    public void setIndexNode2(short in2) {
        _indexNode2 = in2;
    }

    public void setDup(boolean state) {
        _dup = state;
    }

    public double getSimilaridade() {
        return _similaridade;
    }

    public int getIndexNode1() {
        return _indexNode1;
    }

    public int getIndexNode2() {
        return _indexNode2;
    }

    public boolean getDupState() {
        return _dup;
    }

}
