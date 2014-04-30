// Actualmente n�o est� a ser usado. De momento a ordena��o � feita na altura da inser��o
// de um novo elemento.

package DuplicateDetection;

import java.util.List;

public class SortingAlgos {

    private List<ObjSort> _dup;

    public SortingAlgos(List<ObjSort> dupList) {

        _dup = dupList;
    }

    // est� a dar stack overflow nas fun�oes de and e or. Nao esquecer de trocar
    // por outro algoritmo de ordena�ao
    public void quicksort(int p, int r) {

        if (p < r) {
            int q;
            q = partition(_dup, p, r);
            quicksort(p, q - 1);
            quicksort(q + 1, r);
        }

    }

    public int partition(List<ObjSort> l, int p, int r) {

        ObjSort objOrd = l.get(r);
        int i = p - 1;
        int j;
        for (j = p; j <= r - 1; j++) {// sinal como estava era <= (ordem
            // crescente)
            if (l.get(j).getSimilaridade() >= objOrd.getSimilaridade()) {
                i = i + 1;
                ObjSort h;
                h = l.get(i);
                l.set(i, l.get(j));
                l.set(j, h);

            }
        }
        ObjSort k;
        k = l.get(i + 1);
        l.set(i + 1, l.get(r));
        l.set(r, k);

        return i + 1;
    }

    public void insertionSort() {

        int j;
        int i;
        ObjSort key;
        for (j = 1; j < _dup.size(); j++) {
            key = _dup.get(j);
            i = j - 1;
            while (i > -1 && ((ObjSort) _dup.get(i)).getSimilaridade() < key.getSimilaridade()) {
                _dup.set(i + 1, _dup.get(i));
                i = i - 1;

            }
            _dup.set(i + 1, key);// System.out.println("A alterar posi��o "+
            // i+1);
        }
    }

    // HEAPSORT

    public void heapSort(List<ObjSort> v) {
        buildMaxHeap(v);
        int n = v.size();

        for (int i = v.size() - 1; i > 0; i--) {
            swap(v, i, 0);
            maxHeapify(v, 0, --n);
        }
    }

    private void buildMaxHeap(List<ObjSort> v) {
        for (int i = v.size() / 2 - 1; i >= 0; i--)
            maxHeapify(v, i, v.size());
    }

    private void maxHeapify(List<ObjSort> v, int pos, int n) {
        int max = 2 * pos + 1, right = max + 1;
        if (max < n) {
            if (right < n && v.get(max).getSimilaridade() > v.get(right).getSimilaridade())
                max = right;
            if (v.get(max).getSimilaridade() < v.get(pos).getSimilaridade()) {
                swap(v, max, pos);
                maxHeapify(v, max, n);
            }
        }
    }

    public void swap(List<ObjSort> v, int j, int aposJ) {
        ObjSort aux = null;
        aux = (ObjSort) v.get(j);
        v.set(j, v.get(aposJ));
        v.set(aposJ, aux);
    }

    // FIM HEAPSORT

    public List<ObjSort> getDup() {

        return _dup;
    }

}
