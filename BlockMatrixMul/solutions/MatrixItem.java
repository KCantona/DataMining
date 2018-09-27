/**
 * Matrix Item row major
 * (row, col, val)
 */
public class MatrixItem {
    private Integer matRow;
    private Integer matCol;
    private Integer matVal;

    public Integer getMatRow() {
        return matRow;
    }

    public void setMatRow(Integer matRow) {
        this.matRow = matRow;
    }

    public Integer getMatCol() {
        return matCol;
    }

    public void setMatCol(Integer matCol) {
        this.matCol = matCol;
    }

    public Integer getMatVal() {
        return matVal;
    }

    public void setMatVal(Integer matVal) {
        this.matVal = matVal;
    }

    public boolean checkMatrixZero(){
        return matVal == null || matVal== 0;
    }



    @Override
    public String toString() {
        return "(" + matRow + ',' + matCol + ',' + matVal + ')';
    }

}
