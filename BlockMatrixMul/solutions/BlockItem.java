import java.util.Arrays;
import java.util.List;

/**
 * Block Item row major
 * (row, col, A(row, col))
 */
public class BlockItem {
    private Integer blockRow;
    private Integer blockCol;
    private List<MatrixItem> blockVal;

    public Integer getBlockRow() {
        return blockRow;
    }

    public void setBlockRow(Integer blockRow) {
        this.blockRow = blockRow;
    }

    public Integer getBlockCol() {
        return blockCol;
    }

    public void setBlockCol(Integer blockCol) {
        this.blockCol = blockCol;
    }

    public List<MatrixItem> getBlockVal() {
        return blockVal;
    }

    public void setBlockVal(List<MatrixItem> blockVal) {
        this.blockVal = blockVal;
    }

    public boolean checkBlockZero(){
        return blockVal == null || blockVal.size() == 0;
    }


    @Override
    public String toString() {
        return "(" + blockRow + ',' + blockCol + '[' + blockVal.toString() + ']';
    }
}
