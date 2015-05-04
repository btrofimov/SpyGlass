package parallelai.spyglass.hbase;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public abstract class HBaseOperation {
    public enum OperationType {
        PUT_COLUMN, DELETE_COLUMN, DELETE_FAMILY, DELETE_ROW, NO_OP
    }

    public static class PutColumn extends HBaseOperation {
        private final ImmutableBytesWritable value;

        public PutColumn(final ImmutableBytesWritable value) {
            super(OperationType.PUT_COLUMN);
            this.value = value;
        }

        public byte[] getBytes() {
            return value.get();
        }
    }

    public static class DeleteColumn extends HBaseOperation {
        private String family;
        private String column;
        public DeleteColumn(String family, String column) {
            super(OperationType.DELETE_COLUMN);
            this.family = family;
            this.column = column;
        }
        public String getFamily(){
            return family;
        }
        public String getColumn(){
            return column;
        }
    }

    public static class DeleteFamily extends HBaseOperation {
        private String family;
        public DeleteFamily(String family) {
            super(OperationType.DELETE_FAMILY);
            this.family = family;
        }

        public String getFamily(){
            return family;
        }
    }

    public static class DeleteRow extends HBaseOperation {
        public DeleteRow() {
            super(OperationType.DELETE_ROW);
        }
    }

    static class NoOp extends HBaseOperation {
        public NoOp() {
            super(OperationType.NO_OP);
        }
    }

    //public static final DeleteColumn DELETE_COLUMN = new DeleteColumn();
   // public static final DeleteFamily DELETE_FAMILY = new DeleteFamily();
    //public static final DeleteRow DELETE_ROW = new DeleteRow();
    //public static final NoOp NO_OP = new NoOp();

    private final OperationType operationType;

    private HBaseOperation(final OperationType operationType) {
        this.operationType = operationType;
    }

    public OperationType getType() {
        return operationType;
    }
}