package hyren.serv6.base.key;

import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.serv6.base.exception.BusinessException;

class KeyDBPool {

    private long keyMax;

    private long keyMin;

    private long nextKey;

    private int poolSize;

    private String keyName;

    public KeyDBPool(int poolSize, String keyName) {
        this.poolSize = poolSize;
        this.keyName = keyName;
        retrieveFromDB();
    }

    public long getKeyMax() {
        return keyMax;
    }

    public long getKeyMin() {
        return keyMin;
    }

    public long getNextKey() {
        if (nextKey > keyMax) {
            retrieveFromDB();
        }
        return nextKey++;
    }

    private void retrieveFromDB() {
        int keyFromDB = -1;
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            int result = SqlOperator.execute(db, "UPDATE keytable SET key_value = key_value+" + this.poolSize + " WHERE key_name = ?", this.keyName);
            if (result > 1) {
                throw new BusinessException("DB data update exception: keytable, key is duplictate!");
            }
            if (result < 1) {
                int a = SqlOperator.execute(db, "insert into keytable values(?, 1)", this.keyName);
                if (a != 1) {
                    throw new BusinessException("DB data insert exception: keytable, init data fail!");
                }
                result = SqlOperator.execute(db, "UPDATE keytable SET key_value = key_value+" + this.poolSize + " WHERE key_name = ?", this.keyName);
            }
            if (result != 1) {
                throw new BusinessException("DB data update exception: keytable, update key value fail!");
            }
            Result rs = SqlOperator.queryResult(db, "SELECT key_value FROM keytable WHERE key_name = ?", this.keyName);
            if (rs == null || rs.getRowCount() != 1) {
                throw new BusinessException("DB data select exception: keytable select fail!");
            }
            keyFromDB = rs.getInt(0, "key_value");
            SqlOperator.commitTransaction(db);
        }
        keyMax = keyFromDB;
        keyMin = keyFromDB - poolSize + 1;
        nextKey = keyMin;
    }
}
