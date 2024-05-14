package com.mammb.code.db;

public interface Index {

    boolean next();

    static Index hashIndex(Transaction tx, String name, Layout layout) {
        return new HashIndex(tx);
    }


    class HashIndex implements Index {
        private Transaction tx;

        public HashIndex(Transaction tx) {
            this.tx = tx;
        }

        @Override
        public boolean next() {
            return false;
        }
    }

}
