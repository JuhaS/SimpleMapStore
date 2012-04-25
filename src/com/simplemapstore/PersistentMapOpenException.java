/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.simplemapstore;

import com.almworks.sqlite4java.SQLiteException;

/**
 *
 * @author juhasu
 */
class PersistentMapOpenException extends RuntimeException {

    public PersistentMapOpenException(SQLiteException e) {
        super(e);
    }
    
}
