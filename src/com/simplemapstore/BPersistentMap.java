/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.simplemapstore;

import com.sleepycat.je.*;
import java.io.*;
import java.util.LinkedList;
import java.util.List;

/**
 * 
 * @author juhasu
 */
public class BPersistentMap {
    public static final String OBJECT_DB = "objectDb";
    public static final String STRING_DB = "stringDb";
    
    /**
     * Default folder for stored files (inside working directory).
     */
    public static final String defaultEnvironmentFolder = "appdb";
    
    /**
     * Database for storing string values.
     */
    private final Database stringDb;
    
    /**
     * Database for storing object values.
     */
    private Database objectDb;


    private Environment dbEnv;
    private String collectionDbName = "collectionDb";
    private Database collectionDb;
    
    /**
     * Create/open BPersistentMap at default folder inside working directory. 
     * @see #BPersistentMap(String)
     */
    public BPersistentMap() {
        this(defaultEnvironmentFolder);
    }
    
    /**
     * Create instance of BPersistentMap. It first checks for any existing 
     * BPersistentMap inside the given folder. If one is found it is opened and 
     * instance will use that. If no existing database was found then one is 
     * created and that is taken into use. 
     * 
     * If given folder does not exist it is created if possible.
     * 
     * @param folder Environment folder where files are stored.
     */
    public BPersistentMap(String folder) {
        this.dbEnv = initDatabaseEnvironment(folder);
        DatabaseConfig dbconf = new DatabaseConfig();
        
        dbconf.setAllowCreate(true);
        dbconf.setSortedDuplicates(false);
        this.stringDb = getDatabase(STRING_DB, true, false);
        
        try {
            this.objectDb = getDatabase(OBJECT_DB, false, false);
        } catch (DatabaseNotFoundException e) {
            // Do nothing, initiate lazily
        }
        
        try {
            this.collectionDb = getDatabase(collectionDbName, false, true);
        } catch (DatabaseNotFoundException e) {
            // Do nothing, initiate lazily
        }
    }
    
    private Database getDatabase(String dbName, boolean allowCreate, boolean allowDuplicates){
        DatabaseConfig objDbConf = new DatabaseConfig();
        objDbConf.setAllowCreate(allowCreate);
        objDbConf.setSortedDuplicates(allowDuplicates);
        return this.dbEnv.openDatabase(null, dbName, objDbConf);
        
    }
    
    private Environment initDatabaseEnvironment(String folder){
        File homeDir = new File(folder);
        if (!homeDir.exists()){
            if (!homeDir.mkdir()){
                throw new PersistentMapException(null);
            }
        }
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setDurability(Durability.COMMIT_SYNC);
        envConfig.setAllowCreate(true);
        return new Environment(homeDir, envConfig);
    }

    /**
     * Put key value pair into the map. Any existing value with same key is 
     * overwritten. Write is synchronized to the disc before returning 
     * (fulfilling ACID).
     * 
     * Pairs inserted with putObject method are in different namespace than this
     * so same key may exist in object and string namespace.
     * 
     * @param key String key
     * @param value String value
     */
    public void put(String key, String value) {
        if (value == null || key == null){
            throw new NullPointerException();
        }
        try {
            DatabaseEntry keyValue = new DatabaseEntry(key.getBytes("UTF-8"));
            DatabaseEntry dataValue = new DatabaseEntry(value.getBytes("UTF-8"));
            OperationStatus status = stringDb.put(null, keyValue, dataValue); //inserting an entry
        } catch (UnsupportedEncodingException e) {
            throw new PersistentMapException(e);
        }
    }
    
    /**
     * Put key value pair into the map. Any existing value with same key is 
     * overwritten. Write is synchronized to the disc before returning 
     * (fulfilling ACID). The value object must fulfill Serializable interface 
     * and fulfill all Java serialization requirements.
     * 
     * Pairs inserted with put method are in different namespace than this so 
     * same key may exist object and string namespace.
     * 
     * @param key String key
     * @param value Serializable object
     */
    public void putObject(String key, Serializable value) {
        if (value == null || key == null){
            throw new NullPointerException();
        }
        if (this.objectDb == null){
            long start = System.currentTimeMillis();
            DatabaseConfig objDbConf = new DatabaseConfig();
            objDbConf.setAllowCreate(true);
            objDbConf.setSortedDuplicates(true);
            this.objectDb = getDatabase(OBJECT_DB, true, false);
            System.out.println("DB creation took (ms): " + (System.currentTimeMillis()-start));
        }
        try {
            DatabaseEntry keyValue = new DatabaseEntry(key.getBytes("UTF-8"));
            DatabaseEntry dataValue = new DatabaseEntry(serializeObject(value));
            /**
             * TODO: handle overwriting
             */
            OperationStatus status = objectDb.put(null, keyValue, dataValue); //inserting an entry
        } catch (UnsupportedEncodingException e) {
            throw new PersistentMapException(e);
        }
    }

    /**
     * Get value for given String key. If no entry is found, null is returned. 
     * 
     * Values inserted with putObject are in different namespace so they are 
     * not reachable with this method.
     * @param key String key
     * @return Value matching the key or null if not found.
     */
    public String get(String key) {
        try {
            DatabaseEntry searchEntry = new DatabaseEntry();
            stringDb.get(null, new DatabaseEntry(key.getBytes("UTF-8")), searchEntry, LockMode.DEFAULT);
            if (searchEntry.getData() == null){
                return null;
            } else {
                String value = new String(searchEntry.getData(), "UTF-8");
                return value;
            }
            
        } catch (UnsupportedEncodingException e) {
            throw new PersistentMapException(e);
        } 
    }
    
        /**
     * Get an object from the map. If matching entry is not found null is 
     * returned. 
     * 
     * Values inserted with put method are in different namespace so they are 
     * not reachable with this method.
     * 
     * @param key
     * @return Matching object or null if not found.
     */
    public Object getObject(String key) {
        if (objectDb == null){
            return null;
        }
        if (key == null){
            return new NullPointerException();
        }
        try {
            DatabaseEntry searchEntry = new DatabaseEntry();
            objectDb.get(null, new DatabaseEntry(key.getBytes("UTF-8")), searchEntry, LockMode.DEFAULT);
            if (searchEntry.getData() == null){
                return null;
            } else {
                byte[] bytes = searchEntry.getData();
                return deserializeObject(bytes);
            }
        } catch (UnsupportedEncodingException e) {
            throw new PersistentMapException(e);
        } 
    }
    
    /**
     * Delete entry with value of type String (inserted with 
     * put(String, String) method) from the database. If no matching 
     * entry existed false is returned. 
     * 
     * Entries inserted with putObject method are not reachable with this method (check deleteObject).
     * 
     * @param key Key string
     * @return true if entry with given gey was deleted from the database, 
     * false if matching entry was not found.
     */
    public boolean removeString(String key){
        try {
            OperationStatus status = stringDb.delete(null, new DatabaseEntry(key.getBytes("UTF-8")));
            if (status == OperationStatus.SUCCESS){
                return true;
            } else {
                return false;
            }
        } catch (UnsupportedEncodingException e) {
            throw new PersistentMapException(e);
        }
    }
    
    /**
     * Delete entry with value of type Object (inserted with 
     * putObject(String, Object) method) from the database. If no matching entry is found 
     * null is returned. 
     * 
     * Entries inserted with put method are in different namespace and not 
     * reachable with this method (check deleteObject).
     * 
     * @param key Key string
     * @return true if entry with given gey was deleted from the database, 
     * false if matching entry was not found.
     */
    public boolean removeObject(String key){
        try {
            OperationStatus status = objectDb.delete(null, new DatabaseEntry(key.getBytes("UTF-8")));
            if (status == OperationStatus.SUCCESS){
                return true;
            } else {
                return false;
            }
        } catch (UnsupportedEncodingException e) {
            throw new PersistentMapException(e);
        }
    }
    
    /**
     * Return current count of String-String entries (inserted with 
     * #put(String, String).
     * @return count
     */
    public long countStringEntries(){
        return this.objectDb.count();
    }

    /**
     * Return current count of String-Object entries (inserted with 
     * #putObject(String, Object).
     * @return count
     */
    public long countObjectEntries(){
        return this.objectDb.count();
    }
    
    /**
     * Close 
     */
    public void close() {
        
        if (stringDb != null){
            stringDb.close();
        }
        if (objectDb != null){
            objectDb.close();
        }
        if (this.dbEnv != null){
            this.dbEnv.close();
        }
    }

    public void addToCollection(String collectionName, Serializable object){
        if (object == null){
            throw new NullPointerException();
        }
        if (this.collectionDb == null){
            this.collectionDb = getDatabase(collectionDbName, true, true);
        }
        try {
            DatabaseEntry keyValue = new DatabaseEntry(collectionName.getBytes("UTF-8"));
            DatabaseEntry dataValue = new DatabaseEntry(serializeObject(object));
            OperationStatus status = collectionDb.put(null, keyValue, dataValue); //inserting an entry
        } catch (UnsupportedEncodingException e) {
            throw new PersistentMapException(e);
        }
    }
    
    public List<Object> getCollectionItems(String collectionName){
        LinkedList<Object> list = new LinkedList<Object>();
        getCollectionItems(collectionName, list);
        return list;
    }
    
    public void getCollectionItems(String collectionName, List dataList){
        if (collectionName == null){
            throw new NullPointerException();
        }
        if (collectionDb == null){
            return;
        }
        Cursor cursor = null;
        try {
            DatabaseEntry keyValue = new DatabaseEntry(collectionName.getBytes("UTF-8"));
            DatabaseEntry item = new DatabaseEntry();
            cursor = collectionDb.openCursor(null, CursorConfig.DEFAULT);
            LinkedList<Object> list = new LinkedList<Object>();
            
            while (cursor.getNext(keyValue, item, LockMode.DEFAULT) == OperationStatus.SUCCESS){
                if (item.getData() != null){
                    Object object = deserializeObject(item.getData());
                    list.add(object);
                }
            }
        } catch (UnsupportedEncodingException e) {
            throw new PersistentMapException(e);
        } finally {
            if (cursor != null){
                cursor.close();
            }
        } 
    }
    
    public boolean removeCollection(String collectionName){
        if (collectionName == null){
            throw new NullPointerException();
        }
        if (collectionDb == null){
            return false;
        }
        Cursor cursor = null;
        try {
            DatabaseEntry keyValue = new DatabaseEntry(collectionName.getBytes("UTF-8"));
            DatabaseEntry item = new DatabaseEntry();
            cursor = collectionDb.openCursor(null, CursorConfig.DEFAULT);
            LinkedList<Object> list = new LinkedList<Object>();
            boolean deletedItem = false;
            while (cursor.getNext(keyValue, item, LockMode.DEFAULT) == OperationStatus.SUCCESS){
                cursor.delete();
                deletedItem = true;
            }
            cursor.close();
            return deletedItem;
        } catch (UnsupportedEncodingException e) {
            throw new PersistentMapException(e);
        } finally {
            if (cursor != null){
                cursor.close();
            }
        } 
    }
    
    
    /**
     * //////////// Start of private methods
     */
    
    /**
     * Serialize an object using standard Java serialization. The class should 
     * fulfill all requirements for serialization.
     * @param object Object to be serialized.
     * @return object in serialized form.
     */
    private byte[] serializeObject(Serializable object) {
        // Serialize to a byte array
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bos);
            out.writeObject(object);
            out.close();

            byte[] buf = bos.toByteArray();
            return buf;
        } catch (IOException e) {
            throw new PersistentMapException(e);
        }
    }

    /**
     * Deserializes an object using standard Java deserialization.
     * @param bytes Object in serialized form.
     * @return Deserialized object
     */
    private Object deserializeObject(byte[] bytes) {
        // Deserialize from a byte array
        try {
            ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes));
            Object object = in.readObject();
            in.close();
            return object;
        } catch (IOException e) {
            throw new PersistentMapException(e);
        } catch (ClassNotFoundException e) {
            throw new PersistentMapException(e);
        }
    }
}
