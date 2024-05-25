'use strict';
const { driverPrototype } = require('use-db');
const mysql = require('mysql');


// public
const transactionIsolationLevels = {
  'RU':  'ru',
  'RC':  'rc',
  'RR':  'rr',
  'SER': 'ser'
};

// private
const _transactionIsolationLevels = {
  'ru':  'READ UNCOMMITTED',
  'rc':  'READ COMMITTED',
  'rr':  'REPEATABLE READ',
  'ser': 'SERIALIZABLE'
};

const defaultOptions = {
  poolSize: 10,
};


// Options = {
//    host: ''
//    port: 0
//    user: ''
//    password: ''
//    database: ''
//    ssl: '' | { rejectUnauthorized:true|false }
//    poolSize: 0 (0 = no pooling, default = 10)
// }
function MySQL(options) {
  let logger;
  let dbPool;
  let numActiveClients = 0;


  function getConnectionOpts() {
    const opts = {
      host:     options.host,
      port:     options.port,
      user:     options.user,
      password: options.password,
      database: options.database,
    };

    if (!!options.ssl){
      opts.ssl = options.ssl;
    }

    return opts;
  }


  async function createPool(){
    const opts = getConnectionOpts();
    dbPool = mysql.createPool({
      ...opts, 
      connectionLimit: options.poolSize || defaultOptions.poolSize, // max number of clients in the pool
    });

    // dbPool.on('error', (err,client) => {
    //   logger.error("MySQL Pool error: ", err.message, err.stack);
    // });

    logger.info("Created MySQL DB pool");
  }


  function destroyPool(){
    return new Promise((resolve, reject) => {
      if (dbPool){
        logger.info("Destorying MySQL pool. Num active clients: %d", numActiveClients);
        let p = dbPool;
        dbPool = null;
  
        p.end((err) => {
          if (err) {
            reject(err);
          }
          else {
            resolve();
          }
        });
      }
      else {
        resolve();
      }
    });
  }


  async function recreatePool(){
    await destroyPool();
    await createPool();
  }


  function getPooledClient(){
    return new Promise((resolve, reject) => {
      let connectAttempts = 0;

      function get() {
        connectAttempts++;
        dbPool.getConnection((err, connection) => {
          if (err){
            // if it's the first attempt, try recreating the db pool...
            if (connectAttempts === 1){
              logger.warn('Error fetching client from pool, will recreate the db pool and retry... Err: ', err);
              recreatePool().then(() => { get() }).catch(err => { reject(err) });
            }
            // otherwise log the error and get out
            else{
              logger.error('Error fetching client from pool: ', err);
              reject(err);
            }
          }
          else {
            numActiveClients++;
            resolve(connection);
          }
        });
      }

      get();
    });
  }

  async function releasePooledClient(client){
    try{
      numActiveClients--;
      client.release();
    }
    catch(err) {
    }
  }


  this.txIsolationLevels = transactionIsolationLevels;
  this.timestamp = (ms) => (ms==null ? new Date() : new Date(ms));


  this.initialize = async function(opts = {}) {
    logger = opts.logger || this.logger;
    if ((options.poolSize||0) !== 0) {
      await createPool();
    }
  }

  this.shutdown = async function() {
    if (!!dbPool) {
      await destroyPool();
    }
  }

  this.getClient = function() {
    return new Promise((resolve,reject) => {
      if (!!dbPool) {
        getPooledClient()
          .then(client => resolve(client))
          .catch(err => reject(err));
      }
      else {
        const client = mysql.createConnection(getConnectionOpts());
        client.connect();
        numActiveClients++;
        resolve(client);
      }
    });
  }


  this.releaseClient = async function(client) {
    if (!!dbPool) {
      await releasePooledClient(client);
    }
    else {
      try{
        numActiveClients--;
        client.end();
      }
      catch(e) {}
    }
  }


  /*
   * Return an array of rows, or [] if the query returned no data
   */
  this.query = function(client, sql, params) {
    return new Promise((resolve,reject) => {
      client.query(sql, params, (err, results) => {
        if (err){
          reject(err);
        }
        else {
          // results is an array of RowDataPacket, not exactly an Object.
          // This can make comparisons difficult (especially during unit tests)...
          // Should we transform those into pure Object first, before returning the array?
          // Possible implementation:
          // resolve( JSON.parse(JSON.stringify(results)) );
          //
          resolve(results);
        }
      });
    });
  }


  /*
   * Return the number of rows affected by the query
   */
  this.exec = async function(client, sql, params) {
    const data = await this.query(client, sql, params);
    if (Array.isArray(data)){
      return data.length;
    }
    else {
      return data.affectedRows;
    }
  }


  this.startTransaction = async function(client, tx_isolation_level) {
    const tx_level = _transactionIsolationLevels[tx_isolation_level];
    if (tx_level == null){
      logger.error("Invalid tx isolation level [%s]!", tx_isolation_level);
      throw new Error("Invalid transaction isolation level!");
    }

    // Set Transaction written that way cannot be performed after a Start Transaction
    // and will apply to the next transaction only.
    await this.query(client, `SET TRANSACTION ISOLATION LEVEL ${tx_level}`);
    await this.query(client, "START TRANSACTION");
  }

  // Optional method, defaults to: client.query('COMMIT')
  // async function commitTransaction(client) {
  // }

  // Optional method, defaults to: client.query('ROLLBACK')
  // async function rollbackTransaction(client) {
  // }


  this.ensureMigrationsTable = async function (migrationsTableName) {
    const client = await this.getClient();
    try {
      await this.startTransaction(client, transactionIsolationLevels.RR);
      await this.query(client, `CREATE TABLE IF NOT EXISTS ${migrationsTableName}(name VARCHAR(128) NOT NULL, updated_at TIMESTAMP NOT NULL, PRIMARY KEY(name))`);
      await this.query(client, 'COMMIT');
    }
    finally {
      await this.releaseClient(client);
    }
    logger.debug("Migrations table checked OK.");
  }


  this.listExecutedMigrationNames = async function(migrationsTableName) {
    const client = await this.getClient();
    try {
      const rows = await this.query(client, `SELECT name FROM ${migrationsTableName} ORDER BY name`);
      return rows.map(r => r.name);
    }
    finally {
      await this.releaseClient(client);
    }
  }


  this.logMigrationSuccessful = async function(conn, migrationsTableName, migrationName) {
    await conn.exec(`INSERT INTO ${migrationsTableName}(name,updated_at) VALUES(?,?)`, [migrationName, this.timestamp()]);
  }

  
  this.getMigrationTransactionIsolationLevel = function() {
    return transactionIsolationLevels.RR;
  }
}


Object.assign(MySQL.prototype, driverPrototype);
module.exports = MySQL;
