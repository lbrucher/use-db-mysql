'use strict';
const MySQL = require('../index');
const should = require('should');

exports.dbOpts = {};

exports.beforeEach = function(tables) {
  beforeEach(async () => {
    exports.dbOpts = {
      host: process.env['DB_HOST'] || 'localhost',
      port: parseInt(process.env['DB_PORT']||'3306'),
      user: process.env['DB_USER'],
      password: process.env['DB_PASSWORD'],
      database: process.env['DB_NAME'],
    };

    // Drop the test table if one exists
    await exports.dropCreateTables(exports.dbOpts, tables);
  });
}


exports.dropCreateTables = async function(opts, tables) {
  const mysql = new MySQL(opts);
  await mysql.initialize({logger:exports.noopLogger});
  let client;
  try {
    client = await mysql.getClient();

    // Drop all tables first
    // This needs to be done in reverse order compared to creating them
    const invTables = tables.slice(0);   // reverse() changes the array in-place so we slice(0) first
    invTables.reverse();
    for(const table of invTables) {
      try {
        await mysql.exec(client, `DROP TABLE ${table[0]}`);
      }
      catch(err){
        if (err.code !== 'ER_BAD_TABLE_ERROR'){
          should.fail(err);
        }
      }
    }

    // Then recreate all tables
    for(const table of tables) {
      const tableName = table[0];
      const tableFields = table.slice(1);
      if (tableFields.length > 0){
        try {
          await mysql.exec(client, `CREATE TABLE ${tableName}(${tableFields.join(', ')})`);
        }
        catch(err){
          should.fail(err);
        }
      }
    }
  }
  finally {
    await mysql.releaseClient(client);
    await mysql.shutdown();
  }
}


exports.createDriver = async function(opts, fnExec) {
  const mysql = new MySQL(opts);
  await mysql.initialize({logger:exports.noopLogger});
  let client;
  try {
    client = await mysql.getClient();
    await fnExec(mysql, client);
  }
  finally {
    await mysql.releaseClient(client);
    await mysql.shutdown();
  }
}

exports.noopLogger = {
  trace: () => {},
  debug: () => {},
  info:  () => {},
  warn:  () => {},
  error: () => {}
}
