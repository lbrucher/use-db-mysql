'use strict';
const TH = require('./test-helper');
const should = require('should');

describe.only('Migrations', () => {

  TH.beforeEach([["migs"]]);

  describe('ensureMigrationsTable', () => {
    it("should create a migration table when it does not exist", async () => {
      await TH.createDriver(TH.dbOpts, async (mysql, client) => {
        // Ensure there is no 'migs' table
        try{
          await mysql.query(client, "SELECT * FROM migs");
          should.fail("Should not get here!");
        }
        catch(err){
          err.code.should.equal('ER_NO_SUCH_TABLE');
        }

        // Create the migs table
        await mysql.ensureMigrationsTable('migs');

        // Query should now work
        const rows = await mysql.query(client, "SELECT * FROM migs");
        rows.should.eql([]);
      });
    });

    it("should not create a migration table when one already exist", async () => {
      await TH.createDriver(TH.dbOpts, async (mysql, client) => {
        // Create the migs table
        await mysql.exec(client, "CREATE TABLE migs(name VARCHAR(128) NOT NULL, updated_at TIMESTAMP NOT NULL, PRIMARY KEY(name))");
        let rows = await mysql.query(client, "SELECT * FROM migs");
        rows.should.eql([]);

        // Do not re-create the migs table
        await mysql.ensureMigrationsTable('migs');

        // Query should still work
        rows = await mysql.query(client, "SELECT * FROM migs");
        rows.should.eql([]);
      });
    });
  });


  describe('listExecutedMigrationNames', () => {
    it("should return an empty list when there are no completed migrations", async () => {
      await TH.createDriver(TH.dbOpts, async (mysql, client) => {
        await mysql.ensureMigrationsTable('migs');
        const names = await mysql.listExecutedMigrationNames('migs');
        names.should.eql([]);
      });
    });

    it("should return the list of completed migrations", async () => {
      await TH.createDriver(TH.dbOpts, async (mysql, client) => {
        await mysql.ensureMigrationsTable('migs');

        const now = Date.now();
        await mysql.exec(client, "INSERT INTO migs(name, updated_at) VALUES('001-init',?),('002-blah',?)", [mysql.timestamp(now), mysql.timestamp(now+10000)]);

        const names = await mysql.listExecutedMigrationNames('migs');
        JSON.parse(JSON.stringify(names)).should.eql(['001-init', '002-blah']);
      });
    });
  });


  describe('logMigrationSuccessful', () => {
    it("should log migrations", async () => {
      await TH.createDriver(TH.dbOpts, async (mysql, client) => {
        await mysql.ensureMigrationsTable('migs');
        (await mysql.query(client, "SELECT name FROM migs ORDER BY name")).should.eql([]);

        const conn = {
          exec: (sql, params) => mysql.exec(client, sql, params)
        };
        await mysql.logMigrationSuccessful(conn, 'migs', '1-mig');
        await mysql.logMigrationSuccessful(conn, 'migs', '2-mig');
        const rows = await mysql.query(client, "SELECT name FROM migs ORDER BY name");
        JSON.parse(JSON.stringify(rows)).should.eql([{name:'1-mig'},{name:'2-mig'}]);
      });
    });
  });


  it("should expose the transaction isolation level to be used during migrations", async () => {
    await TH.createDriver(TH.dbOpts, async (mysql, client) => {
      mysql.getMigrationTransactionIsolationLevel().should.equal('rr');
    });
  });

});
