'use strict';
const TH = require('./test-helper');
const MySQL = require('../index');
const should = require('should');

describe('Queries', () => {
  const tables = [["test", "id INT NOT NULL AUTO_INCREMENT", "name VARCHAR(255) NOT NULL", "zip INT", "city VARCHAR(128)", "PRIMARY KEY(id)"]];

  TH.beforeEach(tables);


  it("should initialize", async () => {
    const mysql = new MySQL(TH.dbOpts);
    await mysql.initialize({logger:TH.noopLogger});
  });

  it("should execute a query", async () => {
    await TH.createDriver(TH.dbOpts, async (mysql, client) => {
      (await mysql.exec(client, "INSERT INTO test(name,zip,city) VALUES('john', 1390, 'Nethen')")).should.equal(1);
      (await mysql.exec(client, "INSERT INTO test(name,zip,city) VALUES('mary', 1300, 'Jodoigne')")).should.equal(1);
      (await mysql.exec(client, "INSERT INTO test(name,zip,city) VALUES('grace', 1390, 'Grez')")).should.equal(1);

      const rows = await mysql.query(client, "SELECT * FROM test WHERE zip=1390");
      JSON.parse(JSON.stringify(rows)).should.eql([
        {id:1, name:'john', zip:1390, city:'Nethen'},
        {id:3, name:'grace', zip:1390, city:'Grez'}
      ]);
    });
  });

  it("should execute a query returning no data", async () => {
    await TH.createDriver(TH.dbOpts, async (mysql, client) => {
      (await mysql.exec(client, "INSERT INTO test(name,zip,city) VALUES('john', 1390, 'Nethen')")).should.equal(1);
      (await mysql.exec(client, "INSERT INTO test(name,zip,city) VALUES('mary', 1300, 'Jodoigne')")).should.equal(1);
 
      const rows = await mysql.query(client, "SELECT * FROM test WHERE zip=1200");
      rows.should.eql([]);
    });
  });

  describe("with connection pooling", () => {
    it("should execute a query", async () => {
      const opts = {...TH.dbOpts, poolSize:10};

      await TH.createDriver(opts, async (mysql) => {
        const clients = [];
        try {
          // create multiple client connections
          clients.push( await mysql.getClient() );
          (await mysql.exec(clients[0], "INSERT INTO test(name,zip,city) VALUES('john', 1390, 'Nethen')")).should.equal(1);
          (await mysql.exec(clients[0], "INSERT INTO test(name,zip,city) VALUES('mary', 1300, 'Jodoigne')")).should.equal(1);

          clients.push( await mysql.getClient() );
          (await mysql.exec(clients[1], "INSERT INTO test(name,zip,city) VALUES('grace', 1390, 'Grez')")).should.equal(1);

          clients.push( await mysql.getClient() );
          const rows = await mysql.query(clients[2], "SELECT * FROM test WHERE zip=1300");
          JSON.parse(JSON.stringify(rows)).should.eql([
            {id:2, name:'mary', zip:1300, city:'Jodoigne'}
          ]);
        }
        finally {
          for(const client of clients){
            await mysql.releaseClient(client);
          }
        }
      });
    });
  });

});
