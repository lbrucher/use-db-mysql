'use strict';
const TH = require('./test-helper');
const MySQL = require('../index');
const should = require('should');

describe('Transactions', () => {
  const tables = [
    ["address", "id INT NOT NULL AUTO_INCREMENT", "street VARCHAR(255) NOT NULL", "postcode INT NOT NULL", "city VARCHAR(255) NOT NULL", "PRIMARY KEY(id)"],
    ["user",    "id INT NOT NULL AUTO_INCREMENT", "name VARCHAR(255) NOT NULL UNIQUE", "address_id INT REFERENCES address(id) ON DELETE CASCADE", "PRIMARY KEY(id)"]
  ];

  TH.beforeEach(tables);



  it("should commit a transaction", async () => {
    await TH.createDriver(TH.dbOpts, async (mysql, client) => {
      await mysql.startTransaction(client, mysql.txIsolationLevels.RR);
      (await mysql.exec(client, "INSERT INTO address(street,postcode,city) VALUES('Red avenue', 1390, 'Nethen')")).should.equal(1);
      (await mysql.exec(client, "INSERT INTO address(street,postcode,city) VALUES('Purple avenue', 1300, 'Jodoigne'),('Green road', 1390, 'Grez')")).should.equal(2);
      (await mysql.exec(client, "INSERT INTO user(name,address_id) VALUES('John', 2),('Mary', 3)")).should.equal(2);

      let rows = await mysql.query(client, "SELECT * FROM address WHERE postcode=1390");
      JSON.parse(JSON.stringify(rows)).should.eql([
        {id:1, street:'Red avenue', postcode:1390, city:'Nethen'},
        {id:3, street:'Green road', postcode:1390, city:'Grez'}
      ]);

      rows = await mysql.query(client, "SELECT * FROM user ORDER BY name");
      JSON.parse(JSON.stringify(rows)).should.eql([
        {id:1, name:'John', address_id:2},
        {id:2, name:'Mary', address_id:3}
      ]);

      await mysql.exec(client, "COMMIT");
    });

    // Now check that we can still find those records in the DB
    await TH.createDriver(TH.dbOpts, async (mysql, client) => {
      const addresses = await mysql.query(client, "SELECT * FROM address");
      const users = await mysql.query(client, "SELECT name FROM user ORDER BY name");

      addresses.length.should.equal(3);
      JSON.parse(JSON.stringify(users)).should.eql([{name:'John'},{name:'Mary'}]);
    });
  });


  it("should fail a transaction", async () => {
    // Create a user and its adress
    await TH.createDriver(TH.dbOpts, async (mysql, client) => {
      await mysql.startTransaction(client, mysql.txIsolationLevels.RR);
      (await mysql.exec(client, "INSERT INTO address(street,postcode,city) VALUES('Red avenue', 1390, 'Nethen')")).should.equal(1);
      (await mysql.exec(client, "INSERT INTO user(name,address_id) VALUES('John', 1)")).should.equal(1);
      await mysql.exec(client, "COMMIT");
    });

    // Create a second user with the same name as the first user
    try {
      await TH.createDriver(TH.dbOpts, async (mysql, client) => {
        await mysql.startTransaction(client, mysql.txIsolationLevels.RR);
        (await mysql.exec(client, "INSERT INTO address(street,postcode,city) VALUES('Green avenue', 1300, 'Jodoigne')")).should.equal(1);
        await mysql.exec(client, "INSERT INTO user(name,address_id) VALUES('John', 2)");
        await mysql.exec(client, "COMMIT");
        should.fail("Should not get here!");
      });
    }
    catch(err){
      err.code.should.equal('ER_DUP_ENTRY');
    }

    // Now verify that the second address and user were in effect not added to the DB
    await TH.createDriver(TH.dbOpts, async (mysql, client) => {
      const addresses = await mysql.query(client, "SELECT * FROM address");
      const users = await mysql.query(client, "SELECT * FROM user ORDER BY name");

      JSON.parse(JSON.stringify(addresses)).should.eql([{id:1, street:'Red avenue', postcode:1390, city:'Nethen'}]);
      JSON.parse(JSON.stringify(users)).should.eql([{id:1, name:'John', address_id:1}]);
    });
  });


  it("should rollback a transaction", async () => {
    // Create a user and its adress and then rollback instead of commit the transaction
    await TH.createDriver(TH.dbOpts, async (mysql, client) => {
      await mysql.startTransaction(client, mysql.txIsolationLevels.RR);
      (await mysql.exec(client, "INSERT INTO address(street,postcode,city) VALUES('Red avenue', 1390, 'Nethen')")).should.equal(1);
      (await mysql.exec(client, "INSERT INTO user(name,address_id) VALUES('John', 1)")).should.equal(1);
      await mysql.exec(client, "ROLLBACK");
    });

    // Now verify that our DB is still empty
    await TH.createDriver(TH.dbOpts, async (mysql, client) => {
      const addresses = await mysql.query(client, "SELECT * FROM address");
      const users = await mysql.query(client, "SELECT * FROM user ORDER BY name");

      addresses.should.eql([]);
      users.should.eql([]);
    });
  });
});
