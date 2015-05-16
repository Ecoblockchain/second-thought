var DB = require("../index");
var _ = require("underscore")._;
var assert = require("assert");
var DA = require("deasync");

describe.only("Sync ops", function () {
  var db;
  before(function(){
    db = DB.connectSync({db : "test"});
    db.dropDbSync("test");
    db.createDbSync("test");
    db.installSync(["foo", "bar"]);
  });

  describe("Loading", function () {
    it("loads properly", function(){
      assert(db.foo && db.bar, "Load biffed");
    });
  });

  describe("Sync CRUD", function () {
    var newFoo, readFoo, updatedFoo, deletedFoo;
    before(function(){
      newFoo = db.foo.saveSync({name : "Test"});
      readFoo = db.foo.firstSync({name : "Test"});
      newFoo.name = "Cheese";
      updatedFoo = db.foo.saveSync(newFoo);
      deletedFoo = db.foo.destroySync(newFoo.id);
    });
    it("Creates a record", function(){
      assert(newFoo.id)
    });
    it("Reads the new record", function(){
      assert(readFoo.id)
    });
    it("Updates the record", function(){
      assert.equal(updatedFoo.name,"Cheese");
    });
    it("Deleted foo", function(){
      assert(deletedFoo);
    });
  });
  describe("Sync queries", function () {
    var res;
    before(function(){
      var inserted = db.foo.saveSync([{name : "Bob"},{name : "Joe"}, {name : "Jill"}, {name : "Alice"}]);
      res = db.foo.containsSync({field : "name", vals : ["Bob", "Joe"]});
    });
    it("returns 2 records", function(){
      assert.equal(2, res.length);
    });
  });
});