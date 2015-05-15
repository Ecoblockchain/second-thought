var db = require("../index");
var _ = require("underscore")._;
var should = require("chai").should();

describe("Installer", function(){

  beforeEach(function(done){
    db.connect({db : "test"}, function(err, db){
      db.dropDb("test", function(err,result){
        done();
      });
    });
  });

  describe("database creation", function(){
    beforeEach(function(done){
      db.install(['foo', 'bar'],function(err,result){
        done();
      });
    });

    it("creates the test db", function(done){
      db.dbExists("test", function(err,exists){
        exists.should.equal(true);
        done();
      });
    });
    it("creates the foo table", function(done){
      db.tableExists("foo", function(err,exists){
        exists.should.equal(true);
        done();
      });
    });
    it("creates the bar table", function(done){
      db.tableExists("bar", function(err,exists){
        exists.should.equal(true);
        done();
      });
    });
    it("creates an index for foo name", function (done) {
      db.foo.index("name", function(err,result){
        result.should.equal(true);
        done();
      });
    });
  });

});

describe("Tables", function(){
  var testRecord = {};
  beforeEach(function(done){
    db.connect({db : "test"}, function(err,db){

      //save a record in for testing
      db.foo.save({name : "Mikey"}, function(err, result){
        testRecord = result;
        done();
      });
    });
  });
  afterEach(function(done){
    db.foo.destroyAll(function(err,result){
      done();
    });
  });
  it("creates the foo and bar tables", function (done) {
    should.exist(db.foo);
    should.exist(db.bar);
    done();
  });

  it("saves data", function(done){
    should.exist(testRecord.id);
    done();
  });

  it("deletes all data", function(done){
    db.foo.destroyAll(function(err, result){
      result.should.equal(1);
      done();
    });
  });

  it.skip("deletes a single record", function (done) {
    db.foo.destroy(testRecord.id, function (err, result) {
      result.should.equal(true);
      done();
    });
  });

  it("runs a single update", function (done) {
    db.foo.updateOnly({name: "Steve"}, testRecord.id, function(err,result){
      result.should.equal(true);
    });
    done();
  });

  it("queries the docs", function (done) {
    db.foo.query({name: "Mikey"}, function (err, result) {
      result.should.have.length(1);
      done();
    });
  });
  it("queries single docs", function (done) {
    db.foo.first({name: "Mikey"}, function (err, result) {
      result.id.should.equal(testRecord.id);
      done();
    });
  });
  it("tells me that Mikey exists", function (done) {
    db.foo.exists({name: "Mikey"}, function (err, result) {
      result.should.equal(true);
      done();
    });

  });
});