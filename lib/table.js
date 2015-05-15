var r = require("rethinkdb");
var assert = require("assert");
var _ = require("underscore")._;
var DA = require("deasync");

/*
 This is the main abstraction, which tweaks the RethinkDb Table prototype to be a bit
 less verbose - favoring direct method calls over fluent interface.

 The table returned from this constructor is a RethinkDB table, with methods like
 "save" and destroy attached to it.
 */
var Table = function(config, tableName){

  var table = r.db(config.db).table(tableName);
  table.name = tableName;
  table.dbName = config.db;

  table.get = function(id, next){
    table.onConnect(function(err,conn){
      assert.ok(err === null, err);
      r.table(tableName).get(id).run(conn, function(err,res){
        conn.close();
        if(next) next(err,res);
      });
    });
  };

  //give it some abilities yo
  table.first = function(criteria, next){
    table.query(criteria, function(err,array){
      next(err, _.first(array));
    });
  };
  table.firstSync = DA(table.first);

  table.exists = function(criteria, next){
    table.onConnect(function(err,conn){
      assert.ok(err === null,err);
      table.query(criteria,function(err,result){
        assert.ok(err === null,err);
        conn.close();
        next(null,result.length > 0);
      });
    });
  };
  table.existsSync = DA(table.exists);

  table.query = function(criteria, next){
    table.onConnect(function(err,conn){
      assert.ok(err === null,err);
      table.filter(criteria).run(conn,function(err,result){
        assert.ok(err === null,err);
        conn.close();
        result.toArray(next);
      });
    });
  };
  table.querySync = DA(table.query);

  table.save = function(thing, next){
    table.onConnect(function(err,conn){
      assert.ok(err === null,err);
      table.insert(thing, {conflict : "replace"}).run(conn,function(err,result){
        assert.ok(err === null,err);
        if(result.generated_keys && result.generated_keys.length > 0){
          thing.id = _.first(result.generated_keys);
        }
        conn.close();
        next(err,thing);
      });
    });
  };
  table.saveSync = DA(table.save);

  table.updateOnly = function(updates, id, next){
    table.onConnect(function(err,conn){
      assert.ok(err === null,err);
      console.log(table);
      table.get(id).update(updates).run(conn,function(err,result){
        assert.ok(err === null,err);
        next(null,result.replaced > 0);
      });
    });
  };
  table.updateOnlySync = DA(table.updateOnly);

  table.destroyAll = function(next){
    table.onConnect(function(err,conn){
      assert.ok(err === null,err);
      table.delete().run(conn,function(err,result){
        assert.ok(err === null,err);
        conn.close();
        next(err,result.deleted);
      });
    });
  };
  table.destroyAllSync = DA(table.destroyAll);

  table.destroy = function(id, next){
    table.onConnect(function(err,conn){
      assert.ok(err === null,err);
      table.get(id).delete().run(conn,function(err,result){
        assert.ok(err === null,err);
        conn.close();
        next(err,true);
      });
    });
  };
  table.destroySync = DA(table.destroy);

  table.index = function(att, next){
    table.onConnect(function(err,conn){
      assert.ok(err === null,err);
      table.indexCreate(att).run(conn,function(err,result){
        assert.ok(err === null,err);
        conn.close();
        next(err,result.created == 1);
      });
    });
  };

  //stole this from https://github.com/rethinkdb/rethinkdb-example-nodejs-chat/blob/master/lib/db.js
  table.onConnect = function(callback) {
    r.connect(config, function(err, conn) {
      table.conn = conn;
      assert.ok(err === null, err);
      //conn['_id'] = Math.floor(Math.random()*10001);
      callback(err, conn);
    });
  };

  return table;

};


module.exports = Table;
