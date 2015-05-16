# Some Light Abstraction for RethinkDB

The RethinkDB Node driver is already very simple to use, but there are methods that I wish I "just had" at the ready. Those are:

 - Query, which returns an array
 - First, Exists
 - Save, which upserts a record
 - Automatic table/db config
 - DB Manipulation (Create/Drop)

So I created it. That's what we have here.

In addition, I love Meteor.js and thought I would try to support its synchronous nature as best I could. I know that Meteor uses Fibers, but they can cause problems if you're not completely in control of them.

So, SecondThought uses the `deasync` library to "virtually unblock" the synchronous calls. The thread won't be blocked when you use these sync methods, but the consecutive code calls will. It's expermental, at best.
## Usage

Install using

```
npm install secondthought --save
```

To use this in your code, just configure what you need:

```javascript
var DB = require("second-thought");
DB.connect({db : "test"}, function(err,db){

  //you now have access to all of your tables as properties on your db variable:
  //so, assume there's a table called "foo" in your db...
  db.foo.save({name : "Mike"}, function(err,newRecord){

    //output the generated ID
    console.log(newRecord.id);
  });

});
```

You can do this synchronously as well:

```js
var SecondThought = require("second-thought");
var db = SecondThought.connectSync({db : "test"});
var newRecord = db.foo.saveSync({name : "Pete"});
```

You can intermix the sync/async stuff all you like. Each method you see below has an sync counterpart which you can use by attaching `Sync` to the method name.

Each table on your DB object is a full-blown RethinkDB table, so you can step outside the abstraction at any point:

```javascript
db.openConnection(function(err,conn){

  //this is a ReQL query
  db.foo.eqJoin('bar_id', db.bar).run(conn, function(err,cursor){

    //run the joined action and do something interesting
    cursor.toArray(function(err,array){
      //use the array...

      //be sure to close the connection!
      conn.close();
    });

  });
});

```

In addition you can do all kinds of fun things, like...

```javascript
//installation of the DB and tables
db.connect({db : "test"}, function(err, db){
  db.install(['foo', 'bar'], function(err,result){
    //tables should be installed now...
  });
});

//add a secondary index
db.connect({db : "test"}, function(err,db){

  db.foo.index("email", function(err, indexed){
    //indexed == true;
  });
});
```

## Basic Queries
I've tried to keep the API light and simple - with just a bit of sugar to keep the repetetive stuff to a minimum:

```javascript

db.connect({db : "test", function(err,db){

  db.foo.query({category : "beer"}, function(err,beers){
    //beers is an array, so have at it
  });
  
  db.foo.contains({field : "name", vals : ["Bob", "Jill"]}, function(err,res){
    //res is two records with the names Bob and Jill
  });
  
  db.foo.first({email : "rob@tekpub.com"}, function(err,rob){
    //hi Rob
  });

  db.foo.exists({name : "bill"}, function(err, exists){
    //exists will tell you if it's there
  });

  db.foo.destroy({id : "some-id"}, function(err,destroyed){
    //destroyed will be true if something was deleted
  });

  db.foo.destroyAll(function(err,destroyed){
    //destroyed is the count of records whacked
  });

  db.foo.updateOnly({name : "Stevie"}, "some-id", function(err,result){
    //save will do a full swap of the document, updateOnly will partially update
    //a document so you need to pass the id along
    //result will be true if an update happened
  });

});

```

Have a look at the tests to see a bit more


## Wanna Help?
Just do me a favor and open a PR with some ideas and hopefully a test or two. Thanks!