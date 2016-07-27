module.exports = function(RED){
  /**
   * Write to Aerospike
   * Parameters:
   * - servers (example: servers="192.168.33.10:3000,192.168.33.11:3000")
   * - namespace
   * - set
   */
  function writeToAerospike(config){
    RED.nodes.createNode(this,config);
    var node = this;
    var as = require('aerospike');
    var Key = as.Key;
    var asConf = {
      hosts: config.servers
    }
    var namespace = config.namespace;
    var set = config.set;
    var c = null;
    try{
      this.on("input",function(msg){
        var payloads = [];
        var meta = {};
        var policy = { exists: as.policy.exists.CREATE_OR_REPLACE };
        if(msg.key === ""){
          //check the key
          node.error('key is empty');
          return;
        }
        var k = new Key(namespace,set,msg.key);
        var rec = msg.payload;
        if(typeof rec !== "object"){
          //check the object
          node.log('payload nust be an object:',rec);
          try{
            //try to parse
            rec  = JSON.parse(msg.payload);
            node.log('parse string to json');
          }catch(e){
            //parse error
            node.error('parse error: string to json');
            return;
          }
        }
        c.put(k,rec,meta,policy,function(e){
          //put the recode
          if(e) node.error(e);
          node.log("put recode key: " + msg.key);
          node.log("put recode: " + JSON.stringify(rec));
        });
      });
    } catch(e) {
          console.log(e);
      node.error(e);
    }

    /**
     * connect to Aerospike
     */
    as.connect(asConf,function(error,connection){
      if (error) node.error(error);
      c = connection;
    });
  };
  RED.nodes.registerType("aerospike",writeToAerospike);
  /**
   * read from Aerospike By key
   */
  function readFromAerospikeByKey(config){
    RED.nodes.createNode(this,config);

    var node = this;
    var as = require('aerospike');
    var Key = as.Key;
    var asConf = {
      hosts: config.servers
    }
    var namespace = config.namespace;
    var set = config.set;
    try{
      this.on('input',function(msg){
        //create Key
        console.log(msg.key);
        var key = new Key(namespace,set,msg.key);
        c.get(key,function(er,rec,meta){
          if(er && er.code === 2){
            //recode not found
            console.log('recode not found: key = ',msg.key);
          }else if(er){
            console.error('aerospike error: code = ',er.code,', key = ',msg.key);
            node.error(er);
          }
          var r = { payload: rec, meta: meta};
          node.send(r);
        });
      });
    } catch(e) {
      console.log(e);
      node.error(e);
    }
    /**
     * connect to Aerospike
     */
    as.connect(asConf,function(error,connection){
      if (error) node.error(error);
      c = connection;
    });

  };
  RED.nodes.registerType("fromAerospikeByKey",readFromAerospikeByKey);
  /**
   * read from Aerospike
   */
  function readFromAerospike(config){
    RED.nodes.createNode(this,config);

    var node = this;
    var as = require('aerospike');
    var asConf = {
      hosts: config.servers
    }
    var namespace = config.namespace;
    var set = config.set;
    var select = config.select;
    try{
      this.on("input",function(msg){
        var query = c.query(namespace,set);
        if(select && select !== ""){
          if(select.indexOf(",") > 0){
            //split select fields
            var _select = select;
            select = _select.split(",");
          }
          //check the select
          query.select(select);
        }
        if(msg.range && msg.range !== ""){
          console.log(msg.range);
          var range = msg.range;
          var key = range.key;
          var start = range.start;
          var end = range.end;
          query.where(Aerospike.filter.range(key, start, end))
        }
        if(msg.equal && msg.equal !== ""){
          var equal = msg.equal;
          query.where(Aerospike.filter.equal(equal.key, equal.value))
        }
        if(msg.contains && msg.contains !== ""){
          query.where(Aerospike.filter.contains(msg.contains))
        }
        try{
        var stream = query.foreach();
        var result = [];
        stream.on('error',function(e){
          if(e && e.code === 2){
            console.log('recode not found');
          } else {
            console.error(e);
            throw e;
          }
        }).on('data',function(rec){
          console.log(rec);
          result.push(rec);
        }).on('end',function(){
          var r = {payload: result};
          console.log('done');
          node.send(r);
        });
        }catch(e){
          console.error(e);
          node.error(e);
        }
      });
    } catch(e) {
      console.log(e);
      node.error(e);
    }

    /**
     * connect to Aerospike
     */
    as.connect(asConf,function(error,connection){
      if (error) node.error(error);
      c = connection;
    });
  }
  RED.nodes.registerType("fromAerospike",readFromAerospike);
};
