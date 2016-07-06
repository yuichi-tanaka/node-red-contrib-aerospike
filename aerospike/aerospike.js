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
};
