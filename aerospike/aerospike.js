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
        var policy = { exists: as.policy.exsists.CREATE_OR_REPLACE };
        var k = new Key(namespace,set,msg.key);
        var rec = JSON.parse(msg.payload);
        c.put(k,rec,meta,policy,function(e){
          if(e) node.error(e);
          node.log("put recode key: " + msg.key);
          node.log("put recode: " + JSON.stringify(rec));
        });
      });
    } catch(e) {
      node.error(e);
    }
    /**
     * connect to Aerospike
     */
    as.connect(asConf,function(error,connection){
      if (error) node.error(error);
      c = connections;
    });
  };
  RED.nodes.registerType("Aerospike",writeToAerospike);
};
