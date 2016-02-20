var async = require('async')
var hyperlog = require('hyperlog')
var merkledag = require('ipfs-dag')
var defined = require('defined')

module.exports = function (db, opts) {

  opts = defined(opts, {
    hashFunction: undefined,
    asyncHashFunction: hash
  })

  // The inner hyperlog object.
  var log = hyperlog(db, opts)

  // Hash an opaque value blob and a list of links to an IPFS multihash.
  function hash (links, value, cb) {
    convertToMerkleDagNode(value, links, doneConversion)

    function doneConversion (err, dagNode) {
      if (err) return cb(err)

      cb(null, dagNode.multihash)
    }
  }

  function keyToMerkleDagNode (key, cb) {
    // Look up the node by its key and convert it recursively.
    log.get(key, function (err, node) {
      if (err) return cb(err)

      convertToMerkleDagNode(node.value, node.links, cb)
    })
  }

  // Take an opaque value and list of links and conver them into an IPFS Merkle
  // DAG node (recursively).
  function convertToMerkleDagNode (value, links, cb) {
    // Base class: no links.
    if (!links || links.length <= 0) {
      var dagNode = new merkledag.Node(value)
      process.nextTick(function () {
        cb(null, dagNode)
      })
      return
    }

    // Retrieve each key as a hyperlog node.
    async.map(links, keyToMerkleDagNode, onLinksReady)

    function onLinksReady (err, nodes) {
      if (err) return cb(err)

      // Convert each Merkle DAG node to a Merkle DAG link.
      nodes = nodes.map(function (node) {
        return node.asLink('')
      })

      // Create the final Merkle DAG node.
      var dagNode = new merkledag.Node(value, nodes)
      cb(null, dagNode)
    }
  }

  return log
}
