'use strict';

var cluster = require('cluster');
var os = require('os');
var debug = require('debug')('sticky:worker');

var sticky = require('../sticky-session');
var Master = sticky.Master;

async function listen(server, port, options = {}) {

  if (cluster.isMaster) {

      var workerCount = options.workers || os.cpus().length;

      var master = new Master(workerCount, options);
      master.listen(port);
      master.once('listening', function() {
        server.emit('listening');
      });

      return {type: "master", res: master};

  }

  // Override close callback to gracefully close server
  var oldClose = server.close;
  server.close = function close() {
      debug('graceful close');
      process.send({ type: 'close' });
      return oldClose.apply(this, arguments);
  };

  process.on('message', function(msg, socket) {

      if (msg !== 'sticky:balance' || !socket)
        return;

      debug('incoming socket');
      server._connections++;
      socket.server = server;
      server.emit('connection', socket);

  });

  return {type: "worker", };

}
exports.listen = listen;
