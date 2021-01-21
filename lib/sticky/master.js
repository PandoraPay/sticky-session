'use strict';

var cluster = require('cluster');
var util = require('util');
var net = require('net');
var ip = require('ip');
var AsyncEvents = require('async-events');
const crypto = require('crypto');
const os = require('os');

var debug = require('debug')('sticky:master');

function Master(workerCount, options = {}) {

    net.Server.call(this, {
      pauseOnConnect: true
    }, this.balance);

    this.options = options;
    this.env = options.env || {};

    this.seed = (Math.random() * 0xffffffff) | 0;

    this.workers = [];

    this.events = new AsyncEvents();
    debug('master seed=%d', this.seed);

    this.initialized = new Promise( resolve => {

      this.once('listening', async function() {

        debug('master listening on %j', this.address());

        const array = Array.apply(null, Array(workerCount )).map( (x, i) => i )

        await Promise.all( array.map ( it => this.spawnWorker( it )) );

        resolve(true);

      });

    });

}
util.inherits(Master, net.Server);
module.exports = Master;

Master.prototype.hash = function hash(ip) {
  let hash = this.seed;
  for (let i = 0; i < ip.length; i++) {
    let num = ip[i];

    hash += num;
    hash %= 2147483648;
    hash += (hash << 10);
    hash %= 2147483648;
    hash ^= hash >> 6;
  }

  hash += hash << 3;
  hash %= 2147483648;
  hash ^= hash >> 11;
  hash += hash << 15;
  hash %= 2147483648;

  return hash >>> 0;
};

Master.prototype.spawnWorker = function spawnWorker(workerIndex) {

  return new Promise( async resolve => {

    const param = Object.assign( {
      SLAVE_INDEX: workerIndex,
      SLAVE: true,
    }, this.env);

    const worker = cluster.fork( param );

    const parts = [os.hostname(), worker.pid, +(new Date)];
    const hash = crypto.createHash('md5').update(parts.join(''));

    worker.uid = hash.digest('hex');
    worker.index = workerIndex;

    var self = this;
    worker.on('exit', function(code) {

      debug('worker=%d died with code=%d', worker.process.pid, code);

      worker._closed = true;

      return setTimeout(function () {
        self.respawn(worker);
      }, 2500);
    });

    worker.on('close', function(code){

      debug('worker=%d closed with code=%d', worker.process.pid, code);

      worker._closed = true;

    });

    worker.on('message', async msg => {

      if (!msg ) return;

      // Graceful exit
      if ( msg.type === 'close' ) {
          worker._closed = true;
          return self.respawn(worker);
      }

      try{

        await this.events.emit("message", Object.assign( {worker: worker}, msg ) );

      }catch(err){
        console.error("Error processing worker message", msg, err);
      }

      if (msg.msg === "ready-worker!" && msg.data && msg.data.result )
        return resolve(true);

    });

    debug('worker=%d spawn', worker.process.pid);
    this.workers[workerIndex] = worker;

    this.events.emit("workers", { workers: this.countWorkers() } );

  })

};

Master.prototype.respawn = async function respawn(worker) {

  let index = this.workers.length;

  for (let i=0; i < this.workers.length; i++ )
    if (this.workers[i] === worker) {
      this.workers[i] = undefined;
      index = i;
      break;
    }

  if (this.options.autoRespawn)
    return this.spawnWorker(index);

  console.log('StickyMaster: Respawn process',index);

  /**
   * let's remove the undefined workers
   */

  for (let i = this.workers.length-1; i>=0; i--)
    if ( !this.workers[i] )
      this.workers.splice(i, 1);


  await this.events.emit("workers", {workers: this.countWorkers()});

};

Master.prototype.countWorkers = function countWorkers() {

  this.workers.reduce( (res, it ) => it ? res++ : res, 0);

};

Master.prototype.balance = function balance(socket) {

  //if (this.workers.length === 0) return;
  var addr = ip.toBuffer(socket.remoteAddress || '127.0.0.1');
  var hash = this.hash(addr);

  debug('balancing connection %j', addr);

  const worker = this.workers[hash % this.workers.length];
  if (!worker)
    return console.error(`addr couldn't be balanced because the selected worker is closed`)
  worker.send('sticky:balance', socket);
};

Master.prototype._killProcess = function (pid, signal = 'SIGTERM', timeout = 10000) {

  return new Promise((resolve, reject)=>{
    process.kill(pid, signal);
    let count = 0;
    setInterval(() => {
      try {
        process.kill(pid, 0);
      } catch (e) {
        // the process does not exists anymore
        resolve();
      }
      if ((count += 100) > timeout)
        reject(new Error("Timeout process kill"))

    }, 100)
  });

};

Master.prototype.close = async function close() {

  //mark auto respawn to be disabled
  const autoRespawn = this.options.autoRespawn;

  this.options.autoRespawn = false;

  await Promise.all( this.workers.map( it => it ? this._killProcess( it.process.pid ) : false ) );

  console.log( 'StickyMaster: KILLED', this.workers.length );

  this.workers = [];

  net.Server.prototype.close.call(this);

  //revert autorespawn
  this.options.autoRespawn = autoRespawn;

};