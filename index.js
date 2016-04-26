#!/usr/bin/env node
var log = require('single-line-log').stdout
var humanSize = require('human-size')
var Sync = require('./lib/sync')
var config = {}
var path   = require('path')
var _      = require('lodash')
var http = require('http')
var https = require('https')


    var argv = require('yargs')
        .count('verbose')
        .alias('v', 'verbose')
        .usage("s3-lls-sync <options>")
        .options({
          'd': {
            alias: 'delete-removed',
            default: false,
            describe: 'Delete removed local files',
          },
          'max-sockets': {
            default: 10,
            describe: 'Number of http parallel sockets',
            type: 'number'
          },
          'b': {
            demande: true,
            alias: 'bucket',
            describe: "The S3 bucket to use. Overrides config file bucket value if provided (i.e profs.lelivrescolaire.fr)",
            type: 'string'
          },
          'r': {
            alias: "region",
            describe: "Bucket S3 region, (default: eu-west-1)",
            default: "eu-west-1",
            type: 'string'
          },
          'ld': {
            alias: "local-dir",
            describe: "The local folder to sync. Overrides config file value",
            type: "string"
          },
          'P': {
            describe: "Declare uploaded object to be private (defulat: all public)",
            default: true,
            type: 'boolean'
          },
          'r': {
            alias: 'run',
            demand: false,
            describe: "Run in cli command"
          },
          'c': {
            alias: 'config',
            demand: true,
            describe: 'Relative (or absolute) config file path to override/extend defaults from __dirname',
            type: 'string'
          }

        })
        .argv;

    VERBOSE_LEVEL = argv.verbose;

if (argv.run ) {
  if (argv.config) {
    var configLocation = path.resolve(argv.config);
    try {
      var mergeConf = require(configLocation);
    } catch (ex) {
      console.error('Invalid config file or location! --', configLocation, '\nerr:', ex);
      process.exit(1);
    }

    config = _.merge(config, mergeConf);
  }
  if (argv['max-sockets']) {
    http.globalAgent.maxSockets = argv.maxSockets;
    https.globalAgent.maxSockets = argv.maxSockets;
  }
  if (argv.bucket) {
    config.s3Options = config.s3Options || {};
    config.s3Options.Bucket = argv.bucket;
  }

  var commandLineLocalDir = argv.localDir;
  config.localDir = commandLineLocalDir ? path.resolve(/*__dirname,*/ commandLineLocalDir) : config.localDir;
  config.VERBOSE_LEVEL = VERBOSE_LEVEL
  var syncer = new Sync(config);
  var printFn = process.stderr.isTTY ? printProgress : noop;
  printFn(syncer);

  function fmtBytes(byteCount) {
    if (byteCount <= 0) {
      return "0 B";
    } else {
      return humanSize(byteCount, 1);
    }
  }

  function noop() {}

  function printProgress(syncr) {
    var notBytes = false
  syncr.EE.on('log', function(msg) {
    console.log('\n' + msg)

  })
  syncr.EE.on('end', function(msg) {
    console.log("\nDONE")
    process.exit(1)
  })
  var start = null
  syncr.EE.on('progress', _.throttle(function() {
      var o = syncr.EE
      var percent = Math.floor(o.progressAmount / o.progressTotal * 100);
      var amt = notBytes ? String(o.progressAmount) : fmtBytes(o.progressAmount);
      var total = notBytes ? String(o.progressTotal) : fmtBytes(o.progressTotal);
      var parts = [];
      if (o.filesFound > 0 && !o.doneFindingFiles) {
        log(o.filesFound + " files")
        //parts.push(o.filesFound + " files");
      }
      if (o.doneFindingFiles) log.clear()
      if (o.objectsFound > 0 && !o.doneFindingObjects) {
        log(o.objectsFound + " objects")
        //parts.push(o.objectsFound + " objects");
      }
      if (o.doneFindingObjects) log.clear()
      if (o.deleteTotal > 0) {
        log(o.deleteAmount + "/" + o.deleteTotal + " deleted")
        //parts.push(o.deleteAmount + "/" + o.deleteTotal + " deleted");
      }
      if (o.progressMd5Amount > 0 && !o.doneMd5) {
        log((o.progressMd5Amount) + "/" + (o.progressMd5Total) + " MD5")
        //parts.push((o.progressMd5Amount) + "/" + (o.progressMd5Total) + " MD5");
      }
      if(o.doneMd5) {
        log((o.progressMd5Total) + "/" + (o.progressMd5Total) + " MD5")
        log.clear()
      }
      if (o.progressTotal > 0) {
        if (!start) start = new Date();
        var part = amt + "/" + total;
        if (!isNaN(percent)) part += " " + percent + "% done";
        parts.push(part);
        if (!notBytes) {
          var now = new Date();
          var seconds = (now - start) / 1000;
          var bytesPerSec = o.progressAmount / seconds;
          var humanSpeed = fmtBytes(bytesPerSec) + '/s';
          parts.push(humanSpeed);
        }
        log(parts.join(", "))
      }
    }, 100));
    }



}

Sync.isModule = true;
module.exports = {
  Sync: Sync
};
