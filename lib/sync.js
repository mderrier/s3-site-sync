'use strict';

var AWS = require('aws-sdk'),
	fs = require('fs'),
	Promise = require('bluebird'),
	_ = require('lodash'),
	shortid = require('shortid'),
	path = require('path'),
	fileMagik = require('file-magik'),
	mime = require('mime');
var md5File = require('md5-file')
var diff = require('deep-diff')
var maxKeys = 1000
var EventEmitter2 = require('eventemitter2').EventEmitter2;
var Progress = require('progressbar-stream')
var pgstream = require('progress-stream')
var log = require('single-line-log').stdout;
var S3FS = require('s3fs');
var retry = require('bluebird-retry')
const loudRejection = require('loud-rejection');

// Install the unhandledRejection listeners
loudRejection();

function WARN()  { VERBOSE_LEVEL >= 0 && console.log.apply(console, arguments); }
function INFO()  { VERBOSE_LEVEL >= 1 && console.log.apply(console, arguments); }
function DEBUG() { VERBOSE_LEVEL >= 2 && console.log.apply(console, arguments); }

module.exports = Sync;
var S3 = null

/**
 * Sync with the given config.
 * @param config                            {object}    The configuration options
 * @param config.s3Options                  {object}    S3 options
 * @param config.s3Options.accessKeyId      {string}    S3 access key id
 * @param config.s3Options.secretAccessKey  {string}    S3 secret access key
 * @param config.s3Options.region           {string}    S3 region
 * @param config.maxAsyncS3                 {number}    Maximum number of simultaneous requests this client will ever have open to S3.
 *                                                          Defaults to 20.
 * @param config.s3RetryCount               {number}    How many times to try an S3 operation before giving up. Default 3.
 * @param config.s3RetryDelay               {number}    How many milliseconds to wait before retrying an S3 operation. Default 1000.
 * @param config.multipartUploadThreshold   {number}    If a file is this many bytes or greater, it will be uploaded via a multipart
 *                                                          request. Default is 20MB. Minimum is 5MB. Maximum is 5GB
 * @param config.multipartUploadSize        {number}    When uploading via multipart, this is the part size. The minimum size is 5MB. The
 *                                                          maximum size is 5GB. Default is 15MB. Note that S3 has a maximum of 10000 parts
 *                                                          for a multipart upload, so if this value is too small, it will be ignored in
 *                                                          favor of the minimum necessary value required to upload the file.
 */
function Sync(config) {
	var self = this;
	this.EE = new EventEmitter2({
	      wildcard: true,
	      delimiter: '.',
	      newListener: false,
	      maxListeners: 20
	    });
	this.config = _.merge(require('../config'), config);
	// console.log('config:', require('util').inspect(this.config, {depth: null, colors:true}));
	this.AWS = AWS;
	this.AWS.config.update(this.config.s3Options);
	this.S3 = S3 = new this.AWS.S3();
	this.cloudfront = new this.AWS.CloudFront({apiVersion: '2015-04-17'});
	this.fsImpl = new S3FS(this.config.s3Options.Bucket, this.config.s3Options)
	this.bucketWebsite = {}
	this.md5Files = {}
	this.remoteFiles = {}
	this.toDelete = {}
	this.toCreateOrUpdate = {}
	if (this.config.ensureBucketWebsite) {
		this.bucketWebsiteUrl = this.config.s3Options.Bucket + '.s3-website-' + this.config.s3Options.region + '.amazonaws.com';
	}

	var params = {
		localDir: this.config.localDir,
		deleteRemoved: this.config.deleteRemoved,
		s3Params: {
			Bucket: this.config.s3Options.Bucket,
			Prefix: this.config.s3Options.Prefix || '',
			ACL: this.config.s3Options.ACL || 'public-read'
		}
	};
	this.params = params
	self.ensureBucketExists()
					.then(self.ensureBucketWebsite.bind(self))
					.then(self.ensureDistributionExists.bind(self))
					.then(self.ensureDistributionDefaultRootObj.bind(self))
					.then(self.localFiles.bind(self))
					//.then(self.bucketFiles.bind(self))
					.then(self.listKeysAsync.bind(self))
					.then(self.computeDifferences.bind(self))
				  .then(self.uploadToS3.bind(self))
				  .then(self.deleteRemovedToS3.bind(self))
				  .then(function(filePaths) {
						if (Sync.isModule) {
							if (self.cloudfrontDistribution) {
					      var aliases = 'n/a';
					      if (self.cloudfrontDistribution.Aliases && self.cloudfrontDistribution.Aliases.Quantity) {
					        aliases = self.cloudfrontDistribution.Aliases.Items;
					      }
					      // console.log('\nDistribution setting summary: ',
					      // 	'\nUploaded Files:\n', uploadedFiles.map(function(fileObj) {
					      // 	 return fileObj.fullKey
					      // 	 }),
					      // 	'\nDistribution URL:', self.bucketWebsiteUrl,
					      // 	'\nDistribution Id:', self.cloudfrontDistribution.Id,
					      // 	'\nDistribution website:', self.cloudfrontDistribution.DomainName,
					      // 	'\nAliases:\n', aliases);
					    }
							var data = {
					      bucket: self.bucket,
					      bucketWebsite: self.bucketWebsite,
					      bucketWebsiteUrl: self.bucketWebsiteUrl,
					      cloudfrontDistribution: self.cloudfrontDistribution,
					      uploadedFiles: filePaths
					    }
							self.EE.emit("end", data)
					    return data
					  } else {
							process.exit(1)
						}
					}).catch(function (err) {
						INFO('Error occurred during sync:', err);
						if (self.isModule) {
							return reject(err);
						}
						//process.exit(1);
					});
}

Sync.prototype.ensureBucketExists = function () {
	var self = this;
	DEBUG("ensureBucketExists")
	return this.S3.listBuckets().promise().then(function (response) {
		DEBUG(`All buckets:\n ${_.map(response.Buckets, 'Name')}`);
		if (_.find(response.Buckets, {Name: self.config.s3Options.Bucket})) {
			INFO('Bucket exist!');
			return response;
		}
		INFO('Bucket does not exist, creating it!');
		return self.S3.createBucket({
			Bucket: self.config.s3Options.Bucket,
			ACL: 'public-read'
		}).promise().then(function(response) {
			DEBUG(response)
			DEBUG(`Bucket:\n `);
		})
	});
};

Sync.prototype.ensureBucketWebsite = function () {
		var self = this;
		DEBUG("ensureBucketWebsite")
		if (!self.config.ensureBucketWebsite) {
			return ;
		}
		DEBUG('call s3 getBucketWebsite')
	return self.S3.getBucketWebsite({Bucket: self.config.s3Options.Bucket}).promise().then(function (response) {
		return response;
	}).catch(function (err) {
		if (err.code === 'NoSuchWebsiteConfiguration') {
			WARN('No current bucket website!');
			return {};
		}
		//throw err;
	}).then(function (bucketWebsite) {

		DEBUG('Current bucket website:\n', require('util').inspect(bucketWebsite, {depth: null, colors: true}));
		bucketWebsite = _.merge(
			bucketWebsite,
			self.config.bucketWebsite
		);

		if (self.config.DefaultRootObject) {
			bucketWebsite.IndexDocument.Suffix = self.config.DefaultRootObject;
			bucketWebsite.ErrorDocument.Key = self.config.DefaultRootObject;
		}

		// if (bucketWebsite.RoutingRules && bucketWebsite.RoutingRules[0] && !bucketWebsite.RoutingRules[0].Redirect.HostName) {
		// 	bucketWebsite.RoutingRules[0].Redirect.HostName = self.bucketWebsiteUrl;
		// }

		self.bucketWebsite = {Bucket: self.config.s3Options.Bucket, WebsiteConfiguration: bucketWebsite};
		DEBUG('\nEnsure website settings -- PUT bucket website:\n',
			require('util').inspect(self.bucketWebsite.WebsiteConfiguration, {depth: null, colors: true}));
		// TODO: Set ContentMD5 in response for added security
		return self.S3.putBucketWebsite(self.bucketWebsite).promise();
	});
};

Sync.prototype.ensureDistributionExists = function () {
	var self = this;
	DEBUG("ensureDistributionExists")

	if (!self.config.ensureDistribution) {
			return
	}
	return self.cloudfrontFn('listDistributions', {}).then(function (response) {
		var distribution = self.getDistribution(response.data.Items);
		if (!distribution) {
			return self.createDistribution();
		}
		DEBUG('\nDistribution found, full current settings:\n', require('util').inspect(distribution, {depth: null, colors: true}));
		return distribution;
	}).then(function (cloudfrontDistribution) {
		DEBUG('\nDistribution setting summary: "', self.bucketWebsiteUrl,
			'" --\ndistributionId:', cloudfrontDistribution.Id, ', distribution website:', cloudfrontDistribution.DomainName);
		self.cloudfrontDistribution = cloudfrontDistribution;
	});
};

Sync.prototype.getDistribution = function getDistribution(distributions) {
	DEBUG("getDistribution")

	var self = this,
		distribution = null;

	distributions.forEach(function (distro) {
		var exists = _.find(distro.Origins.Items, {
			DomainName: self.bucketWebsiteUrl
		});
		if (exists) {
			distribution = distro;
		}
	});
	return distribution;
};

Sync.prototype.createDistribution = function () {
	DEBUG("createDistribution")

	var self = this
	if (!self.config.ensureDistributionDefaultRootObj) {
		return;
	}
	var distributionConfig = self.config.cloudfrontDistribution.DistributionConfig;

	distributionConfig.CallerReference = shortid.generate();
	distributionConfig.DefaultCacheBehavior.TargetOriginId = 'Custom-' + self.bucketWebsiteUrl;
	distributionConfig.Origins.Items[0].DomainName = self.bucketWebsiteUrl;
	distributionConfig.Origins.Items[0].Id = 'Custom-' + self.bucketWebsiteUrl;
	// OriginPath must start with slash, and not have trailing slash
	distributionConfig.Origins.Items[0].OriginPath = '/' +
		(self.config.s3Options.Prefix || '').replace(/^(\\|\/)/, '').replace(/(\\\/)*$/, '');
	DEBUG('Creating distribution with these settings:\n', require('util').inspect(self.config.cloudfrontDistribution,
		{depth: null, colors: true}));
	return self.cloudfrontFn('createDistribution', self.config.cloudfrontDistribution).then(function (response) {
		return response.data;
		//TODO: Set up a `waitFor` call to cloudfront to notify when distribution is ready? (Can take up to 25 minutes)
	});
};

Sync.prototype.ensureDistributionDefaultRootObj = function () {
	var self = this;

	DEBUG("ensureDistributionDefaultRootObj")
	if (!self.config.ensureDistributionDefaultRootObj) {
		return;
	}
	return self.cloudfrontFn('getDistribution', {Id: self.cloudfrontDistribution.Id}).then(function (response) {
		var distribution = response.data,
			bucketPrefix = self.config.s3Options.Prefix || '',
			configDefaultRootObj = self.config.DefaultRootObject;

		if (distribution.DistributionConfig.Comment === null) {
			distribution.DistributionConfig.Comment = '';
		}
		if (distribution.DistributionConfig.Logging.Enabled === false) {
			distribution.DistributionConfig.Logging.Bucket = '';
			distribution.DistributionConfig.Logging.Prefix = '';
		}

		if (distribution.DistributionConfig.Origins.Items instanceof Array &&
			distribution.DistributionConfig.Origins.Items[0].S3OriginConfig &&
			distribution.DistributionConfig.Origins.Items[0].S3OriginConfig.OriginAccessIdentity === null) {
			distribution.DistributionConfig.Origins.Items[0].S3OriginConfig.OriginAccessIdentity = '';
		}

		if (distribution.DistributionConfig.DefaultRootObject === configDefaultRootObj &&
			distribution.DistributionConfig.Origins.Items[0].OriginPath === bucketPrefix) {
			DEBUG('Distribution default root object (', configDefaultRootObj, ') not changed, no update needed');
			DEBUG('Distribution origin path (', bucketPrefix, ') not changed, no update needed');
			return distribution;
		}

		distribution.DistributionConfig.DefaultRootObject = configDefaultRootObj;
		// OriginPath must start with slash, and not have trailing slash
		distribution.DistributionConfig.Origins.Items[0].OriginPath = '/' + bucketPrefix.replace(/^(\\|\/)/, '').replace(/(\\\/)*$/, '');
		return self.cloudfrontFn('updateDistribution', {
			Id: distribution.Id,
			IfMatch: distribution.ETag,
			DistributionConfig: distribution.DistributionConfig
		}).then(function (response) {
			DEBUG('Default cloudfront distribution (Id:', distribution.Id, ') default root object updated to:', configDefaultRootObj);
			return distribution;
		});
	});
};

Sync.prototype.localFiles = function() {
	var self = this
	DEBUG("localFiles")
	if (self.config.noUpload) {
		return {
			bucket: self.bucket,
			bucketWebsite: self.bucketWebsite,
			bucketWebsiteUrl: self.bucketWebsiteUrl,
			cloudfrontDistribution: self.cloudfrontDistribution,
			uploadedFiles: []
		};
	}
	return new Promise(function(resolve, reject) {
		try {
			var filePaths = fileMagik.get(self.params.localDir, {excludeDirectories: ['.s3sync', '.git'], recursive: true, extension: null})
			var started = 0;
		 	self.md5Files = {}
			self.EE.filesFound = filePaths.length
			self.EE.doneFindingFiles = true
			self.EE.emit('progress')
		//	self.EE.emit("log", `${filePaths.length} local objects found`)
			filePaths.forEach(function(item, i) {
				self.EE.progressMd5Amount = _.size(self.md5Files)
				self.EE.progressMd5Total = filePaths.length

				let key = item.split(self.params.localDir)[1].replace(/\\/g, '/').replace(/^\//, '');
				self.md5Files[key] = `"${md5File(item)}"`
				self.EE.emit('progress')
			})
			self.EE.doneMd5 = true
			self.EE.emit('progress')

			resolve(self.md5Files)
		} catch(e) {
			return reject(e)
		}
	})
}

Sync.prototype.computeDifferences = function() {
	var self = this
	DEBUG("computeDifferences")

	return new Promise(function(resolve, reject){
		try {
			var toDelete = []
			var toCreateOrUpdate = []
			var differences =	diff(self.remoteFiles, self.md5Files)

			if (self.config.deleteRemoved === true) {
				toDelete = _.filter(differences, {kind: "D"})
			}
			toCreateOrUpdate = _.reject(differences, {kind: "D"})
			self.toDelete = toDelete
			self.toCreateOrUpdate = toCreateOrUpdate
			//self.EE.emit("log", `${toDelete.length} objects to delete, ${toCreateOrUpdate.length} objects to create or update`)

			resolve({toDelete: toDelete, toCreateOrUpdate: toCreateOrUpdate})
		} catch (e) {
			 return reject(e)
		}
	})
}

Sync.prototype.uploadToS3 = function() {
	var self = this

	if (_.size(self.toCreateOrUpdate) == 0) {
		self.EE.emit('log', 'Nothing to create or update')
		return {}
	}
	var started = 0
	var completed = 0
	var failed = 0
	var computeTotalSizeToSend = _.size(self.toCreateOrUpdate) ? self.toCreateOrUpdate.map(item => item.path[0]).reduce((pv, cv) => {
		if (typeof pv == 'string') pv = fs.statSync(path.resolve(self.params.localDir, pv)).size
		return pv + fs.statSync(path.resolve(self.params.localDir, cv)).size
	}) : 0
	self.EE.progressTotal = computeTotalSizeToSend
	self.EE.progressAmount = 0
	return Promise.map(self.toCreateOrUpdate, function uploadFile(obj){
		var filePath = path.resolve(self.params.localDir, obj.path[0])
		var key = `${self.params.s3Params.Prefix}/${obj.path[0]}`
		key = key.replace(/^\//, '')
		var length = fs.statSync(filePath).size;
		var input = fs.createReadStream(filePath)

		let s3Object = Object.assign({}, _.omit(self.params.s3Params, 'Prefix'), {
			ACL: self.config.s3Options.ACL,
			Bucket: self.config.s3Options.Bucket,
			Body: input,//.pipe(str),
			ContentType: mime.lookup(filePath, 'application/octet-stream'),
			Key: key
		});
		DEBUG('uploading '+key);
		var uploadS3 = function() {
			return new Promise(function(resolve, reject) {
					var test = S3.putObject(s3Object, function (err, data) {
					 if (err) {
							 //INFO(err);
							 if (err.retryable == false) throw new retry.StopError('ERROR')
							 return reject(err);
					 } else {
							 DEBUG('uploaded '+key, data, self.md5Files[key]);
							 self.EE.progressAmount += length
	 						 self.EE.emit("progress")
							 return resolve(data.Location);
					 }
					});
					// var prevBytes = 0
					// test.on('httpUploadProgress', function(progress) {
					// 	if (progress.loaded) {
					// 		var delta = progress.loaded - prevBytes
					// 		prevBytes = progress.loaded
					// 	self.EE.progressAmount += delta
					// 	self.EE.emit("progress")
					// }
					// });
				})
		}
		return retry(uploadS3, { max_tries: 5, interval: 2000, backoff: 2 })

	}, {concurrency: self.config.s3Options.concurrency})
}

Sync.prototype.deleteRemovedToS3 = function() {
		var self = this;
		if (_.size(self.toDelete) == 0 ) {
			self.EE.emit('log', 'Nothing to delete')
			return {}
		}
		return Promise.map(self.toDelete, function deleteFile(obj){
				return new Promise(function (resolve, reject) {
					S3.deleteObject({
							Bucket: self.config.s3Options.Bucket,
							Key: obj.path[0]
					}, function (err, data) {
							if (err) {
								console.log('NOT deleted ', obj.path[0])
								return reject(err);
							}
							console.log('deleted ', obj.path[0])
							resolve(data);
					});
			});
	});
}

Sync.prototype.bucketFiles = function() {
		var self = this
		console.log('bucketFiles')
		return this.fsImpl.readdirp().then(files => {console.log(JSON.stringify(files)); process.exit(1)}).catch(e => {console.log(JSON.stringify(e))})
}

Sync.prototype.listKeysAsync = function () {
	var self = this
	let s3HeadParams = Object.assign({}, {
		Bucket: self.config.s3Options.Bucket,
		Prefix: self.config.s3Options.Prefix,
		Delimiter: self.config.s3Options.Delimiter
	});
	return new Promise(function(resolve, reject) {
		listKeys.call(self, s3HeadParams, function(err, data) {
			if (err !== null) return reject(err)
			self.remoteFiles = data
			self.EE.objectsFound = data.length
			self.EE.doneFindingObjects = true
			self.EE.emit('progress')
			resolve(data)
		})
	})
}

function listKeys (options, callback) {
  var keys = {};
	var self = this
  /**
   * Recursively list keys.
   *
   * @param {String|undefined} marker - A value provided by the S3 API
   *   to enable paging of large lists of keys. The result set requested
   *   starts from the marker. If not provided, then the list starts
   *   from the first key.
   */
  function listKeysRecusively (marker) {
    options.Marker = marker;

    listKeyPage(
      options,
      function (error, nextMarker, keyset) {
        if (error) {
          return callback(error, keys);
        }
				keys = Object.assign(keys, keyset)
				self.EE.objectsFound = _.size(keys)
				self.EE.emit('progress')
        if (nextMarker) {
          listKeysRecusively(nextMarker);
        } else {
          callback(null, keys);
        }
      }
    );
  }

  // Start the recursive listing at the beginning, with no marker.
  listKeysRecusively();
}

/**
 * List one page of a set of keys from the specified bucket.
 *
 * If providing a prefix, only keys matching the prefix will be returned.
 *
 * If providing a delimiter, then a set of distinct path segments will be
 * returned from the keys to be listed. This is a way of listing "folders"
 * present given the keys that are there.
 *
 * If providing a marker, list a page of keys starting from the marker
 * position. Otherwise return the first page of keys.
 *
 * @param {Object} options
 * @param {String} options.bucket - The bucket name.
 * @param {String} [options.prefix] - If set only return keys beginning with
 *   the prefix value.
 * @param {String} [options.delimiter] - If set return a list of distinct
 *   folders based on splitting keys by the delimiter.
 * @param {String} [options.marker] - If set the list only a paged set of keys
 *   starting from the marker.
 * @param {Function} callback - Callback of the form
    function (error, nextMarker, keys).
 */
function listKeyPage (options, callback) {
  var params = {
    Bucket : options.Bucket,
    Delimiter: options.Delimiter,
    Marker : options.Marker,
    MaxKeys : maxKeys,
    Prefix : options.Prefix
  };

  S3.listObjects(params, function (error, response) {
    if (error) {
      return callback(error);
    } else if (response.err) {
      return callback(new Error(response.err));
    }

    // Convert the results into an array of key strings, or
    // common prefixes if we're using a delimiter.
    var keys = {};
    response.Contents.forEach(function (item) {
			let key = item.Key.replace(/^\/+/, '')
			keys[key] = item.ETag;
    });

    // Check to see if there are yet more keys to be obtained, and if so
    // return the marker for use in the next request.
    var nextMarker;
    if (response.IsTruncated) {
      if (options.Delimiter) {
        // If specifying a delimiter, the response.NextMarker field exists.
        nextMarker = response.NextMarker;
      } else {
        // For normal listing, there is no response.NextMarker
        // and we must use the last key instead.
				nextMarker = response.Contents[response.Contents.length - 1].Key;
      }
    }

    callback(null, nextMarker, keys);
  });
}



Sync.prototype.s3Fn = function s3Fn(methodName, opts, opts2) {
	return this.promisifyAWS('S3', methodName, opts, opts2);
};

Sync.prototype.cloudfrontFn = function cloudfrontFn(methodName, opts, opts2) {
	return this.promisifyAWS('cloudfront', methodName, opts, opts2);
};

Sync.prototype.promisifyAWS = function (awsAPI, methodName, opts, opts2) {
	var self = this;
	var args = arguments;
	return new Promise(function (resolve, reject) {
		var fn;
		if (args.length < 4) {
			fn = self[awsAPI][methodName](opts);
		}
		else if (args.length === 4) {
			fn = self[awsAPI][methodName](opts, opts2);
		}// needed for `upload` and `getSignedUrl` S3 methods
		else {
			throw new Error('Unknown/unhandled method with argument length greater than 3');
		}

		fn.on('success', function (response) {
			if (!self.config.enableBucketCORS || awsAPI !== 'S3' || (self.config.enableBucketCORS && methodName !== 'createBucket')) {
				return resolve(response);
			}

			// Make sure all buckets created have CORS support for uploading files with pre-signed URL
			self.S3.putBucketCors({
				Bucket: opts.Bucket,
				CORSConfiguration: {
					CORSRules: [{
						AllowedOrigins: ['*'],
						AllowedHeaders: ['*'],
						AllowedMethods: ['GET', 'PUT', 'DELETE', 'POST'],
						MaxAgeSeconds: 30000
					}]
				}
			}).on('success', resolve).on('error', reject).send();
		}).on('error', reject).send();
	});
};

/**
 * Run a generator function
 * @param gen
 */
function run(gen) {
	var iter = gen(function (err, data) {
		if (err) { iter.throw(err); }
		return iter.next(data);
	});
	iter.next();
}
