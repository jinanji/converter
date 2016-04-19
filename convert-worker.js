var fse = require('fs-extra');
var async = require('async');
var mongoose = require('mongoose');
var prettyBytes = require('pretty-bytes');
var redis = require("redis").createClient({host:'video2home.net'});
var exec = require('child_process').exec; 
var mimovie = require('mimovie'); 
var hb = require("handbrake-js");
var ffmpeg = require('fluent-ffmpeg');

var File = mongoose.Schema({
	reserved_for_conversion:'boolean',
	reserved_for_conversion_time:'date',
	converted:'boolean'
},{strict:false});
var schema = mongoose.Schema({
	files:[File]
},{strict:false})
var Media = mongoose.model('Media', schema);

var db_host = 'video2home.net';

var worker = process.pid + ":" ;


var file;
var sizes = [];
var sizes_files = [];
async.waterfall([
	function connectDB(fn){
		var db_host = 'video2home.net';
		mongoose.connect('mongodb://'+db_host+':27017/v2h');
		mongoose.connection.on('connected', function(){
			fn();
		});
		console.log(worker,'Connecting to DB');
	},
	function getFile(fn){
		redis.rpop('files', function(err, f){
			if(err){
				return fn(err);
			}
			file = JSON.parse(f);
			var d = Math.random().toString(36).slice(2);
			file.mnt_dir = d;
			file.save_dir = __dirname + "/temp/" + d;
			file.save_location =  file.save_dir + '/' + file.name;
			fse.ensureDir(file.save_dir, function(){
				fn();
			})
		});
	},
	function assess(fn){
		console.log('Assessing file ' + file.name);
		if("mkv avi flv mp4 m4v webm mov".indexOf(file.extension) == -1){
			fn("Invalid file for conversion");
		}else{
			fn();
		}
	},
	function reserve(fn){
		console.log(worker,'Reserving file ' + file.name);
		Media
		.update({'files._id':file._id},{$set:{'files.$.reserved_for_conversion':true,'files.$.reserved_for_conversion_time':new Date()}}, function(err){
			fn();
		})
	},
	function copyFile(fn){
		console.log(worker,'Fetching file... ' + prettyBytes(file.size));
		var ip, password;
		file.fs_location = file.fs_location.replace('/mnt','/root')
		if(file.fs_location.indexOf("20TB") != -1){
			ip = "172.16.5.1";
			password = "k0dese7en";
		}else{
			ip = "172.16.5.2";
			password = "r00t3d@s3rv1c3";
		}
		var rsync =  [
			'rsync',
			'--rsh="sshpass -p '+password+' ssh"', 
			'--partial', 
			'--progress', 
			'--inplace', 
			'root@'+ip+':' + file.fs_location,  
			'"'+file.save_location+'"'
		];		
		var cmd = rsync.join(" ")
		var dw = exec(cmd);
		dw.stdout.on('data', function(d){
			console.log(file.name+"=>",d.toString());
		})		
		dw.stderr.on('data', function(d){
			console.log(file.name+"=>",d.toString());
		})
		dw.on('close', function(){
			fn();
		})		
	},
	function determineSizes(fn){
		mimovie(file.save_location, function(err, m){
			if(err){
				return fn(err);
			}
			if(!m.video){
				return fn("Unable to get information from " + file.name);
			}
			var stream = m.video.pop();
			if(!stream || !stream.height){
				return fn("Invalid video file " + file.name);
			}
			var h = stream.height;
			var resolutions = [720, 480];
			resolutions.reverse().forEach(function(r){
				if(h>=r){
					sizes.push(r);
				}
			});
			if(!sizes.length){
				sizes.push(480);
			}
			fn();
		});
	},
	function convert(fn){
		async.eachSeries(sizes, function(size, done){
			var nm  = file.name.split(".");
			nm = nm[0] + ".mp4";
			var fnm =  size + nm;
			var out = file.save_dir + "/" + fnm;
			sizes_files.push(fnm);
			// hb.spawn({
			// 	input:file.save_location, 
			// 	output:out
			// })
			// .on('error',done)
			// .on('progress', function(progress){
			// 	console.log("%s (%s) %s\t\t%s% | ETA: %s | FPS: %s", worker, size, file.name, progress.percentComplete, progress.eta, progress.fps);
			// })
			// .on('end', function(progress){
			// 	console.log(worker, file.name, size, "Encoding completed");
			// })
			// .on('complete', function(){
			// 	done();
			// });
			ffmpeg()
			.input(file.save_location)
			.outputOptions([
				'-c:v libx264',
				'-crf 23',
				'-threads 0',
				'-acodec libmp3lame',
				'-ar 44100',
				'-ab 128k',
				'-ac 2',
				'-movflags +faststart',
			])
			.size('?x'+size)
			.format('mp4')
			.save(out)
			.on('start', function(c){
				console.log(c);
			})
			.on('progress', function(progress) {
				console.log(progress);
			})
			.on('end', function(e){
				console.log(worker, file.name, size, "Encoding completed");
				done();
			});
		}, fn);		
	},
	function clean(fn){
		fse.removeSync(file.save_location);
		fn();
	},
	function determineDriveWithFreeSpaceAndUpload(fn){
		var ip = "172.16.5.1";
		var password = "k0dese7en";
		file.mnt_location = "/mnt/20TB/d3/media/" + file.mnt_dir;
		var rsync =  [
			'rsync',
			'--rsh="sshpass -p '+password+' ssh"',  
			'"'+file.save_dir+'"',
			'-r',
			'root@'+ip+':/root/20TB/d3/media',  
		];		
		var cmd = rsync.join(" ")
		var dw = exec(cmd);
		console.log(cmd);
		dw.stdout.on('data', function(d){
			console.log(file.name+"=>",d.toString());
		})		
		dw.stderr.on('data', function(d){
			console.log(file.name+"=>",d.toString());
		})
		dw.on('close', function(){
			fn();
		})			
	},
	function clean(fn){
		console.log(worker, 'removing local data')
		fse.removeSync(file.save_dir);
		fn();
	},
	function save(fn){
		console.log('saving db')
		Media
		.findOne({'files._id':file._id},{'files.$':1})
		.lean()
		.exec(function(err, m){
			if(m.files.length > 1){
				return fn("error, unexpected result, aborting");
			}
			var f = m.files.pop();
			f.converted = true;
			f.sizes = sizes;
			f.sizes_files = sizes_files;
			f.sizes_dir = file.mnt_location;
			Media.update({'files._id':file._id},{$set:{'files.$':f}}, function(err, c){
				if(err){
					return fn(err);
				}
				console.log(worker, 'successfully saved:' + file._id);
				fn();
			});
		})
		
	},
	function finish(fn){
		console.log(worker, 'done');
		fn();
	},
], function(err){
	if(err){
		console.log(worker, err);
	}
	console.log(worker, "dying...");
	process.exit();
})