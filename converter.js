var async = require("async")
var clusterMaster = require("cluster-master")
clusterMaster({
	size:1,
	signals:true,
	exec:'./convert-worker.js'
})