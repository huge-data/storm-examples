var http = require('http');
var Static = require('node-static');
var app = http.createServer(handler);
var io = require('socket.io').listen(app);
var redis = require('redis');
client = redis.createClient();
clientSend = redis.createClient();
var port = 8081;

var files = new Static.Server('./public');

function handler (request, response) {
	request.on('end', function() {
		files.serve(request, response);
	}).resume();
}


client.on("message", function (channel, message) {
    console.log("client channel " + channel + ": " + message);
    clientSend.emit('get', message)
});

client.subscribe("tweets");


// delete to see more logs from sockets
io.set('log level', 1);

io.sockets.on('connection', function (socket) {
	
	//where is the user
	socket.on('send:coords', function (data) {
		socket.broadcast.emit('load:coords', data);
	});
	
	//Emit tweets
	clientSend.on('get', function (message){
		for(var i = 1; i<=message; i++){   
			clientSend.hgetall("count:"+i, function (err, obj) {
	    			console.dir(obj);
				var location = {}
				location.lng = obj.long;
				location.lat = obj.lat;
				socket.emit('load:coords', location, obj.count, obj.text);
			}); 
	        }
		
	});
	
	

});

// start app on specified port
app.listen(port);
console.log('Your server goes on localhost:' + port);
