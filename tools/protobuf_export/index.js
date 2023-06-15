const protobufjs = require('protobufjs');

function buildRoot() {  
  var root;
  const files = require.context('./protos', true, /\.proto$/)
  files.keys().forEach((key) => {
    const file = files(key);
    const fileContent = file.default;
    root = protobufjs.parse(fileContent, root).root;
  });
  return root;
}

var chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=';

btoa = function(input) {
   var str = String(input);
   for (
       // initialize result and counter
       var block, charCode, idx = 0, map = chars, output = [];
       //   change the mapping table to "="
       //   check if d has no fractional digits
       str.charAt(idx | 0) || (map = '=', idx % 1);
       // "8 - idx % 1 * 8" generates the sequence 2, 4, 6, 8
       output.push(map.charAt(63 & block >> 8 - idx % 1 * 8))
   ) {
       charCode = str.charCodeAt(idx += 3 / 4);
       if (charCode > 0xFF) {
           throw Error("'btoa' failed: The string to be encoded contains characters outside of the Latin1 range.");
       }
       block = block << 8 | charCode;
   }
   return output.join("");
};
var root = buildRoot()
const proto_parser_cache = {};
function setup(protoMessage){
	// Check the cache to see if the object already exists
	const proto_key = protoMessage
	if (proto_parser_cache[proto_key]) {
		return proto_parser_cache[proto_key];
	}
	
	// Obtain a message type
	var messageProto = root.lookupType(protoMessage);
	// Add it to cache
	proto_parser_cache[proto_key] = messageProto;

	return proto_parser_cache[proto_key];
}

function parse(messageProto, input){
	// Verify the payload if necessary (i.e. when possibly incomplete or invalid)
	var errMsg = messageProto.verify(input);
	if (errMsg)
	   throw Error(errMsg);

	// Create a new message
	var message = messageProto.create(input);

	// Encode a message to an Uint8Array
	var buffer = messageProto.encode(message).finish();
	return btoa(buffer.reduce((arr, byte) => {arr.push(String.fromCharCode(byte)); return arr;}, []).join(""));
}

module.exports = {setup: setup, parse: parse};

