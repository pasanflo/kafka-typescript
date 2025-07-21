import net from "net";
// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
const server: net.Server = net.createServer((connection: net.Socket) => {
  // Handle connection
  console.log("Connection established");
  console.log(connection);
  connection.on("data", (data) => {
    console.log("Data received");
    console.log(data);
    const response = initResponse();
    response.correlationId = data.readInt32BE(8);
    const buffer = response.toBuffer();
    connection.write(buffer);
    connection.end();
  });
});

function initResponse() {

  const response = {
    correlationId: 0,
    toBuffer():Buffer {
      const mesageSizeBytes = 4;
      const correlationIdBytes = 4;
      const totalBytes = mesageSizeBytes + correlationIdBytes;
      const message_size_buffer = Buffer.alloc(mesageSizeBytes);
      message_size_buffer.writeInt32BE(totalBytes, 0);
      const correlation_id_buffer = Buffer.alloc(correlationIdBytes);
      correlation_id_buffer.writeInt32BE(this.correlationId, 0);
      return Buffer.concat([message_size_buffer, correlation_id_buffer]);
    }
  };
  return response;
}
//
server.listen(9092, "127.0.0.1");