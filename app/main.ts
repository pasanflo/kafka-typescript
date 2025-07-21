import net from "net";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
const server: net.Server = net.createServer((connection: net.Socket) => {
   connection.on("data", () => {
    // Prepare response buffer (8 bytes total):
    // 4 bytes for message_size (set to 0 for now)
    // 4 bytes for correlation_id (set to 7)

    const response = Buffer.alloc(8);
    response.writeInt32BE(0, 0); // message_size (ignored by tester)
    response.writeInt32BE(7, 4); // correlation_id

    connection.write(response);
  });
});


server.listen(9092, "127.0.0.1");
