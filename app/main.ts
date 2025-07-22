import net from "net";

const server: net.Server = net.createServer((connection: net.Socket) => {
  connection.on("data", (data: Buffer) => {
    const correlationId = data.readInt32BE(8);
    console.log("Correlation ID:", correlationId);
    const buffer = Buffer.alloc(10);
    buffer.writeInt32BE(correlationId, 4);
    buffer.writeInt16BE(35, 8);
    connection.write(buffer);
  });
});

server.listen(9092, "127.0.0.1");