import net from "net";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
const server: net.Server = net.createServer((connection: net.Socket) => {
    // Handle connection
    connection.on("data", (data: Buffer) => {
        // Log the received data
        console.log(`Received data: ${data.toString()}`);
        const data_size = data.subarray(0, 4);
        const data_api_key = data.subarray(4, 6);
        const data_api_version = data.subarray(6, 8);
        const correlation_id = data.subarray(8, 12);
        const message_size = Buffer.alloc(4);
        const error_code = Buffer.from('0000', 'hex');

        if (data_api_version.toString("hex") > '0004') {
            error_code.writeUInt16BE(35, 0);
        }

        const api_version_length = Buffer.from('02', 'hex');
        const api_version_item = Buffer.from('00120000000400', 'hex');
        const api_version = Buffer.concat([api_version_length, api_version_item]);
        const throttle_time_ms = Buffer.from('00000000', 'hex');
        const tag_buffer = Buffer.from('00', 'hex');
        const body = Buffer.concat([error_code, api_version, throttle_time_ms, tag_buffer]);
        const message = Buffer.concat([correlation_id, body]);

        message_size.writeUInt32BE(message.length, 0);
        connection.write(Buffer.concat([message_size, message]))
    });
});

server.listen(9092, "127.0.0.1");