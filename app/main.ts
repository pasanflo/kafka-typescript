import net from "net";

enum ErrorCode {
  NO_ERROR = 0,
  UNSUPPORTED_VERSION = 35,
}

enum ApiKey {
  FETCH = 1,
  API_VERSIONS = 18,
  DescribeTopicPartitions = 75,
}

const numberToHex = (number: number, bytes: number) => {
  return number.toString(16).padStart(bytes * 2, "0");
};

const parseMessage = (data: Buffer<ArrayBufferLike>) => {
  const uint8Array = new Uint8Array(data.buffer);

  const messageSizeBuffer = uint8Array.slice(0, 4);
  const apiKeyBuffer = uint8Array.slice(4, 6);
  const apiVersionBuffer = uint8Array.slice(6, 8);
  const correlationIdBuffer = uint8Array.slice(8, 12);
  const clientIdLengthBuffer = uint8Array.slice(12, 14);
  const clientIdLength = Buffer.from(clientIdLengthBuffer).readUintBE(0, 2);

  let startIndex = 14;
  let endIndex = startIndex + clientIdLength;

  const clientIdBuffer = uint8Array.slice(startIndex, endIndex);
  startIndex = endIndex;
  endIndex += 1;

  const __tagBuffer = uint8Array.slice(startIndex, endIndex);

  startIndex = endIndex;
  endIndex += 1;
  const clientIdCompactLengthBuffer = uint8Array.slice(startIndex, endIndex);
  const clientIdCompactLength = Buffer.from(
    clientIdCompactLengthBuffer
  ).readUintBE(0, 1);
  startIndex = endIndex;
  endIndex += clientIdCompactLength - 1;

  const clientIdCompactStringBuffer = uint8Array.slice(startIndex, endIndex);
  startIndex = endIndex;
  endIndex += 1;

  const clientSoftwareVersionLengthBuffer = uint8Array.slice(
    endIndex,
    endIndex + 2
  );
  startIndex = endIndex;
  endIndex +=
    Buffer.from(clientSoftwareVersionLengthBuffer).readUintBE(0, 2) - 1;

  const clientSoftwareVersionBuffer = uint8Array.slice(startIndex, endIndex);

  return {
    messageSizeBuffer,
    apiKeyBuffer,
    apiVersionBuffer,
    correlationIdBuffer,
    clientIdBuffer,
    clientIdCompactStringBuffer,
    clientSoftwareVersionBuffer,
  };
};

const processRequest = (data: Buffer<ArrayBufferLike>, socket: net.Socket) => {
  const { correlationIdBuffer, apiKeyBuffer, apiVersionBuffer } =
    parseMessage(data);

  const apiKey = Buffer.from(apiKeyBuffer).readUintBE(
    0,
    apiKeyBuffer.length
  ) as ApiKey;
  const apiVersion = Buffer.from(apiVersionBuffer).readUintBE(
    0,
    apiVersionBuffer.length
  );

  const supportedAPIKeys = [
    ApiKey.API_VERSIONS,
    //   ApiKey.DescribeTopicPartitions,
    ApiKey.FETCH,
  ] as const;
  const supportedAPIVersions: Record<ApiKey, Array<number>> = {
    [ApiKey.FETCH]: [
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
    ],
    [ApiKey.API_VERSIONS]: [0, 1, 2, 3, 4],
    [ApiKey.DescribeTopicPartitions]: [0],
  } as const;

  let errorCode: ErrorCode;
  if (!supportedAPIVersions[apiKey].includes(apiVersion)) {
    errorCode = ErrorCode.UNSUPPORTED_VERSION;
  } else {
    errorCode = ErrorCode.NO_ERROR;
  }

  const errorCodeBuffer = Buffer.from(numberToHex(errorCode, 2), "hex");
  const supportedAPIsLengthBuffer = Buffer.from(
    numberToHex(supportedAPIKeys.length + 1, 1),
    "hex"
  );
  const throttleTimeBuffer = Buffer.from(numberToHex(0, 4), "hex");
  const tagBuffer = Buffer.from(numberToHex(0, 1), "hex");

  const responseBuffer = Buffer.concat([
    correlationIdBuffer,
    errorCodeBuffer,
    supportedAPIsLengthBuffer,
    supportedAPIKeys.reduce((acc, apiKey) => {
      const minVersion = supportedAPIVersions[apiKey][0];
      const maxVersion =
        supportedAPIVersions[apiKey][supportedAPIVersions[apiKey].length - 1];

      return Buffer.concat([
        acc,
        Buffer.from(numberToHex(apiKey, 2), "hex"),
        Buffer.from(numberToHex(minVersion, 2), "hex"),
        Buffer.from(numberToHex(maxVersion, 2), "hex"),
        tagBuffer,
      ]);
    }, Buffer.from([])),
    throttleTimeBuffer,
    tagBuffer,
  ]);

  const responseMessageSize = Buffer.alloc(4);
  responseMessageSize.writeUInt32BE(responseBuffer.length, 0);

  socket.write(Buffer.concat([responseMessageSize, responseBuffer]));
};

const server: net.Server = net.createServer((socket: net.Socket) => {
  socket.on("data", (data) => processRequest(data, socket));
});

server.listen(9092, "127.0.0.1");