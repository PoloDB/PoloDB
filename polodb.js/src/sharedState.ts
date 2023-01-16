import net, { Socket } from 'net';
import child_process from 'child_process';
import { decode } from './encoding';
import { REQUEST_HEAD, PING_HEAD } from './common';
import os from 'os';

const sleep = (time) => new Promise((resolve) => setTimeout(resolve, time));

export interface Config {
  executablePath: string;
  log: boolean;
}

export interface RequestItem {
  reqId: number,
  resolve: (result: any) => void,
  reject: (err: Error) => void,
}

const HEADER_SIZE = 16;

class SharedState {

  private __socketPath: string;
  public socket?: Socket;
  public reqidCounter: number;
  private __process: child_process.ChildProcess;
  private __buffer: Buffer = Buffer.from("");
  private __programExit: boolean = false;
  public promiseMap: Map<number, RequestItem> = new Map();

  constructor(
    public dbPath: string,
    public config: Config,
    public errorHandler: (err: Error) => void,
  ) {
    const params: string[] = ['serve'];
    if (dbPath === 'memory') {
      params.push('--memory');
    } else {
      params.push('--path');
      params.push(dbPath);
    }

    if (config.log) {
      params.push('--log');
    }

    if (os.platform() === 'win32') {
      this.__socketPath = `\\\\.\\pipe\\polodb-ipc-${Math.round(Math.random() * 0xFFFFFF)}`;
    } else {
      this.__socketPath = `/tmp/polodb-${Math.round(Math.random() * 0xFFFFFF)}.sock`;
    }

    params.push('--socket');
    params.push(this.__socketPath);

    this.__process = child_process.spawn(
      this.config.executablePath,
      params,
      {
        stdio: 'inherit'
      }
    );

    this.__process.on('exit', () => {
      this.__programExit = true;
    });

    this.reqidCounter = Math.round(Math.random() * 0xFFFFFF);
  }

  public async start(): Promise<void> {
    const sleepTimes = [5, 7, 9, 11];
    for (let i = 0; i < 320; i++) {
      if (this.__programExit) {
        break;
      }
      try {
        await this.ping();
        return;
      } catch (err) {
        await sleep(sleepTimes[i % sleepTimes.length]);
      }
    }
    throw new Error('can not connect to the database');
  }

  private appendData(buf: Buffer) {
    const totalSize = this.__buffer.length + buf.length;
    const newBuffer = Buffer.alloc(totalSize);

    this.__buffer.copy(newBuffer, 0);
    buf.copy(newBuffer, this.__buffer.length);

    this.__buffer = newBuffer;
  }

  private tryParseBuffer() {
    const buf = this.__buffer;

    if (buf.length < HEADER_SIZE) {
      return;
    }

    function bytesEqual(b1: Uint8Array, b2: Uint8Array): boolean {
      if (b1.length !== b2.length) {
        return false;
      }

      for (let i = 0; i < b1.length; i++) {
        if (b1[i] !== b2[i]) {
          return false;
        }
      }

      return true;
    }

    let head = buf.subarray(0, 4);
    if (!bytesEqual(head, REQUEST_HEAD)) {
      if (bytesEqual(head, PING_HEAD)) {
        if (buf.length < 8) {  // bytes not enough
          return;
        }
        let reqId = buf.readUInt32BE(4);
        this.__buffer = buf.subarray(8);

        const ctx = this.promiseMap.get(reqId);
        if (!ctx) {
          return;
        }

        ctx.resolve(undefined);
        return this.tryParseBuffer();
      }
      this.socket.destroy(new Error('response header not match'));
      this.socket = undefined;
      return;
    }

    let reqId = buf.readUInt32BE(4);
    const requestContext = this.promiseMap.get(reqId);
    if (!requestContext) {
      this.socket.destroy(new Error('request id not found, ' + reqId));
      this.socket = undefined;
      this.__buffer = Buffer.alloc(0);
      return;
    }

    const msgTy = buf.readInt32BE(8);
    if (msgTy < 0) {  // error
      this.tryParseErrorMessage(requestContext);
      return;
    }

    const bodySize = buf.readUInt32BE(12);
    if (bodySize === 0) {
      requestContext.resolve(undefined);
      this.__buffer = buf.subarray(HEADER_SIZE);
      return 0;
    }

    const endSize = HEADER_SIZE + bodySize;
    if (buf.length < endSize) {  // body not enough
      return;
    }

    const body = buf.subarray(HEADER_SIZE, endSize);
    try {
      const obj = decode(body);
      requestContext.resolve(obj);
      this.__buffer = buf.subarray(endSize);
    } catch (err) {
      requestContext.reject(err);
      this.socket.destroy();
      this.socket = null;
    }
    this.tryParseBuffer();
  }

  private tryParseErrorMessage(ctx: RequestItem) {
    const buf = this.__buffer;

    const errorMsgSize = buf.readUInt32BE(12);

    const endSize = HEADER_SIZE + errorMsgSize;
    if (buf.length < endSize) {  // body not enough
      return;
    }

    const errorMsgBuffer = buf.subarray(HEADER_SIZE, HEADER_SIZE + errorMsgSize);
    const textDecoder = new TextDecoder();
    const errString = textDecoder.decode(errorMsgBuffer);
    ctx.reject(new Error(errString));
    this.__buffer = buf.subarray(endSize);
    this.tryParseBuffer();
  }

  private __handleData = (buf: Buffer) => {
    this.appendData(buf);
    this.tryParseBuffer();
  }

  public initSocketIfNotExist(spare?: Socket) {
    if (this.socket) {
      return;
    }
    if (spare) {
      this.socket = spare;
    } else {
      this.socket = net.createConnection({
        path: this.__socketPath,
      });
    }

    this.socket.on('error', (err: Error) => {
      this.rejectAllPromises(err);
      this.errorHandler(err);
      this.socket = undefined;
    });

    this.socket.on('data', this.__handleData);

    this.socket.on('close', () => {
      this.socket = undefined;
    });
  }

  private rejectAllPromises(err: Error) {
    for (const [, value] of this.promiseMap) {
      value.reject(err);
    }
    this.promiseMap.clear();
  }

  public ping(): Promise<void> {
    return new Promise((resolve, reject) => {
      const socket = new Socket();
      const handleError = (err: Error) => {
        reject(err);
      };
      socket.connect(this.__socketPath, () => {
        socket.removeListener('error', handleError);
        this.initSocketIfNotExist(socket);
        resolve();
      });
      socket.on('error', handleError);
    });
  }

  public writeUint32(num: number, cb?: (err: Error) => void): boolean {
    num = (num|0);
    const buffer = new ArrayBuffer(4);
    const view = new DataView(buffer);
    view.setUint32(0, num);
    return this.socket.write(new Uint8Array(buffer), cb);
  }

  public writeInt32(num: number, cb?: (err: Error) => void): boolean {
    num = (num|0);
    const buffer = new ArrayBuffer(4);
    const view = new DataView(buffer);
    view.setInt32(0, num);
    return this.socket.write(new Uint8Array(buffer), cb);
  }

  public writeBody(buf: Uint8Array, cb?: (err: Error) => void): boolean {
    this.writeUint32(buf.byteLength, cb);
    return this.socket.write(buf, cb);
  }

  private generateHandleWrite(reqId: number): (err?: Error) => void {
    return (err?: Error) => {
      if (!err) {
        return;
      }

      const item = this.promiseMap.get(reqId);
      if (!item) {
        return;
      }

      this.promiseMap.delete(reqId);

      item.reject(err);
    };
  }

  public sendRequest(body?: Uint8Array): Promise<any> {
    return new Promise((resolve, reject) => {
      const reqId = this.reqidCounter++;
      this.promiseMap.set(reqId, {
        reqId,
        resolve,
        reject,
      });

      const handleWrite = this.generateHandleWrite(reqId);

      this.socket.write(REQUEST_HEAD, handleWrite);

      this.writeUint32(reqId, handleWrite);

      if (body) {
        this.writeBody(body, handleWrite);
      } else {
        this.writeUint32(0);
      }
    });
  }

  public kill() {
    this.socket.destroy();
  }

  public dispose() {
    this.kill();
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
    }
  }

}

export default SharedState
