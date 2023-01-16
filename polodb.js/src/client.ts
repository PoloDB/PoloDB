import path from 'path';
import { EventEmitter } from 'events';
import SharedState, { Config } from './sharedState';
import Collection from './colleciton';
import child_process from 'child_process';
import { TransactionType } from './transactionType';
import os from 'os';
import { encode } from './encoding';

const defaultConfig: Config = {
  executablePath: os.platform() === 'win32' ? path.join(__dirname, '../bin/polodb.exe') : path.join(__dirname, '../bin/polodb'),
  log: false,
};

class PoloDbClient extends EventEmitter {

  private __state: SharedState;
  private __shuttingDown: boolean = false;

  public static version(config?: Partial<Config>): Promise<string> {
    const mergedConfig = {
      ...defaultConfig,
      ...config,
    };

    const params: string[] = ['--version'];

    return new Promise((resolve, reject) => {
      const process = child_process.spawn(
        mergedConfig.executablePath,
        params,
        {
          stdio: ['pipe']
        }
      );

      let stdOutBuffer = Buffer.alloc(0);

      process.stdout.on('data', (chunk: Buffer) => {
        const newBuffer = Buffer.alloc(stdOutBuffer.length + chunk.length);
        stdOutBuffer.copy(newBuffer, 0);
        chunk.copy(newBuffer, stdOutBuffer.length);
        stdOutBuffer = newBuffer;
      });

      process.on('error', (err: Error) => {
        reject(err);
      });

      process.stdout.on('close', () => {
        const decoder = new TextDecoder();
        let content = decoder.decode(stdOutBuffer);
        content = content.replace('\n', '');
        resolve(content)
      });
    });
  }

  public static async createConnection(dbPath: string, config?: Partial<Config>): Promise<PoloDbClient> {
    const client = new PoloDbClient(dbPath, config);

    await client.start();

    return client;
  }

  private constructor(dbPath: string, config?: Partial<Config>) {
    super();

    const mergedConfig = {
      ...defaultConfig,
      ...config,
    };

    this.__state = new SharedState(dbPath, mergedConfig, (err) => {
      if (this.__shuttingDown) {
        this.__state.kill();
      }
      this.emit('error', err);
    });
  }

  public startTransaction(ty?: TransactionType): Promise<void> {
    if (typeof ty === 'undefined') {
      ty = TransactionType.Auto;
    }
    const pack = encode(ty);
    return this.__state.sendRequest(pack);
  }

  public dropCollection(name: string): Promise<void> {
    const pack = encode(name);
    return this.__state.sendRequest(pack);
  }

  public commit(): Promise<void> {
    throw new Error("not implemented");
  }

  public rollback(): Promise<void> {
    throw new Error("not implemented");
  }


  private start(): Promise<void> {
    return this.__state.start();
  }

  public collection(name: string) {
    return new Collection(this.__state, name);
  }

  public async createCollection(name: string): Promise<Collection> {
    const pack = encode({ name });
    await this.__state.sendRequest(pack);
    return this.collection(name);
  }

  public dispose() {
    this.__shuttingDown = true;
    this.__state.dispose();
  }

  get config(): Config {
    return { ...this.__state.config };
  }

}

export default PoloDbClient;
