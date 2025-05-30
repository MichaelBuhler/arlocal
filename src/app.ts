import { Server } from 'http';
import { rmSync, mkdirSync, existsSync } from 'fs';
import path, { join } from 'path';
import Koa from 'koa';
import cors from '@koa/cors';
import bodyParser from 'koa-bodyparser';
import Router from 'koa-router';
import json from 'koa-json';
import logger from 'koa-logger';
import { ApolloServer } from 'apollo-server-koa';
import { Knex } from 'knex';
import { connect } from './db/connect';
import { down, up } from './db/initialize';
import { graphServer } from './graphql/server';
import { dataRouteRegex, dataHeadRoute, dataRoute, subDataRoute } from './routes/data';
import { mineRoute, mineWithFailsRoute } from './routes/mine';
import { statusRoute } from './routes/status';
import {
  txAnchorRoute,
  txRoute,
  txPostRoute,
  txOffsetRoute,
  txStatusRoute,
  txFieldRoute,
  txFileRoute,
  txRawDataRoute,
  deleteTxRoute,
  txDataRoute,
  txPendingRoute,
} from './routes/transaction';
import { /*txAccessMiddleware,*/ txValidateMiddleware } from './middlewares/transaction';
import { NetworkInterface } from './faces/network';
import Logging from './utils/logging';
import { blocksRoute, blocksRouteViaHeight } from './routes/blocks';
import {
  addBalanceRoute,
  createWalletRoute,
  getBalanceRoute,
  getLastWalletTxRoute,
  updateBalanceRoute,
} from './routes/wallet';
import { getChunkOffsetRoute, postChunkRoute } from './routes/chunk';
import { peersRoute } from './routes/peer';
import { WalletDB } from './db/wallet';
import { BlockDB } from './db/block';
import { logsRoute, resetRoute } from './routes/utils';

declare module 'koa' {
  interface BaseContext {
    connection: Knex;
    network: NetworkInterface;
    transactions: string[];
    dbPath: string;
    logging: Logging;
    fails: number;
    timestamp: number;
    txInBundle?: boolean;
  }
}

export default class ArLocal {
  private port: number = 1984;
  private dbPath: string;
  private persist: boolean;
  private fails: number;
  private log: Logging;

  private connection: Knex;
  private apollo: ApolloServer;

  private server: Server;
  private app = new Koa();
  private router = new Router();
  private walletDB: WalletDB;

  constructor(port: number = 1984, showLogs: boolean = true, dbPath?: string, persist = false, fails = 0) {
    this.port = port || this.port;
    dbPath = dbPath || path.join(__dirname, '.db', port.toString());

    this.dbPath = dbPath;

    this.persist = persist;
    this.fails = fails;
    this.log = new Logging(showLogs, this.persist);

    this.connection = connect(dbPath);

    this.app.context.network = {
      network: 'arlocal.N.1',
      version: 1,
      release: 1,
      queue_length: 0,
      peers: 1,
      node_state_latency: 0,
      // blocks, current, and height will be set when the app is started
      // either from the most recent block in the database
      // or from a new genesis block if there is an empty database
      blocks: 0,
      current: 'block_id',
      height: -1,
    };

    this.app.context.logging = this.log;
    this.app.context.dbPath = dbPath;
    this.app.context.connection = this.connection;
    this.app.context.fails = this.fails / 100;
    // server start date for genesis block timestamp
    this.app.context.timestamp = new Date().getTime();
    this.walletDB = new WalletDB(this.connection);
  }

  async start() {
    await this.startDB();

    const blockDB = new BlockDB(this.connection);
    const lastBlock = await blockDB.getLastBlock();
    if (lastBlock) {
      this.app.context.network.blocks = lastBlock.height + 1;
      this.app.context.network.current = lastBlock.id;
      this.app.context.network.height = lastBlock.height;
    } else {
      // save the genesis block to db
      const blockId = await blockDB.mineGenesisBlock();
      this.app.context.network.blocks = 1;
      this.app.context.network.current = blockId;
      this.app.context.network.height = 0;
    }

    if (process.env.DISABLE_ADMIN_ROUTES !== 'true') this.router.get('/logs', logsRoute);
    this.router.get('/', statusRoute);
    this.router.get('/info', statusRoute);
    this.router.get('/peers', peersRoute);
    if (process.env.DISABLE_ADMIN_ROUTES !== 'true') this.router.get('/reset', resetRoute);
    if (process.env.DISABLE_ADMIN_ROUTES !== 'true') this.router.get('/mine/:qty?', mineRoute);
    if (process.env.DISABLE_ADMIN_ROUTES !== 'true') this.router.get('/mineWithFails/:qty?', mineWithFailsRoute);

    this.router.get('/tx_anchor', txAnchorRoute);
    this.router.get(
      '/price/:bytes/:addy?',
      async (ctx) => (ctx.body = Math.round(+ctx.params.bytes * +(process.env.WINSTON_PER_BYTE || 65595.508)).toString()),
    );

    this.router.get('/tx/pending', txPendingRoute);

    // tx filter endpoint to restrict ans-104 txs
    // this.router.get(/^\/tx(?:\/|$)/, txAccessMiddleware);

    this.router.get('/tx/:txid/offset', txOffsetRoute);
    this.router.get('/tx/:txid/status', txStatusRoute);
    this.router.get('/tx/:txid/data', txRawDataRoute);
    this.router.get('/tx/:txid/data.:ext', txDataRoute);
    this.router.get('/tx/:txid/:field', txFieldRoute);
    this.router.get('/tx/:txid/:file', txFileRoute);
    this.router.get('/tx/:txid', txRoute);
    this.router.get('/raw/:txid', txRawDataRoute);
    if (process.env.DISABLE_ADMIN_ROUTES !== 'true') this.router.delete('/tx/:txid', deleteTxRoute);
    this.router.post('/tx', txValidateMiddleware, txPostRoute);

    this.router.post('/chunk', postChunkRoute);
    this.router.get('/chunk/:offset', getChunkOffsetRoute);

    this.router.get('/block/hash/:indep_hash', blocksRoute);
    this.router.get('/block/height/:height', blocksRouteViaHeight);

    if (process.env.DISABLE_ADMIN_ROUTES !== 'true') this.router.post('/wallet', createWalletRoute);
    if (process.env.DISABLE_ADMIN_ROUTES !== 'true') this.router.patch('/wallet/:address/balance', updateBalanceRoute);
    if (process.env.DISABLE_ADMIN_ROUTES !== 'true') this.router.get('/mint/:address/:balance', addBalanceRoute);

    this.router.get('/wallet/:address/balance', getBalanceRoute);
    this.router.get('/wallet/:address/last_tx', getLastWalletTxRoute);

    this.router.head(dataRouteRegex, dataHeadRoute);
    this.router.get(dataRouteRegex, dataRoute);

    this.router.get('/(.*)', subDataRoute);

    this.router.get('/:other', (ctx) => {
      ctx.type = 'application/json';
      ctx.body = {
        status: 400,
        error: 'Request type not found.',
      };
    });

    this.app.use(
      cors({
        origin: '*',
      }),
    );
    this.app.use(async (ctx, next) => {
      // set cross origin resource policy header
      try {
        ctx.set('Cross-Origin-Resource-Policy', 'cross-origin');
      } catch {}
      return await next();
    });
    this.app.use(json());
    this.app.use(
      logger({
        transporter: (str, args) => {
          this.log.log(str);
          this.log.logInFile(args);
        },
      }),
    );
    this.app.use(
      bodyParser({
        jsonLimit: '15mb',
      }),
    );
    this.app.use(this.router.routes()).use(this.router.allowedMethods());

    this.server = this.app.listen(this.port, () => {
      console.log(`arlocal started on port ${this.port}`);
    });
  }

  private async startDB() {
    // Delete old database
    try {
      if (!this.persist) rmSync(this.dbPath, { recursive: true });
    } catch (e) {}

    if (!existsSync(this.dbPath)) mkdirSync(this.dbPath, { recursive: true });

    // sqlite
    this.apollo = graphServer(
      {
        introspection: true,
        playground: true,
      } as any,
      this.app.context,
      this.connection,
    );

    await this.apollo.start();
    this.apollo.applyMiddleware({ app: this.app, path: '/graphql' });
    if (this.dbPath !== ':memory:' && !existsSync(join(this.dbPath, 'db.sqlite'))) await up(this.connection);
  }

  async stop() {
    if (this.server) {
      this.server.close((err) => {
        if (err) {
          try {
            if (!this.persist) rmSync(this.dbPath, { recursive: true });
          } catch (err) {}
          return;
        }
      });
    }
    down(this.connection, this.persist)
      .then(() => {
        this.apollo
          .stop()
          .then(() => {
            this.connection
              .destroy()
              .then(() => {
                try {
                  if (!this.persist) rmSync(this.dbPath, { recursive: true });
                } catch (e) {}
              })
              .catch(() => {});
          })
          .catch(() => {});
      })
      .catch(() => {});
  }

  getServer(): Server {
    return this.server;
  }
  getNetwork(): NetworkInterface {
    return this.app.context.network;
  }

  getDbPath(): string {
    return this.dbPath;
  }
  getWalletDb(): WalletDB {
    return this.walletDB;
  }
}
