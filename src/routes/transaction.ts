import Arweave from 'arweave';
import Router from 'koa-router';
import mime from 'mime';
import { formatTransaction, TransactionDB } from '../db/transaction';
import { DataDB } from '../db/data';
import { Utils } from '../utils/utils';
import { TransactionType } from '../faces/transaction';
import { Bundle } from 'arbundles';
import { WalletDB } from '../db/wallet';
import { b64UrlToBuffer, bufferTob64Url, hash } from '../utils/encoding';
import { ChunkDB } from '../db/chunks';
import { Next } from 'koa';
import Transaction from 'arweave/node/lib/transaction';

export const pathRegex = /^\/?([a-z0-9-_]{43})/i;

let transactionDB: TransactionDB;
let dataDB: DataDB;
let walletDB: WalletDB;
let chunkDB: ChunkDB;
let oldDbPath: string;
let connectionSettings: string;
const FIELDS = [
  'id',
  'last_tx',
  'owner',
  'tags',
  'target',
  'quantity',
  'data_root',
  'data_size',
  'reward',
  'signature',
];

export async function txAnchorRoute(ctx: Router.RouterContext) {
  const txs = await ctx.connection.select('id').from('blocks').orderBy('created_at', 'desc').limit(1);
  if (txs.length) {
    ctx.body = txs[0].id;
    return;
  }
  ctx.body = '';
}

export async function txRoute(ctx: Router.RouterContext) {
  try {
    if (
      oldDbPath !== ctx.dbPath ||
      !transactionDB ||
      connectionSettings !== ctx.connection.client.connectionSettings.filename
    ) {
      transactionDB = new TransactionDB(ctx.connection);
      oldDbPath = ctx.dbPath;
      connectionSettings = ctx.connection.client.connectionSettings.filename;
    }
    const path = ctx.params.txid.match(pathRegex) || [];
    const transaction = path.length > 1 ? path[1] : '';

    const metadata = await transactionDB.getById(transaction);
    ctx.logging.log(metadata);

    if (!metadata) {
      ctx.status = 404;
      ctx.body = { status: 404, error: 'Not Found' };
      return;
    }

    ctx.status = 200;
    ctx.headers['accept-ranges'] = 'bytes';
    ctx.headers['content-length'] = metadata.data_size;
    ctx.body = metadata;
  } catch (error) {
    console.error({ error });
  }
}

export async function txOffsetRoute(ctx: Router.RouterContext) {
  try {
    if (
      oldDbPath !== ctx.dbPath ||
      !transactionDB ||
      !chunkDB ||
      !dataDB ||
      connectionSettings !== ctx.connection.client.connectionSettings.filename
    ) {
      transactionDB = new TransactionDB(ctx.connection);
      chunkDB = new ChunkDB(ctx.connection);
      dataDB = new DataDB(ctx.dbPath);
      oldDbPath = ctx.dbPath;
      connectionSettings = ctx.connection.client.connectionSettings.filename;
    }

    const path = ctx.params.txid.match(pathRegex) || [];
    const transaction = path.length > 1 ? path[1] : '';

    const metadata: Transaction = await transactionDB.getById(transaction);

    if (!metadata) {
      ctx.status = 404;
      ctx.body = { status: 404, error: 'Not Found' };
      return;
    }

    const dataSize = BigInt(metadata.data_size)

    const chunk = await chunkDB.getByRootAndLocalOffset(metadata.data_root, dataSize);

    if (!chunk) {
      ctx.status = 404;
      ctx.body = { status: 404, error: 'Not Found' };
      return;
    }

    ctx.status = 200;
    ctx.body = {
      offset: chunk.global_offset,
      size: dataSize.toString(),
    };
  } catch (error) {
    console.error({ error });
  }
}

export async function txPostRoute(ctx: Router.RouterContext) {
  try {
    if (oldDbPath !== ctx.dbPath || !dataDB || !walletDB) {
      dataDB = new DataDB(ctx.dbPath);
      walletDB = new WalletDB(ctx.connection);
      chunkDB = new ChunkDB(ctx.connection);

      oldDbPath = ctx.dbPath;
    }
    const tx = ctx.request.body as unknown as TransactionType;
    const owner = bufferTob64Url(await hash(b64UrlToBuffer(tx.owner)));

    const wallet = await walletDB.getWallet(owner);
    const calculatedReward = Math.round(+(tx.data_size || '0') * +(process.env.WINSTON_PER_BYTE || 65595.508));

    if (
      !tx.bundledIn &&
      calculatedReward > 0 &&
      (!wallet || wallet.balance < calculatedReward)
    ) {
      ctx.status = 410;
      ctx.body = { code: 410, msg: "You don't have enough tokens" };
      return;
    }

    let bundleFormat = '';
    let bundleVersion = '';
    ctx.logging.log('posted tx', tx);
    for (const tag of tx.tags) {
      const name = Utils.atob(tag.name);
      const value = Utils.atob(tag.value);
      ctx.logging.log(`Parsed tag: ${name}=${value}`);
      if (name === 'Bundle-Format') bundleFormat = value;
      if (name === 'Bundle-Version') bundleVersion = value;
    }

    if (bundleFormat === 'binary' && bundleVersion === '2.0.0') {
      // ANS-104

      const createTxsFromItems = async (buffer: Buffer) => {
        const bundle = new Bundle(buffer);

        const items = bundle.items;
        for (let i = 0; i < items.length; i++) {
          const item = items[i];

          await txPostRoute({
            ...ctx,
            connection: ctx.connection,
            dbPath: ctx.dbPath,
            logging: ctx.logging,
            network: ctx.network,
            request: {
              ...ctx.request,
              body: {
                id: bundle.get(i).id,
                bundledIn: tx.id,
                ...item.toJSON(),
              },
            },
            txInBundle: true,
          });
        }
      };

      if (tx.data) {
        const buffer = Buffer.from(tx.data, 'base64');
        await createTxsFromItems(buffer);
      } else {
        (async () => {
          const chunks = await chunkDB.getRoot(tx.data_root);
          ChunkDB.sort(chunks)
          const buffers = chunks.map(x => b64UrlToBuffer(x.chunk));
          const buffer = Buffer.concat(buffers);
          await createTxsFromItems(buffer);
        })();
      }
    }

    // if this tx is not part of a bundle,
    // and if there is no data in this tx,
    // but the size of the data is defined
    if (!ctx.txInBundle && !tx.data && !!tx.data_size) {
      // then we expect the client to make chunk uploads.
      // check to see if this object has already been chunk uploaded
      const chunks = await chunkDB.getRoot(tx.data_root)
      if (chunks.length === 0) {
        // it has never been uploaded before
        // so we will reserve space in the global chunk memory.
        const dataSize = BigInt(tx.data_size)
        const localOffset = dataSize - BigInt(1)
        const currentGlobalOffset = await chunkDB.getCurrentGlobalOffset()
        const newGlobalOffset = currentGlobalOffset + dataSize - BigInt(1)
        await chunkDB.create({
          chunk: '',
          data_root: tx.data_root,
          data_size: tx.data_size,
          chunk_size: '0',
          local_offset: localOffset.toString(),
          global_offset: newGlobalOffset.toString(),
          data_path: '',
        })
      }
    }

    // AR Transfers
    if (tx?.target && tx?.quantity) {
      let targetWallet = await walletDB.getWallet(tx.target);
      if (!targetWallet) {
        await walletDB.addWallet({
          address: tx?.target,
          balance: 0,
        });

        targetWallet = await walletDB.getWallet(tx.target);
      }

      if (!wallet || !targetWallet) {
        ctx.status = 404;
        ctx.body = { status: 404, error: `Wallet not found` };
        return;
      }
      if (wallet?.balance < +tx.quantity + +tx.reward) {
        ctx.status = 403;
        ctx.body = { status: 403, error: `you don't have enough funds to send ${tx.quantity}` };
        return;
      }
      await walletDB.incrementBalance(tx.target, +tx.quantity);
      await walletDB.decrementBalance(wallet.address, +tx.quantity);
    }

    await dataDB.insert({ txid: tx.id, data: tx.data });

    const txToInsert = formatTransaction(tx);
    txToInsert.created_at = new Date().toISOString();
    txToInsert.height = ctx.network.blocks;

    await ctx.connection.insert(txToInsert).into('transactions');

    let index = 0;
    for (const tag of tx.tags) {
      const name = Utils.atob(tag.name);
      const value = Utils.atob(tag.value);

      await ctx.connection
        .insert({
          index,
          tx_id: tx.id,
          name,
          value,
        })
        .into('tags');

      index++;
    }

    // Don't charge wallet for ANS-104 bundled data items
    // @ts-ignore
    if (!ctx.txInBundle) {
      const fee = +tx.reward > calculatedReward ? +tx.reward : calculatedReward;
      await walletDB.decrementBalance(owner, +fee);
    }
    ctx.body = tx;
  } catch (error) {
    console.error({ error });
  }
}

export async function txStatusRoute(ctx: Router.RouterContext) {
  try {
    if (
      oldDbPath !== ctx.dbPath ||
      !transactionDB ||
      connectionSettings !== ctx.connection.client.connectionSettings.filename
    ) {
      transactionDB = new TransactionDB(ctx.connection);
      oldDbPath = ctx.dbPath;
      connectionSettings = ctx.connection.client.connectionSettings.filename;
    }

    const path = ctx.params.txid.match(pathRegex) || [];
    const transaction = path.length > 1 ? path[1] : '';

    const metadata = await transactionDB.getById(transaction);

    if (!metadata) {
      ctx.status = 404;
      ctx.body = { status: 404, error: 'Not Found !' };
      return;
    }
    if (!metadata.block) {
      ctx.body = 'Pending';
      return;
    }

    ctx.body = {
      block_height: metadata.height,
      block_indep_hash: metadata.block,
      number_of_confirmations: ctx.network.height - metadata.height,
    };
    return;
  } catch (error) {
    console.error({ error });
  }
}

export async function txFieldRoute(ctx: Router.RouterContext, next: Next) {
  try {
    if (
      oldDbPath !== ctx.dbPath ||
      !transactionDB ||
      connectionSettings !== ctx.connection.client.connectionSettings.filename
    ) {
      transactionDB = new TransactionDB(ctx.connection);
      oldDbPath = ctx.dbPath;
      connectionSettings = ctx.connection.client.connectionSettings.filename;
    }

    const path = ctx.params.txid.match(pathRegex) || [];
    const transaction = path.length > 1 ? path[1] : '';

    const field = ctx.params.field;
    if (field.includes('.')) {
      await next();
      return;
    }
    if (!FIELDS.includes(field)) {
      ctx.status = 404;
      ctx.body = { status: 404, error: 'Field Not Found !' };
      return;
    }

    const metadata = await transactionDB.getById(transaction);
    if (!metadata) {
      ctx.status = 404;
      ctx.body = { status: 404, error: 'Not Found !' };
      return;
    }

    if (!metadata.block) {
      ctx.body = 'Pending';
      return;
    }

    ctx.body = metadata[field];
    return;
  } catch (error) {
    console.error({ error });
  }
}

export async function txFileRoute(ctx: Router.RouterContext) {
  try {
    if (
      oldDbPath !== ctx.dbPath ||
      !transactionDB ||
      connectionSettings !== ctx.connection.client.connectionSettings.filename
    ) {
      transactionDB = new TransactionDB(ctx.connection);
      oldDbPath = ctx.dbPath;
      connectionSettings = ctx.connection.client.connectionSettings.filename;
    }

    const path = ctx.params.txid.match(pathRegex) || [];
    const transaction = path.length > 1 ? path[1] : '';

    const file = ctx.params.file;

    const metadata = await transactionDB.getById(transaction);
    if (!metadata) {
      ctx.status = 404;
      ctx.body = { status: 404, error: 'Not Found !' };
      return;
    }

    if (!metadata.block) {
      ctx.body = 'Pending';
      return;
    }

    ctx.redirect(`http://${ctx.request.header.host}/${transaction}/${file}`);
    return;
  } catch (error) {
    console.error({ error });
  }
}

export async function txRawDataRoute(ctx: Router.RouterContext) {
  try {
    if (
      !transactionDB ||
      !dataDB ||
      !chunkDB ||
      oldDbPath !== ctx.dbPath ||
      connectionSettings !== ctx.connection.client.connectionSettings.filename
    ) {
      transactionDB = new TransactionDB(ctx.connection);
      dataDB = new DataDB(ctx.dbPath);
      chunkDB = new ChunkDB(ctx.connection)
      oldDbPath = ctx.dbPath;
      connectionSettings = ctx.connection.client.connectionSettings.filename;
    }

    const path = ctx.params.txid.match(pathRegex) || [];
    const txid = path.length > 1 ? path[1] : '';

    const metadata: TransactionType = await transactionDB.getById(txid);

    if (!metadata) {
      ctx.status = 404;
      ctx.body = { status: 404, error: 'Not found' };
      return;
    }

    // Check for the data_size
    if (BigInt(metadata.data_size) > 10_000_000) {
      ctx.status = 400;
      ctx.body = { problem: 'data is too big', solution: 'use the `/chunk/:offset` routing to download bigger chunks' };
      return;
    }

    // Find the transaction data
    const { data } = await dataDB.findOne(txid);

    let buffer: Buffer
    if (data.length === 0) {
      // then we need fetch the chunks
      const chunks = await chunkDB.getRoot(metadata.data_root)
      ChunkDB.sort(chunks)
      buffer = Buffer.concat(
        chunks.map(({ chunk: chunkData }) => {
          return Buffer.from(Arweave.utils.b64UrlToBuffer(chunkData))
        })
      )
    } else {
      buffer = Buffer.from(Arweave.utils.b64UrlToBuffer(data))
    }

    ctx.status = 200;
    ctx.type = Utils.tagValue(metadata.tags, 'Content-Type') || 'application/octet-stream'
    ctx.body = buffer;
  } catch (error) {
    ctx.logging.error(error)
    ctx.status = 500;
    ctx.body = { error: error.message };
  }
}

export async function txDataRoute(ctx: Router.RouterContext, next: Next) {
  try {
    if (
      !transactionDB ||
      !dataDB ||
      oldDbPath !== ctx.dbPath ||
      connectionSettings !== ctx.connection.client.connectionSettings.filename
    ) {
      transactionDB = new TransactionDB(ctx.connection);
      dataDB = new DataDB(ctx.dbPath);
      oldDbPath = ctx.dbPath;
      connectionSettings = ctx.connection.client.connectionSettings.filename;
    }

    const path = ctx.params.txid.match(pathRegex) || [];
    const txid = path.length > 1 ? path[1] : '';

    const metadata: TransactionType = await transactionDB.getById(txid);

    if (!metadata) {
      ctx.status = 404;
      ctx.body = { status: 404, error: 'Not found' };
      return;
    }

    const ext = ctx.params.ext;
    const contentType = mime.getType(ext);

    // Find the transaction data
    const data = await dataDB.findOne(txid);

    if (!data || !data.data) {
      // move to next controller
      return await next();
    }

    // parse raw data to manifest
    const parsedData = Utils.atob(data.data);

    ctx.header['content-type'] = contentType;
    ctx.status = 200;
    ctx.body = parsedData;
  } catch (error) {
    ctx.status = 500;
    ctx.body = { error: error.message };
  }
}

export async function txPendingRoute(ctx: Router.RouterContext) {
  try {
    if (
      oldDbPath !== ctx.dbPath ||
      !transactionDB ||
      connectionSettings !== ctx.connection.client.connectionSettings.filename
    ) {
      transactionDB = new TransactionDB(ctx.connection);
      oldDbPath = ctx.dbPath;
      connectionSettings = ctx.connection.client.connectionSettings.filename;
    }

    const txIds = await transactionDB.getUnminedTxs();

    ctx.status = 200;
    ctx.body = txIds;
  } catch (error) {
    console.error({ error });
    ctx.status = 500;
    ctx.body = { error: error.message };
  }
}

export async function deleteTxRoute(ctx: Router.RouterContext) {
  try {
    if (
      !transactionDB ||
      !dataDB ||
      oldDbPath !== ctx.dbPath ||
      connectionSettings !== ctx.connection.client.connectionSettings.filename
    ) {
      transactionDB = new TransactionDB(ctx.connection);
      dataDB = new DataDB(ctx.dbPath);
      oldDbPath = ctx.dbPath;
      connectionSettings = ctx.connection.client.connectionSettings.filename;
    }

    const path = ctx.params.txid.match(pathRegex) || [];
    const txid = path.length > 1 ? path[1] : '';

    const metadata: TransactionType = await transactionDB.getById(txid);

    if (!metadata) {
      ctx.status = 404;
      ctx.body = { status: 404, error: 'Not found' };
      return;
    }

    await transactionDB.deleteById(txid);

    ctx.status = 200;
    return;
  } catch (error) {
    ctx.status = 500;
    ctx.body = { error: error.message };
  }
}
