import { ChunkDB } from '../db/chunks';
import Router from 'koa-router';
import { b64UrlToBuffer } from '../utils/encoding';

let chunkDB: ChunkDB;
let oldDbPath: string;

export async function postChunkRoute(ctx: Router.RouterContext) {
  try {
    if (oldDbPath !== ctx.dbPath || !chunkDB) {
      chunkDB = new ChunkDB(ctx.connection);
      oldDbPath = ctx.dbPath;
    }

    const incomingChunk = ctx.request.body as unknown as {
      chunk: string;
      data_root: string;
      data_size: string;
      offset: string;
      data_path: string;
    };
    const chunkData    = incomingChunk.chunk
    const data_root    = incomingChunk.data_root
    const data_size    = BigInt(incomingChunk.data_size)
    const local_offset = BigInt(incomingChunk.offset)
    const data_path    = incomingChunk.data_path
    const chunk_size   = BigInt(b64UrlToBuffer(chunkData).length)

    const totalOffset = data_size - BigInt(1)

    ctx.logging.log(`Ingesting chunk`, {
      data_root,
      chunk_size,
      local_offset,
    })

    const placeholderChunk = await chunkDB.getByRootAndLocalOffset(data_root, totalOffset);
    if (local_offset === totalOffset) {
      // the incoming chunk needs to replace (update) the placeholder chunk
      // this should be the final chunk of this tx
      await chunkDB.update(placeholderChunk.id, {
        chunk: chunkData,
        chunk_size: chunk_size.toString(),
        data_path,
      })
      // and clean up the db in case someone re-uploaded a data_root with a different set of chunks
      await chunkDB.deleteOrphanedChunks(data_root)
    } else {
      // this is not the last chunk of this tx
      let shouldPersist = false
      const existingChunk = await chunkDB.getByRootAndLocalOffset(data_root, local_offset);
      if (!existingChunk) {
        // we don't have this chunk yet
        shouldPersist = true
      } else {
        // it seems like this chunk has been uploaded before, but
        // let's check if its length is different than the one we have
        if (existingChunk.chunk_size !== chunk_size.toString()) {
          shouldPersist = true
        }
      }
      if (shouldPersist) {
        const global_offset = BigInt(placeholderChunk.global_offset) + BigInt(1) - data_size + local_offset
        await chunkDB.create({
          chunk: chunkData,
          data_root,
          data_size: data_size.toString(),
          chunk_size: chunk_size.toString(),
          local_offset: local_offset.toString(),
          global_offset: global_offset.toString(),
          data_path,
        });
      }
    }

    ctx.body = {};
  } catch (error) {
    console.error({ error });
  }
}

export async function getChunkOffsetRoute(ctx: Router.RouterContext) {
  try {
    if (!chunkDB) {
      chunkDB = new ChunkDB(ctx.connection);
    }
    const global_offset = BigInt(ctx.params.offset);

    const chunk = await chunkDB.getByGlobalOffset(global_offset);

    if (!chunk) {
      ctx.status = 204;
      return;
    }

    const { chunk: chunkData, data_path } = chunk;

    ctx.body = { chunk: chunkData, data_path }
    return;
  } catch (error) {
    console.error({ error });
  }
}
