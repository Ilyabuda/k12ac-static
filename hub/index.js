import express from 'express';
import { createServer } from 'node:http';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { Server } from 'socket.io';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import { availableParallelism } from 'node:os';
import cluster from 'node:cluster';
import { createAdapter, setupPrimary } from '@socket.io/cluster-adapter';

if (cluster.isPrimary) {
  const numCPUs = availableParallelism();
  // Fork workers.
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork({
      PORT: 3000 + i
    });
  }

  setupPrimary();
} else {
  const db = await open({
    filename: 'chat.db',
    driver: sqlite3.Database
  });

  // UPDATED: Added 'username' column to schema
  await db.exec(`
    CREATE TABLE IF NOT EXISTS messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      client_offset TEXT UNIQUE,
      content TEXT,
      username TEXT
    );
  `);

  const app = express();
  const server = createServer(app);
  const io = new Server(server, {
    connectionStateRecovery: {},
    adapter: createAdapter()
  });

  const __dirname = dirname(fileURLToPath(import.meta.url));

  app.get('/', (req, res) => {
    res.sendFile(join(__dirname, 'hub.html'));
  });

  io.on('connection', async (socket) => {
    // UPDATED: Destructure msg object to get user and text
    socket.on('chat message', async (msgObj, clientOffset, callback) => {
      let result;
      try {
        // UPDATED: Insert username and content
        // msgObj is expected to be { user: "Name", text: "Hello" }
        result = await db.run(
            'INSERT INTO messages (content, username, client_offset) VALUES (?, ?, ?)', 
            msgObj.text, 
            msgObj.user, 
            clientOffset
        );
      } catch (e) {
        if (e.errno === 19 /* SQLITE_CONSTRAINT */ ) {
          callback();
        } else {
          // nothing to do, just let the client retry
        }
        return;
      }
      // UPDATED: Emit the full object back to clients
      io.emit('chat message', { user: msgObj.user, text: msgObj.text }, result.lastID);
      callback();
    });

    if (!socket.recovered) {
      try {
        // UPDATED: Select username as well
        await db.each('SELECT id, content, username FROM messages WHERE id > ?',
          [socket.handshake.auth.serverOffset || 0],
          (_err, row) => {
            // UPDATED: Emit object structure
            socket.emit('chat message', { user: row.username, text: row.content }, row.id);
          }
        )
      } catch (e) {
        // something went wrong
      }
    }
  });

  const port = process.env.PORT;

  server.listen(port, () => {
    console.log(`server running at http://localhost:${port}`);
  });
}