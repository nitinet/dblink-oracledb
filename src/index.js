import { Handler, model } from 'dblink-core';
import oracledb from 'oracledb';
export default class Oracle extends Handler {
  connectionPool;
  constructor(config) {
    super(config);
  }
  async init() {
    this.connectionPool = await oracledb.createPool(this.config);
  }
  async getConnection() {
    const conn = await oracledb.getConnection(this.config);
    return conn;
  }
  async initTransaction() {}
  async commit(conn) {
    return conn.commit();
  }
  async rollback(conn) {
    return conn.rollback();
  }
  async close(conn) {
    return conn.close();
  }
  async run(query, dataArgs, connection) {
    dataArgs = dataArgs ?? [];
    let temp;
    if (connection) {
      temp = await connection.execute(query, dataArgs);
    } else {
      const conn = await this.connectionPool.getConnection();
      try {
        temp = await conn.execute(query, dataArgs);
      } finally {
        conn.close();
      }
    }
    const result = new model.ResultSet();
    result.rows = temp.rows ?? [];
    return result;
  }
  runStatement(queryStmt, connection) {
    const { query, dataArgs } = this.prepareQuery(queryStmt);
    return this.run(query, dataArgs, connection);
  }
  async stream(query, dataArgs, connection) {
    dataArgs = dataArgs ?? [];
    let stream;
    if (connection) {
      stream = connection.queryStream(query, dataArgs);
    } else {
      const conn = await this.connectionPool.getConnection();
      stream = conn.queryStream(query, dataArgs);
      stream.on('end', function () {
        stream.destroy();
      });
      stream.on('close', function () {
        conn.close();
      });
    }
    return stream;
  }
  streamStatement(queryStmt, connection) {
    const { query, dataArgs } = this.prepareQuery(queryStmt);
    return this.stream(query, dataArgs, connection);
  }
}
//# sourceMappingURL=index.js.map
