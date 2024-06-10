import { Handler, model, sql } from "dblink-core";
import oracledb from "oracledb";
import { Readable } from "stream";

/**
 * Oracle Handler
 *
 * @export
 * @class Oracle
 * @typedef {Oracle}
 * @extends {Handler}
 */
export default class Oracle extends Handler {
  /**
   * Connection Pool
   *
   * @type {!oracledb.Pool}
   */
  connectionPool!: oracledb.Pool;

  /**
   * Handler initialisation
   *
   * @async
   * @returns {Promise<void>}
   */
  async init(): Promise<void> {
    this.connectionPool = await oracledb.createPool({
      user: this.config.username,
      password: this.config.password,
      connectString: `${this.config.host}:${this.config.port}/${this.config.database}`,
    });
  }

  /**
   * Get a new Connection
   *
   * @async
   * @returns {Promise<oracledb.Connection>}
   */
  async getConnection(): Promise<oracledb.Connection> {
    const conn = await oracledb.getConnection({
      user: this.config.username,
      password: this.config.password,
      connectString: `${this.config.host}:${this.config.port}/${this.config.database}`,
    });
    return conn;
  }

  /**
   * Initialize a Transaction
   *
   * @async
   * @param {oracledb.Connection} conn
   * @returns {Promise<void>}
   */
  async initTransaction(): Promise<void> {
    // conn: oracledb.Connection
    /* document why this async method 'initTransaction' is empty */
  }

  /**
   * Commit Transaction
   *
   * @async
   * @param {oracledb.Connection} conn
   * @returns {Promise<void>}
   */
  async commit(conn: oracledb.Connection): Promise<void> {
    return conn.commit();
  }

  /**
   * Rollback Transaction
   *
   * @async
   * @param {oracledb.Connection} conn
   * @returns {Promise<void>}
   */
  async rollback(conn: oracledb.Connection): Promise<void> {
    return conn.rollback();
  }

  /**
   * Close Connection
   *
   * @async
   * @param {oracledb.Connection} conn
   * @returns {Promise<void>}
   */
  async close(conn: oracledb.Connection): Promise<void> {
    return conn.close();
  }

  /**
   * Run string query
   *
   * @async
   * @param {string} query
   * @param {?any[]} [dataArgs]
   * @param {?oracledb.Connection} [connection]
   * @returns {Promise<model.ResultSet>}
   */
  async run(
    query: string,
    dataArgs?: unknown[],
    connection?: oracledb.Connection,
  ): Promise<model.ResultSet> {
    dataArgs = dataArgs ?? [];
    let temp: oracledb.Result<Record<string, unknown>>;
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
    result.rowCount = temp.rowsAffected ?? 0;
    return result;
  }

  /**
   * Run statements
   *
   * @param {(sql.Statement | sql.Statement[])} queryStmt
   * @param {?oracledb.Connection} [connection]
   * @returns {Promise<model.ResultSet>}
   */
  runStatement(
    queryStmt: sql.Statement | sql.Statement[],
    connection?: oracledb.Connection,
  ): Promise<model.ResultSet> {
    const { query, dataArgs } = this.prepareQuery(queryStmt);
    return this.run(query, dataArgs, connection);
  }

  /**
   * Run quries and stream output
   *
   * @async
   * @param {string} query
   * @param {?any[]} [dataArgs]
   * @param {?oracledb.Connection} [connection]
   * @returns {Promise<Readable>}
   */
  async stream(
    query: string,
    dataArgs?: unknown[],
    connection?: oracledb.Connection,
  ): Promise<Readable> {
    dataArgs = dataArgs ?? [];
    let stream: Readable;
    if (connection) {
      stream = connection.queryStream(query, dataArgs);
    } else {
      const conn = await this.connectionPool.getConnection();

      stream = conn.queryStream(query, dataArgs);
      stream.on("end", function () {
        stream.destroy();
      });
      stream.on("close", function () {
        conn.close();
      });
    }
    return stream;
  }

  /**
   * Run statements and stream output
   *
   * @param {(sql.Statement | sql.Statement[])} queryStmt
   * @param {?oracledb.Connection} [connection]
   * @returns {Promise<Readable>}
   */
  streamStatement(
    queryStmt: sql.Statement | sql.Statement[],
    connection?: oracledb.Connection,
  ): Promise<Readable> {
    const { query, dataArgs } = this.prepareQuery(queryStmt);
    return this.stream(query, dataArgs, connection);
  }
}
