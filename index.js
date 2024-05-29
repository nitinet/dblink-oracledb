import { Handler, model } from 'dblink-core';
import oracledb from 'oracledb';
export default class Oracle extends Handler {
    connectionPool;
    async init() {
        this.connectionPool = await oracledb.createPool({
            user: this.config.username,
            password: this.config.password,
            connectString: `${this.config.host}:${this.config.port}/${this.config.database}`
        });
    }
    async getConnection() {
        let conn = await oracledb.getConnection({
            user: this.config.username,
            password: this.config.password,
            connectString: `${this.config.host}:${this.config.port}/${this.config.database}`
        });
        return conn;
    }
    async initTransaction(conn) {
    }
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
        }
        else {
            let conn = await this.connectionPool.getConnection();
            try {
                temp = await conn.execute(query, dataArgs);
            }
            finally {
                conn.close();
            }
        }
        let result = new model.ResultSet();
        result.rows = temp.rows ?? [];
        result.rowCount = temp.rowsAffected ?? 0;
        return result;
    }
    runStatement(queryStmt, connection) {
        let { query, dataArgs } = this.prepareQuery(queryStmt);
        return this.run(query, dataArgs, connection);
    }
    async stream(query, dataArgs, connection) {
        dataArgs = dataArgs ?? [];
        let stream;
        if (connection) {
            stream = connection.queryStream(query, dataArgs);
        }
        else {
            let conn = await this.connectionPool.getConnection();
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
        let { query, dataArgs } = this.prepareQuery(queryStmt);
        return this.stream(query, dataArgs, connection);
    }
}
//# sourceMappingURL=index.js.map