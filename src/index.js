import { Handler, model } from 'dblink-core';
import oracledb from 'oracledb';
export default class Oracle extends Handler {
  connectionPool;
  constructor(config) {
    super(config);
    this.config = config;
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW5kZXguanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyJpbmRleC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQSxPQUFPLEVBQUUsT0FBTyxFQUFFLEtBQUssRUFBTyxNQUFNLGFBQWEsQ0FBQztBQUNsRCxPQUFPLFFBQVEsTUFBTSxVQUFVLENBQUM7QUFXaEMsTUFBTSxDQUFDLE9BQU8sT0FBTyxNQUFPLFNBQVEsT0FBTztJQU16QyxjQUFjLENBQWlCO0lBUS9CLFlBQVksTUFBK0I7UUFDekMsS0FBSyxDQUFDLE1BQU0sQ0FBQyxDQUFDO1FBRWQsSUFBSSxDQUFDLE1BQU0sR0FBRyxNQUFNLENBQUM7SUFDdkIsQ0FBQztJQVFELEtBQUssQ0FBQyxJQUFJO1FBQ1IsSUFBSSxDQUFDLGNBQWMsR0FBRyxNQUFNLFFBQVEsQ0FBQyxVQUFVLENBQUMsSUFBSSxDQUFDLE1BQWlDLENBQUMsQ0FBQztJQUMxRixDQUFDO0lBUUQsS0FBSyxDQUFDLGFBQWE7UUFDakIsTUFBTSxJQUFJLEdBQUcsTUFBTSxRQUFRLENBQUMsYUFBYSxDQUFDLElBQUksQ0FBQyxNQUF1QyxDQUFDLENBQUM7UUFDeEYsT0FBTyxJQUFJLENBQUM7SUFDZCxDQUFDO0lBU0QsS0FBSyxDQUFDLGVBQWU7SUFHckIsQ0FBQztJQVNELEtBQUssQ0FBQyxNQUFNLENBQUMsSUFBeUI7UUFDcEMsT0FBTyxJQUFJLENBQUMsTUFBTSxFQUFFLENBQUM7SUFDdkIsQ0FBQztJQVNELEtBQUssQ0FBQyxRQUFRLENBQUMsSUFBeUI7UUFDdEMsT0FBTyxJQUFJLENBQUMsUUFBUSxFQUFFLENBQUM7SUFDekIsQ0FBQztJQVNELEtBQUssQ0FBQyxLQUFLLENBQUMsSUFBeUI7UUFDbkMsT0FBTyxJQUFJLENBQUMsS0FBSyxFQUFFLENBQUM7SUFDdEIsQ0FBQztJQVdELEtBQUssQ0FBQyxHQUFHLENBQUMsS0FBYSxFQUFFLFFBQW9CLEVBQUUsVUFBZ0M7UUFDN0UsUUFBUSxHQUFHLFFBQVEsSUFBSSxFQUFFLENBQUM7UUFDMUIsSUFBSSxJQUE4QyxDQUFDO1FBQ25ELElBQUksVUFBVSxFQUFFLENBQUM7WUFDZixJQUFJLEdBQUcsTUFBTSxVQUFVLENBQUMsT0FBTyxDQUFDLEtBQUssRUFBRSxRQUFRLENBQUMsQ0FBQztRQUNuRCxDQUFDO2FBQU0sQ0FBQztZQUNOLE1BQU0sSUFBSSxHQUFHLE1BQU0sSUFBSSxDQUFDLGNBQWMsQ0FBQyxhQUFhLEVBQUUsQ0FBQztZQUN2RCxJQUFJLENBQUM7Z0JBQ0gsSUFBSSxHQUFHLE1BQU0sSUFBSSxDQUFDLE9BQU8sQ0FBQyxLQUFLLEVBQUUsUUFBUSxDQUFDLENBQUM7WUFDN0MsQ0FBQztvQkFBUyxDQUFDO2dCQUNULElBQUksQ0FBQyxLQUFLLEVBQUUsQ0FBQztZQUNmLENBQUM7UUFDSCxDQUFDO1FBRUQsTUFBTSxNQUFNLEdBQUcsSUFBSSxLQUFLLENBQUMsU0FBUyxFQUFFLENBQUM7UUFDckMsTUFBTSxDQUFDLElBQUksR0FBRyxJQUFJLENBQUMsSUFBSSxJQUFJLEVBQUUsQ0FBQztRQUM5QixPQUFPLE1BQU0sQ0FBQztJQUNoQixDQUFDO0lBU0QsWUFBWSxDQUFDLFNBQTBDLEVBQUUsVUFBZ0M7UUFDdkYsTUFBTSxFQUFFLEtBQUssRUFBRSxRQUFRLEVBQUUsR0FBRyxJQUFJLENBQUMsWUFBWSxDQUFDLFNBQVMsQ0FBQyxDQUFDO1FBQ3pELE9BQU8sSUFBSSxDQUFDLEdBQUcsQ0FBQyxLQUFLLEVBQUUsUUFBUSxFQUFFLFVBQVUsQ0FBQyxDQUFDO0lBQy9DLENBQUM7SUFXRCxLQUFLLENBQUMsTUFBTSxDQUFDLEtBQWEsRUFBRSxRQUFvQixFQUFFLFVBQWdDO1FBQ2hGLFFBQVEsR0FBRyxRQUFRLElBQUksRUFBRSxDQUFDO1FBQzFCLElBQUksTUFBZ0IsQ0FBQztRQUNyQixJQUFJLFVBQVUsRUFBRSxDQUFDO1lBQ2YsTUFBTSxHQUFHLFVBQVUsQ0FBQyxXQUFXLENBQUMsS0FBSyxFQUFFLFFBQVEsQ0FBQyxDQUFDO1FBQ25ELENBQUM7YUFBTSxDQUFDO1lBQ04sTUFBTSxJQUFJLEdBQUcsTUFBTSxJQUFJLENBQUMsY0FBYyxDQUFDLGFBQWEsRUFBRSxDQUFDO1lBRXZELE1BQU0sR0FBRyxJQUFJLENBQUMsV0FBVyxDQUFDLEtBQUssRUFBRSxRQUFRLENBQUMsQ0FBQztZQUMzQyxNQUFNLENBQUMsRUFBRSxDQUFDLEtBQUssRUFBRTtnQkFDZixNQUFNLENBQUMsT0FBTyxFQUFFLENBQUM7WUFDbkIsQ0FBQyxDQUFDLENBQUM7WUFDSCxNQUFNLENBQUMsRUFBRSxDQUFDLE9BQU8sRUFBRTtnQkFDakIsSUFBSSxDQUFDLEtBQUssRUFBRSxDQUFDO1lBQ2YsQ0FBQyxDQUFDLENBQUM7UUFDTCxDQUFDO1FBQ0QsT0FBTyxNQUFNLENBQUM7SUFDaEIsQ0FBQztJQVNELGVBQWUsQ0FBQyxTQUEwQyxFQUFFLFVBQWdDO1FBQzFGLE1BQU0sRUFBRSxLQUFLLEVBQUUsUUFBUSxFQUFFLEdBQUcsSUFBSSxDQUFDLFlBQVksQ0FBQyxTQUFTLENBQUMsQ0FBQztRQUN6RCxPQUFPLElBQUksQ0FBQyxNQUFNLENBQUMsS0FBSyxFQUFFLFFBQVEsRUFBRSxVQUFVLENBQUMsQ0FBQztJQUNsRCxDQUFDO0NBQ0YifQ==
