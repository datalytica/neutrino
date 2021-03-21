
import {
  Table, Visitor, Precision, Column, Field, Int,
  Float, Null, Bool, Utf8, Binary, FixedSizeBinary, Date_,
  Timestamp, Dictionary
} from "apache-arrow";

import { PerspectiveModule } from './psp';

import loadModule from "@internal/psp.async";

let Module: PerspectiveModule;

const ENV = {
/*    PSP_LOG_PROGRESS: false,
    PSP_LOG_DATA_GNODE_FLATTENED: true,
    PSP_LOG_DATA_GNODE_FLATTENED_MASK: true,
    PSP_LOG_DATA_GNODE_DELTA: true,
    PSP_LOG_DATA_GNODE_PREV: true,
    PSP_LOG_DATA_GNODE_CURRENT: true,
    PSP_LOG_DATA_GNODE_TRANSITIONS: true,
    PSP_LOG_DATA_GNODE_EXISTED: true,
    PSP_LOG_DATA_NSPARSE_STREE_PREV: true,
    PSP_LOG_DATA_NSPARSE_STREE_AFTER: true,
*/
}


/**
 * A lightweight wrapper around the perspective web assembly.
 *
 */
class WorkerHost {
  /**
   * Construct a new worker.
   *
   */
  constructor(scope: DedicatedWorkerGlobalScope) {
    this._scope = scope;

    let queued_listener: EventListener = (ev: MessageEvent) => {
      this._queued_messages.push(ev.data);
    };

    this._scope.addEventListener("message", queued_listener, false);

    loadModule({
      printErr: (x: any) => console.error(x),
      print: (x: any) => console.log(x),
      ENV: ENV
    }).then((instance: PerspectiveModule) => {
      Module = instance;

      this._pool = new Module.t_pool({
        _update_callback: () => this._poolUpdated()
      });

      // Start updating pool
      this._intervalID = setInterval(() => this._pool.process(), this._timeout);

      // Process currently cached messages
      while (this._queued_messages.length > 0) {
        let msg = this._queued_messages.shift();
        this._processMessage(msg);
      }

      this._scope.removeEventListener("message", queued_listener);
      this._scope.addEventListener("message", (ev: MessageEvent) => { this._processMessage(ev.data); }, false);
    });
  }

  shutdown() {
    // Worker has been asked to shutdown
    clearInterval(this._intervalID);
    this.postMessage({ cmd: "shutting_down" });
    this._scope.close();
  }

  postMessage(msg: any) {
    this._scope.postMessage(msg);
  }

  private _processMessage(msg: any) {
    switch (msg.cmd) {
      case 'create-table': {
        let cfg = msg.data as any;
        let { names, types, index } = cfg;
        let name = cfg.name as Private.TableName;

        try {
          let gnode = Module.make_gnode(names, types.map(Private.mapType), index);
          cfg.gnode_id = this._pool.register_gnode(gnode);

          this._table_map.set(name, cfg);
        } catch (e) {
          // Signal error to client
          console.error(e);
        }
        break;
      }
      case 'delete-table': {
        let config = msg.data as any;
        let name = config.name as Private.TableName;
        let table = this._table_map.get(name) as any;
        this._table_map.delete(name);

        let gnode = this._pool.get_gnode(table.gnode_id);
        this._pool.unregister_gnode(gnode.get_id());
        gnode.delete();
        break;
      }
      case 'add-computed': {
        let config= msg.data as any;
        let name = config.name as Private.TableName;
        let table = this._table_map.get(name) as any;
        let computed = config.computed as Array<any>;
        // rehydrate computed column functions
        for (let column of computed) {
          eval("column.func = " + column.func);
        }
        table.computed = computed;
        break;
      }
      case 'update': {
        let { name, records } = msg.data as any;
        let cfg = this._table_map.get(name) as any;
        let port = 0;

        if (cfg === undefined) {
          console.error("Table doesn't exist");
          break;
        }

        let nrecords = 0;
        let isArrow = false;
        let names = cfg.names;
        let cdata: Array<any>;
        let types = cfg.types.map(Private.mapType);

        if (records instanceof ArrayBuffer) {
          let pdata = Arrow.loadArrowBuffer(records);
          nrecords = pdata.nrecords;
          names = pdata.names;
          types = pdata.types;
          cdata = pdata.cdata;
          isArrow = true;
        } else {
          // Convert records to arrays of arrays
          cdata = [];
          for (let i = 0; i < names.length; ++i) {
            cdata.push([]);
          }
          for (let i = 0; i < records.length; ++i) {
            let record = records[i] as { [key: string]: any};
            for (let j = 0; j < names.length; ++j) {
              let name = names[j] as string;
              cdata[j].push(record[name]);
            }
          }
          nrecords = records.length;
        }

        let is_delete = false;

        let tbl;
        try {
          tbl = Module.make_table(nrecords, names, types,
            cdata, cfg.index, isArrow, is_delete);

          if (cfg.computed) {
            for (let { name, type, inputs, func } of cfg.computed) {
              let dtype = Private.mapType(type);
              Module.table_add_computed_column(tbl, name, dtype, func, inputs);
            }
          }

          this._pool.send(cfg.gnode_id, port, tbl);
        } catch (e) {
          // Signal error to client
          console.error(e);
        } finally {
          if (tbl) tbl.delete();
        }
        break;
      }
      case 'create-view': {
        let config = msg.data as any;
        let name = config.name as Private.ViewName;
        let table = this._table_map.get(config.table_name) as any;
        config.sortby_map = table.sortby_map;
        let gnode = this._pool.get_gnode(table.gnode_id);

        let schema = gnode.get_tblschema();
        let { context, context_type } = Private.generateContext(config, schema);
        schema.delete();

        this._pool.register_context(gnode.get_id(), name, context_type, context.$$.ptr);
        this._context_map.set(name, context);
        break;
      }
      case 'delete-view': {
        let config = msg.data as any;
        let name = config.name as Private.ViewName;
        let table = this._table_map.get(config.table_name) as any;
        let gnode = this._pool.get_gnode(table.gnode_id);

        this._pool.unregister_context(gnode.get_id(), name);
        let context = this._context_map.get(name);
        this._context_map.delete(name);
        context.delete();
        break;
      }
      case 'view-snapshot': {
        let { name } = msg.data as any;
        let context = this._context_map.get(name as Private.ViewName);
        this._sendSnapshot(name, context);
        break;
      }
      case 'set-view-depth': {
        let { name, depth, isColumn } = msg.data as any;
        let context = this._context_map.get(name as Private.ViewName);
        if (context.sidedness() === 1) {
          context.set_depth(depth);
        } else {
          if (isColumn) {
            context.set_depth(Module.t_header.HEADER_COLUMN, depth);
          } else {
            context.set_depth(Module.t_header.HEADER_ROW, depth);
          }
        }
        this._sendSnapshot(name, context);
        break;
      }
      case 'clear-selection': {
        let { name } = msg.data as any;
        let context = this._context_map.get(name as Private.ViewName);
        context.clear_selection();
        this._sendSnapshot(name, context);
        break;
      }
      case 'select-node': {
        let { name, start, end } = msg.data as any;
        let context = this._context_map.get(name as Private.ViewName);
        for (let i = start; i <= end; i++) {
          context.select_node(i);
        }
        this._sendSnapshot(name, context);
        break;
      }
      case 'deselect-node': {
        let { name, start, end } = msg.data as any;
        let context = this._context_map.get(name as Private.ViewName);
        for (let i = start; i <= end; i++) {
          context.deselect_node(i);
        }
        this._sendSnapshot(name, context);
        break;
      }
      case 'drill-to-child': {
        let { name, idx } = msg.data as any;
        let context = this._context_map.get(name as Private.ViewName);
        context.drill_to_child(idx);
        this._sendSnapshot(name, context);
        break;
      }
      case 'drill-to-top': {
        let { name } = msg.data as any;
        let context = this._context_map.get(name as Private.ViewName);
        context.drill_to_top();
        this._sendSnapshot(name, context);
        break;
      }
      case "shutdown": {
        this.shutdown();
        break;
      }
    }
  }

  private _sendSnapshot(name: Private.ViewName, context: Private.PerspectiveContext) {
    this.postMessage({
      cmd: 'snapshot',
      name: name,
      snapshot: Private.generateFlatSnapshot(context)
    })
  }

  private _poolUpdated() {
    let updates = this._pool.get_contexts_last_updated();

    for (let i = 0; i < updates.size(); ++i) {
      let upd = updates.get(i);
      // Pull out the data and send to the client
      let context = this._context_map.get(upd.ctx_name);

      let rc = context.get_row_count();
      let delta = context.get_step_delta(0, rc);

      let count = 0; //delta.cells.size();
      if (!(delta.row_changed || delta.columns_changed) && count > 0) {
        let cells = [];
        let ccount = delta.cells.size();
        for (let j = 0; j < ccount; ++j) {
          let cell = delta.cells.get(j);
          cell.old_value = Module.scalar_to_val(cell.old_value);
          cell.new_value = Module.scalar_to_val(cell.new_value);
          cells.push(cell);
        }

        this.postMessage({
          cmd: "update",
          name: upd.ctx_name,
          updates: cells
        });
      } else {
        this._sendSnapshot(upd.ctx_name, context);
      }

      delta.cells.delete();
    }
    updates.delete();
  }

  private _scope: DedicatedWorkerGlobalScope;

  private _queued_messages: Array<any> = [];

  private _pool: any;
  private _timeout: number = 500;
  private _intervalID: any;

  private _table_map: Map<Private.TableName, object> = new Map();
  private _context_map: Map<Private.ViewName, Private.PerspectiveContext> = new Map();
}


const scope: DedicatedWorkerGlobalScope = self as any;
if (typeof self !== "undefined" && self.addEventListener) {
  new WorkerHost(scope);
}

namespace Private {

  export type TableName = string;
  export type ViewName = string;

  export type PerspectiveContext = any;

  export
  function mapType(type: string): any {
    switch (type) {
      case "int32":
        return Module.t_dtype.DTYPE_INT32;
      case "integer":
        return Module.t_dtype.DTYPE_INT64;
      case "float32":
        return Module.t_dtype.DTYPE_FLOAT32;
      case "float":
        return Module.t_dtype.DTYPE_FLOAT64;
      case "boolean":
        return Module.t_dtype.DTYPE_BOOL;
      case "date":
        return Module.t_dtype.DTYPE_DATE;
      case "datetime":
        return Module.t_dtype.DTYPE_TIME;
      case "string":
      default:
        return Module.t_dtype.DTYPE_STR;
    }
  }

  const sortOrders = ["asc", "desc", "none", "asc abs", "desc abs"];

  function mapFilterOps(op: string): any {
    switch (op) {
      case "&": return Module.t_filter_op.FILTER_OP_AND;
      case "|": return Module.t_filter_op.FILTER_OP_OR;
      case "<": return Module.t_filter_op.FILTER_OP_LT;
      case ">": return Module.t_filter_op.FILTER_OP_GT;
      case "==": return Module.t_filter_op.FILTER_OP_EQ;
      case "contains": return Module.t_filter_op.FILTER_OP_CONTAINS;
      case "<=": return Module.t_filter_op.FILTER_OP_LTEQ;
      case ">=": return Module.t_filter_op.FILTER_OP_GTEQ;
      case "!=": return Module.t_filter_op.FILTER_OP_NE;
      case "begins with": return Module.t_filter_op.FILTER_OP_BEGINS_WITH;
      case "ends with": return Module.t_filter_op.FILTER_OP_ENDS_WITH;
      case "or": return Module.t_filter_op.FILTER_OP_OR;
      case "in": return Module.t_filter_op.FILTER_OP_IN;
      case "not in": return Module.t_filter_op.FILTER_OP_NOT_IN;
      case "and": return Module.t_filter_op.FILTER_OP_AND;
      case "is nan": return Module.t_filter_op.FILTER_OP_IS_NAN;
      case "is not nan": return Module.t_filter_op.FILTER_OP_IS_NOT_NA;
    }
  }

  function mapAggName(agg: string): any {
    switch (agg) {
      case "distinct": return Module.t_aggtype.AGGTYPE_DISTINCT_COUNT;
      case "sum": return Module.t_aggtype.AGGTYPE_SUM;
      case "mul": return Module.t_aggtype.AGGTYPE_MUL;
      case "avg": return Module.t_aggtype.AGGTYPE_MEAN;
      case "mean": return Module.t_aggtype.AGGTYPE_MEAN;
      case "count": return Module.t_aggtype.AGGTYPE_COUNT;
      case "weighted mean": return Module.t_aggtype.AGGTYPE_WEIGHTED_MEAN;
      case "unique": return Module.t_aggtype.AGGTYPE_UNIQUE;
      case "any": return Module.t_aggtype.AGGTYPE_ANY;
      case "median": return Module.t_aggtype.AGGTYPE_MEDIAN;
      case "join": return Module.t_aggtype.AGGTYPE_JOIN;
      case "div": return Module.t_aggtype.AGGTYPE_SCALED_DIV;
      case "add": return Module.t_aggtype.AGGTYPE_SCALED_ADD;
      case "dominant": return Module.t_aggtype.AGGTYPE_DOMINANT;
      case "first by index": return Module.t_aggtype.AGGTYPE_FIRST;
      case "last by index": return Module.t_aggtype.AGGTYPE_LAST;
      case "and": return Module.t_aggtype.AGGTYPE_AND;
      case "or": return Module.t_aggtype.AGGTYPE_OR;
      case "last": return Module.t_aggtype.AGGTYPE_LAST_VALUE;
      case "high": return Module.t_aggtype.AGGTYPE_HIGH_WATER_MARK;
      case "low": return Module.t_aggtype.AGGTYPE_LOW_WATER_MARK;
      case "sum abs": return Module.t_aggtype.AGGTYPE_SUM_ABS;
      case "sum not null": return Module.t_aggtype.AGGTYPE_SUM_NOT_NULL;
      case "mean by count": return Module.t_aggtype.AGGTYPE_MEAN_BY_COUNT;
      case "identity": return Module.t_aggtype.AGGTYPE_IDENTITY;
      case "distinct leaf": return Module.t_aggtype.AGGTYPE_DISTINCT_LEAF;
      case "pct sum parent": return Module.t_aggtype.AGGTYPE_PCT_SUM_PARENT;
      case "pct sum grand total": return Module.t_aggtype.AGGTYPE_PCT_SUM_GRAND_TOTAL;
      case "udf float": return Module.t_aggtype.AGGTYPE_UDF_JS_REDUCE_FLOAT64;
    }
  }

  function title(str: string) {
    return str.toLowerCase().split(' ').map(function(word) {
      return (word.charAt(0).toUpperCase() + word.slice(1));
    }).join(' ');
  }

  export
    function generateContext(config: any, schema: any)
    : { context: PerspectiveContext, context_type: any } {
    config = { ...config };

    config.row_pivot = config.row_pivot || [];
    config.column_pivot = config.column_pivot || [];

    // Filters
    let filters = [];
    let filter_op = Module.t_filter_op.FILTER_OP_AND;
    if (config.filter) {
      filters = config.filter.map((filter: [string, string, any]) => {
        return [filter[0], mapFilterOps(filter[1]), filter[2]];
      });
    }

    // Sort
    let sort_spec = [];
    if (config.sort) {
      sort_spec = config.sort.map((sort: [number, string]) => {
        return [sort[0], sortOrders.indexOf(sort[1])];
      });
    }

    // Sort by
    let sortby_map: any = {};
    if (config.sortby_map) {
      config.sortby_map.forEach((v: string, k: string) => {
        sortby_map[k] = v;
      });
    }

    // Aggregates
    let aggregates = [];
    if (config.aggregate) {
      for (let aidx = 0; aidx < config.aggregate.length; aidx++) {
        let agg = config.aggregate[aidx];
        let agg_op = mapAggName(agg.op);
        if (typeof agg.column === "string") {
          agg.column = [agg.column];
        } else {
          let dep_length = agg.column.length;
          if ((agg.op === "weighted mean" && dep_length != 2) || (agg.op !== "weighted mean" && dep_length != 1)) {
            throw `'${agg.op}' has incorrect arity ('${dep_length}') for column dependencies.`;
          }
        }

        let kernel;
        if (agg.op === "udf float") {
          eval(`kernel = ${agg.kernel}`);
        }

        let name = agg.name || (`${title(agg.op)} of (${agg.column.join('|')})`);
        aggregates.push([name, agg_op, agg.column, kernel]);
      }
    } else {
      let columns = schema.columns();
      for (let i = 0; i < columns.size(); ++i) {
        let column = columns.get(i);
        aggregates.push([column, Module.t_aggtype.AGGTYPE_ANY, [column]]);
      }
      columns.delete();
    }

    let context, type;
    if (config.row_pivot.length > 0 || config.column_pivot.length > 0) {
      if (config.column_pivot.length > 0) {
        type = Module.t_ctx_type.TWO_SIDED_CONTEXT;
        context = Module.make_context_two(schema, config.row_pivot, config.column_pivot, filter_op, filters, aggregates, sort_spec, sortby_map);

        context.set_depth(Module.t_header.HEADER_ROW, config.row_pivot.length);
        context.set_depth(Module.t_header.HEADER_COLUMN, config.column_pivot.length);
      } else {
        type = Module.t_ctx_type.ONE_SIDED_CONTEXT;
        context = Module.make_context_one(schema, config.row_pivot, filter_op, filters, aggregates, sort_spec, sortby_map);

        context.set_depth(config.row_pivot.length);
      }
    } else {
      type = Module.t_ctx_type.ZERO_SIDED_CONTEXT;
      context = Module.make_context_zero(schema, filter_op, filters, aggregates.map(agg => agg[2][0]), sort_spec);
    }

    return { context: context, context_type: type };
  }

  export
    function generateFlatSnapshot(context: any) {
    let start_row = 0;
    let start_col = 0;
    let schema: any = {};
    let header: Array<string> = [];
    let row_pivots: Array<string> = [];
    let column_pivots: Array<string> = [];
    let end_row, end_col, stride;

    let sides = context.sidedness();

    let columns = context.unity_get_column_names();
    let dtype_idx = (sides === 0) ? 0 : 1;

    for (let i = 0, end = columns.size(); i < end; ++i) {
      let column = columns.get(i);
      header.push(column);
      let type = context.get_column_dtype(i + dtype_idx).value;
      switch (type) {
        case 1:
        case 2:
          schema[column] = 'integer';
          break;
        case 19:
          schema[column] = 'string';
          break;
        case 9:
        case 10:
        case 17:
          schema[column] = 'float';
          break;
        case 11:
          schema[column] = 'boolean';
          break;
        case 12:
          schema[column] = 'datetime';
          break;
        case 13:
          schema[column] = 'date';
          break;
      }
    }

    columns.delete();

    let data: any[] = [];
    let row_spans: any[] = [];
    let col_spans: any[] = [];

    let selected_indices: number[] = []
    let idx = context.get_selected_indices();
    for (let i = 0; i < idx.size(); i++) {
        selected_indices.push(idx.get(i));
    }
    idx.delete();

    if (sides === 0) {
      end_col = context.unity_get_column_count();
      stride = end_col - start_col;

      end_row = context.unity_get_row_count();

      let slice = context.get_data(start_row, end_row, start_col, end_col);

      for (let r = start_row; r < end_row; r++) {
        let row = [];
        for (let c = start_col; c < end_col; c++) {
          row.push(Module.scalar_vec_to_val(slice, (r * stride) + c));
        }
        data.push(row);
      }
      slice.delete();
    } else {
      let row_depth = 0;
      let column_depth = 0;
      stride = 0;

      const rp = context.get_row_pivots();
      for(let i = 0, end = rp.size(); i < end; i++) {
        row_pivots.push(rp.get(i).name());
      }
      rp.delete();

      let slice;
      if (sides === 1) {
        row_depth = context.get_depth() + 1;
        end_col = context.unity_get_column_count() + row_depth;
        stride = end_col - start_col;
        end_row = context.get_leaf_count();
        slice = context.get_leaf_data(start_row, end_row, start_col, end_col);
      } else if (sides === 2) {
        row_depth = context.get_depth(Module.t_header.HEADER_ROW) + 1;
        end_col = context.unity_get_column_count() + row_depth;
        stride = end_col - start_col;
        column_depth = context.get_depth(Module.t_header.HEADER_COLUMN) + 1;
        end_row = context.get_leaf_count(Module.t_header.HEADER_ROW);
        slice = context.get_leaf_data(start_row, end_row, start_col, end_col);

        const cp = context.get_column_pivots();
        for(let i = 0, end = cp.size(); i < end; i++) {
          column_pivots.push(cp.get(i).name());
        }
        cp.delete();
      }

      let start = 0;
      if (column_depth > 0) {
        let unit_sep = String.fromCharCode(31);
        for (let d = 0; d < column_depth; d++) {
          col_spans.push([]);
        }
        for (let c = row_depth; c < stride; c++) {
          let val = Module.scalar_vec_to_val(slice, c);
          let span_reset = false;
          val = val.split(unit_sep);
          for (let i = 0; i < val.length; i++) {
            let span = col_spans[i];
            let curr = span[span.length - 1];
            if (span_reset || !curr || curr[0] !== val[i]) {
              curr = [val[i], 0];
              span.push(curr);
              span_reset = true;
            }
            curr[1] += 1;
          }
        }
        start += 1;
      }

      if (row_depth > 0) {
        for (let d = 0; d < row_depth; d++) {
          row_spans.push([]);
        }

        for (let r = start; r < end_row + start; r++) {
          let row = [];
          let header = [];
          for (let c = 0; c < stride; c++) {
            let val = Module.scalar_vec_to_val(slice, r * stride + c);

            if (c < row_depth - 1) {
              header.push(val);
            } else if (c === row_depth - 1) {
              header.push(val);

              let span_reset = false;
              for (let i = 0; i < header.length; i++) {
                let span = row_spans[i];
                let curr = span[span.length - 1];
                if (span_reset || !curr || curr[0] !== header[i]) {
                  curr = [header[i], 0];
                  span.push(curr);
                  span_reset = true;
                }
                curr[1] += 1;
              }
            } else {
              row.push(val);
            }
          }
          data.push(row);
        }

        let visit_layer = (depth: number, start: number, total: number): number => {
          let spans = row_spans[depth];
          let child_start = 0;
          let i = 0;
          for (let count = 0; (count < total) && (start + i < spans.length); ++i) {
            let span = spans[start + i];
            let [, c] = span;
            // If we aren't at the penultimate level, count our children
            if (depth < row_depth - 2) {
              let cc = visit_layer(depth + 1, child_start, c);
              child_start += cc;
              span.push(cc);
            } else {
              span.push(1);
            }
            count += c;
          }
          return i;
        }

        if (row_depth > 1) {
          visit_layer(0, 0, Number.POSITIVE_INFINITY);
        }
      }
      slice.delete();
    }

    return { 
      header: header,
      row_spans: row_spans,
      col_spans: col_spans,
      data: data,
      schema: schema,
      row_pivots: row_pivots,
      column_pivots: column_pivots,
      selected_indices: selected_indices,
    };
  }
}

namespace Arrow {
  /**
   * Converts arrow data into a canonical representation for
   * interfacing with perspective.
   *
   * @private
   * @param {object} data Array buffer
   * @returns An object with 3 properties:
   **/
  export
  function loadArrowBuffer(data: ArrayBuffer)
    : {nrecords: number, names: Array<any>, types: Array<any>, cdata: Array<Array<any>>} {
    // TODO Need to validate that the names/types passed in match those in the buffer
    let arrow = Table.from([new Uint8Array(data)]);
    let loader = arrow.schema.fields.reduce((loader: any, field: any, colIdx: any) => {
      return loader.loadColumn(field, arrow.getColumnAt(colIdx));
    }, new ArrowColumnLoader());

    if (typeof loader.cdata[0].values === "undefined") {
      let nchunks = loader.cdata[0]._chunks.length;
      let chunks = [];
      for (let x = 0; x < nchunks; x++) {
        chunks.push({
          nrecords: loader.cdata[0]._chunks[x].length,
          names: loader.names,
          types: loader.types,
          cdata: loader.cdata.map((y:any) => y._chunks[x])
        });
      }
      return chunks[0];
    } else {
      return {
          nrecords: arrow.length,
          names: loader.names,
          types: loader.types,
          cdata: loader.cdata
      };
    }
  }

  class ArrowColumnLoader extends Visitor {

    cdata: Array<any>;
    names: Array<any>;
    types: Array<any>;

    constructor() {
      super();
      this.cdata = [];
      this.names = [];
      this.types = [];
    }

    loadColumn(field: Field, column: Column) {
      if (this.visit(field.type)) {
        this.cdata.push(column);
        this.names.push(field.name);
      }
      return this;
    }
    visitNull(type: Null) {}
    visitBool(type: Bool) {
      this.types.push(Module.t_dtype.DTYPE_BOOL);
      return true;
    }
    visitInt(type: Int) {
      const bitWidth = type.bitWidth;
      if (bitWidth === 64) {
        this.types.push(Module.t_dtype.DTYPE_INT64);
      } else if (bitWidth === 32) {
        this.types.push(Module.t_dtype.DTYPE_INT32);
      } else if (bitWidth === 16) {
        this.types.push(Module.t_dtype.DTYPE_INT16);
      } else if (bitWidth === 8) {
        this.types.push(Module.t_dtype.DTYPE_INT8);
      }
      return true;
    }
    visitFloat(type: Float) {
      const precision = type.precision;
      if (precision === Precision.DOUBLE) {
        this.types.push(Module.t_dtype.DTYPE_FLOAT64);
      } else if (precision === Precision.SINGLE) {
        this.types.push(Module.t_dtype.DTYPE_FLOAT32);
      }
      // todo?
      // else if (type.precision === Precision.HALF) {
      //     this.types.push(Module.t_dtype.DTYPE_FLOAT16);
      // }
      return true;
    }
    visitUtf8(type: Utf8) {
      this.types.push(Module.t_dtype.DTYPE_STR);
      return true;
    }
    visitBinary(type: Binary) {
      this.types.push(Module.t_dtype.DTYPE_STR);
      return true;
    }
    visitFixedSizeBinary(type: FixedSizeBinary) {}
    visitDate(type: Date_) {
      this.types.push(Module.t_dtype.DTYPE_DATE);
      return true;
    }
    visitTimestamp(type: Timestamp) {
      this.types.push(Module.t_dtype.DTYPE_TIME);
      return true;
    }
    visitDictionary(type : Dictionary) {
      return this.visit(type.dictionary);
    }
    /*visitTime(type: Time) {}
    visitDecimal(type: Decimal) {}
    visitList(type: List) {}
    visitStruct(type: Struct) {}
    visitUnion(type: Union<any>) {}
    visitInterval(type: Interval) {}
    visitFixedSizeList(type: FixedSizeList) {}
    visitMap(type: Map_) {} */
  }
}
