

export
interface Module {
  printErr?: Function;
  print?: Function;
  onRuntimeInitialized?: Function;
  ENV?: any,
}

export
interface PerspectiveModule extends Module {
  t_pool: any;
  t_dtype: any;
  t_filter_op: any;
  t_header: any;
  t_aggtype: any;
  t_ctx_type: any;
  make_table: Function;
  make_gnode: Function;
  gnode_add_computed: Function;
  make_context_zero: Function;
  make_context_one: Function;
  make_context_two: Function;
  scalar_vec_to_val: Function;
  scalar_to_val: Function;
  sort: Function;
  fill: Function;
}
