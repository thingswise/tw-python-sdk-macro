import d2i_queries

def apply(defs=None, args=None, macro="timeseries"):
  if not defs:
    return

  if not args:
    return

  print args

  stream = args["stream"]

  keys = None
  if "keys" in args:
    if isinstance(args["keys"], list) and len(args["keys"]) > 0:
      keys = d2i_queries.get_input_keys(args)
    else:
      raise ValueError("Invalid keys list in macro %s" % macro)

  values = None
  if "values" in args:
    if isinstance(args["values"], list) and len(args["values"]) > 0:
      values = args["values"]
    else:
      raise ValueError("Invalid values list in macro %s" % macro)

  period = args["period"] if "period" in args else "1s"

  if "input" in defs:
    if stream in defs["input"]:
      sv = defs["input"][stream]
      keys = keys if keys else d2i_queries.get_input_keys(sv)
      values = values if values else sv["values"]

      if values and len(values) > 0:

        available_values = set()
        if "values" in sv and isinstance(sv["values"], list):
          for v in sv["values"]:
            if type(v) == str:
              available_values.add(v)
            elif type(v) == dict and len(v) == 1:
              for k,_ in v.iteritems():
                available_values.add(k)
            else:
              raise ValueError("Invalid value definition: %s" % v)

        for v in values:
          vname = None
          if type(v) == str:
            vname = v
          elif type(v) == dict and len(v) == 1:
            for k,_ in v.iteritems():
              vname = k
          else:
            raise ValueError("Invalid value definition: %s" % v)

          if vname not in available_values:
            sv["values"].append(v)
            available_values.add(vname)

          query = "%s for %s by %s" % (vname, d2i_queries.dump_vars(keys), period)
          queries = defs["queries"] = defs["queries"] if "queries" in defs else {}
          timeseries = queries["timeseries"] = queries["timeseries"] if "timeseries" in queries else []
          timeseries.append(query)

