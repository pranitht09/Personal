qry_sp = "SELECT min(sn_end_time) FROM dummy_table_new"
start_time =
qry_ep = "SELECT max(sn_end_time) FROM dummy_table_new"
end_time =
cur_agg_time = start_time + 15
while (cur_agg_time < qry_ep):
    insert()
    start_time = cur_agg_time
    end_time = start_time+15