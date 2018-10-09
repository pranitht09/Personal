import psycopg2

def doPostgresCheckConnections():
    try:
        mydb = psycopg2.connect(host='localhost',database='postgres',user='postgres',password='Walgr33ns2',port=5432)
        cur = mydb.cursor()
        print("Cursor created")
        query = "SELECT SUM(sn_volume_amt_ip_bytes_uplink+sn_volume_amt_ip_bytes_downlink) FROM dummy_table_new WHERE  (bearer_3gpp_imsi = 311480000000000) AND (sn_end_time >= (SELECT min(sn_end_time) FROM dummy_table_new)) AND (sn_end_time <= ((SELECT min(sn_end_time) FROM dummy_table_new)+ (15 * interval '1 minute')))"
        # print('printing query:')
        # print(query)
        cur.execute(query)
        for row in cur:
             print (row)
        # query2 = "INSERT INTO aggregate_bytes (aggregate_bytes_sum,sn_end_time_reference, beaer_3gpp_imsi) values ( %d, '%s', %s)"
        # cur.execute(query2)
        # mydb.commit()
        # mydb.close()
    except:
     print('Error in getting connections')


#doPostgresCheckConnections()

def insert():
    mydb = psycopg2.connect(host='localhost', database='postgres', user='postgres', password='Walgr33ns2', port=5432)
    cur = mydb.cursor()
    query = "SELECT SUM(sn_volume_amt_ip_bytes_uplink+sn_volume_amt_ip_bytes_downlink) FROM dummy_table_new WHERE  (bearer_3gpp_imsi = 311480000000000) AND (sn_end_time >= (SELECT min(sn_end_time) FROM dummy_table_new)) AND (sn_end_time <= ((SELECT min(sn_end_time) FROM dummy_table_new)+ (15 * interval '1 minute')))"
        # print('printing query:')
        # print(query)
    agg_val = cur.execute(query)
    rowss = cur.fetchall()
    print(str(rowss)[2:-3])
    st_time = '2018-08-03 10:27:10'
    bearer_imsi = 311480000000000
    end_time = '2018-08-03 10:31:25'
    query2 = "INSERT INTO aggregate_bytes (aggregate_bytes_sum,sn_end_time_reference, beaer_3gpp_imsi) values ( %d, '%s', %d);"%(int(str(rowss)[2:-3]),end_time,bearer_imsi)
    print(query2)
    cur.execute(query2)
    mydb.commit()
    mydb.close()
insert()