import csv
import psycopg2
import os

with open('user_data.csv') as f:
    csv_reader = csv.reader(f)
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            print(f'Column names are {", ".join(row)}')
            line_count += 1
        else:   
            #print(row)
            line_count += 1
    print(f'Processed {line_count} lines.')

uploaded_files = []
#csv_path = "\data"
def doPostgresCheckConnections():
    try:
        mydb = psycopg2.connect(host='localhost',database='postgres',user='postgres',password='Walgr33ns2',port=5432)
        cur = mydb.cursor()
        print("Cursor created")
        #qry = "SELECT * FROM dummy_table_new"
        #data_list = ['08/03/2018 10:27:11',100,'08/03/2018 10:27:11' ,400,443,1234567,-1]
        #qry1 = "INSERT INTO dummy_table_new (sn_start_time, sn_volume_amt_ip_bytes_uplink, sn_end_time, sn_volume_amt_ip_bytes_downlink, ip_protocol, bearer_3gpp_imsi, tcp_sn_tcp_accl_reject_reason) values (%s);"%str(data_list)[1:-1]
        for data_file in os.listdir(os.getcwd()):
            if data_file[-4:] != '.csv':
                continue
            if data_file in uploaded_files:
                continue
            with open data_file as f:
                csv_reader = csv.reader(f)
                line_count = 0
                for row in csv_reader:
                    print("CSV opened")
                    if line_count == 0:
                        print(f'Column names are {", ".join(row)}')
                        line_count += 1
                        pass
                    else:
                        line_count += 1
                        print(type(row))
                        qry1 = "INSERT INTO dummy_table_new (sn_start_time, sn_volume_amt_ip_bytes_uplink, sn_end_time, sn_volume_amt_ip_bytes_downlink, ip_protocol, bearer_3gpp_imsi, tcp_sn_tcp_accl_reject_reason) values ('%s',%d,'%s',%d,%d,%s,%d)"%(str(row[0][:-4]),int(row[1]),str(row[2][:-4]),int(row[3]),int(row[4]),row[5],int(row[6]))
                        #qry1 = "INSERT INTO dummy_table_new (sn_start_time, sn_volume_amt_ip_bytes_uplink, sn_end_time, sn_volume_amt_ip_bytes_downlink, ip_protocol, bearer_3gpp_imsi, tcp_sn_tcp_accl_reject_reason) values (%s);" %str(row)[1:-1]
                        print('printing query:')
                        print(qry1)
                        cur.execute(qry1)
                        mydb.commit()
                        uploaded_files.append(data_file)

                        print("Able to connect")

           print(f'Processed {line_count} lines.')
        print(qry1)

        '''
        out =[]
        if cur.description:
           colnames = [x[0] for x in cur.description]
           out = cur.fetchall()
           for i in range(len(out)):
               out[i] = dict(zip(colnames, out[i]))
        print(out)
        '''
        mydb.close()

    except:
        print('Error in getting connections -')
doPostgresCheckConnections()



