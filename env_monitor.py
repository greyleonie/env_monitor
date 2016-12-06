#!D:/Python27
import psycopg2
import pymssql
import time


# postgresql connection string
pg_host = "192.168.0.209"
pg_port = "5450"
pg_database = "emsMonitor"
pg_user = "postgres"
pg_password = ""

# mssql connection string
ms_host = "192.168.0.202"
ms_port = "1433"
ms_database = "dxtrainnew"
ms_user = "sa"
ms_password = "1qaz2wsx3edc4rfv.."

# mobile phone list
mobile = ("15918515785", "13682241959", "13620406845", "13580345465", "18819821389", "13316036387", "13560218614", "13570396545", "13642704686", "15999962756")
mobiles = "|".join(mobile)

# set of event id, save the id of not renormal events.
event_id_set = set()

print "Environment event monitor start..."
pg_start_id = pg_max_id = 0
# main loop
while True:
    
    # check postgres update
    pg_conn = psycopg2.connect(database=pg_database, user=pg_user, password=pg_password, host=pg_host, port=pg_port)
    pg_cur = pg_conn.cursor()
    pg_cur.execute("select max(id) from t_alarm;")
    pg_rows = pg_cur.fetchone()
    pg_max_id = pg_rows[0]

    if pg_start_id == 0 or pg_max_id <= pg_start_id:
        # no update
        pg_cur.close()
        pg_conn.close()
        pg_start_id = pg_max_id
    else:
        # read new data from postgres and write it into mssql 
        sql = "select id, happen_time, alarm_content, renormal_time, renormal_content from t_alarm where id>%d order by happen_time;" % (pg_start_id)
        pg_cur.execute(sql)
        pg_rows = pg_cur.fetchall()
        pg_cur.close()
        pg_conn.close()

        ms_conn = pymssql.connect(database=ms_database, user=ms_user, password=ms_password, host=ms_host, port=ms_port)
        ms_cur = ms_conn.cursor()
        
        for row in pg_rows:
            event_id = row[0]

            # send alarm message
            if event_id not in event_id_set:
                ms_cur.execute("select code from EOS_UNIQUE_TABLE where name='SendMsg.sendmsgid'")
                ms_rows = ms_cur.fetchone()
                ms_id = ms_rows[0] + 1
                sql = "update EOS_UNIQUE_TABLE set code=%d where name='SendMsg.sendmsgid'" % (ms_id)
                ms_cur.execute(sql)
                ms_conn.commit()

                sql = "insert into SendMsg (SendMsgID,DestAddrIsdnNum,MsgFormat,MsgContent,AuthorID) values (%d, '%s', %d, '%s %s', '%s');"\
                    % (ms_id, mobiles, 1, row[1], row[2], 16346)
                print sql
                ms_cur.execute(sql)
                ms_conn.commit()

                # add event id into set
                event_id_set.add(event_id)

            # send renormal message
            if row[3] != None:
                ms_cur.execute("select code from EOS_UNIQUE_TABLE where name='SendMsg.sendmsgid'")
                ms_rows = ms_cur.fetchone()
                ms_id = ms_rows[0] + 1
                sql = "update EOS_UNIQUE_TABLE set code=%d where name='SendMsg.sendmsgid'" % (ms_id)
                ms_cur.execute(sql)
                ms_conn.commit()

                sql = "insert into SendMsg (SendMsgID,DestAddrIsdnNum,MsgFormat,MsgContent,AuthorID) values (%d, '%s', %d, '%s %s', '%s');"\
                    % (ms_id, mobiles, 1, row[3], row[4], 16346)
                print sql
                ms_cur.execute(sql)
                ms_conn.commit()

                # remove id from set
                if event_id in event_id_set:
                    event_id_set.remove(event_id)
                    
        ms_cur.close()
        ms_conn.close()

        # update pg_start_id
        if len(event_id_set) != 0:
            # get min id of set
            is_first = True
            for event_id in event_id_set:
                if is_first == True:
                    min_id = event_id
                    is_first = False
                else:
                    if event_id < min_id:
                        min_id = event_id
            pg_start_id = min_id - 1
        else:
            pg_start_id = pg_max_id
    
    time.sleep(60)



