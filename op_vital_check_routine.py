##### functions for DB searching

import pandas as pd
import numpy as np
import pymysql
import datetime
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
from matplotlib import rc

server_ip = '192.168.44.106'
user_id = 'sa_server'
user_passwd = 'qwer1234!'
db_name = 'sa_server'

# def get_table_fields(table_name='number_gec'):
def get_table_fields(table_name):
    #############################################################
    #### input parameter
    #### -- table_name: (str) name of each DB table
    #### output: all column info. of input table
    #### -- id: (int) auto increment index
    #### -- upload_date, begin_date, end_date: (datetime(6), UTC)
    #### -- file_path: filepath+filename
    #### -- file_basename: file name only
    #### -- bed_id: rosette (or ICU bed) index
    #### -- client_id: acquisition device index
    #############################################################
    conn = pymysql.connect(host=server_ip, user=user_id, password=user_passwd,
                       db=db_name, charset='utf8')
    curs = conn.cursor()
    sql ="""show columns from """+table_name + """;"""
    curs.execute(sql)
    result = curs.fetchall()
    result = np.array(result)

    curs.close()
    conn.close()
    return result


def get_op_table(start, end):
    
    start_datetime = datetime.datetime.strptime(str(start), '%Y%m%d') - datetime.timedelta(hours=9)
    start = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
    
    end_datetime = datetime.datetime.strptime(str(end), '%Y%m%d') - datetime.timedelta(hours=9)
    end = end_datetime.strftime("%Y-%m-%d %H:%M:%S")
    
    conn = pymysql.connect(host=server_ip, user=user_id, password=user_passwd,
                       db=db_name, charset='utf8')
    curs = conn.cursor()
    sql = """select name, start_date, end_date from sa_api_op as op join sa_api_bed as bed on op.bed_id = bed.id where start_date> %s and end_date< %s;"""
#     sql = """select id, start_date, end_date from sa_api_op where start_date> %s and end_date< %s;"""
    curs.execute(sql, (start, end))
    result = curs.fetchall()
    result = np.array(result)

    curs.close()
    conn.close()
    return result[:,0], result[:,1], result[:,2]




def get_op_table_oneday(start):

#     end = str(int(int(start)))
#     end = end[0:4]+'-'+end[4:6]+'-'+end[6:8]+' 15:00:00'
#     start = str(int(int(start)-1))
#     start = start[0:4]+'-'+start[4:6]+'-'+start[6:8]+' 15:00:00'

    start_datetime = datetime.datetime.strptime(str(start), '%Y%m%d') - datetime.timedelta(hours=9)
    end_datetime = datetime.datetime.strptime(str(start), '%Y%m%d') + datetime.timedelta(hours=20)

    start = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
    end = end_datetime.strftime("%Y-%m-%d %H:%M:%S")

    
    conn = pymysql.connect(host=server_ip, user=user_id, password=user_passwd,
                       db=db_name, charset='utf8')
    curs = conn.cursor()
    sql = """select name, start_date, end_date from sa_api_op as op join sa_api_bed as bed on op.bed_id = bed.id where start_date> %s and end_date< %s;"""
    curs.execute(sql, (start, end))
    result = curs.fetchall()
    result = np.array(result)

    curs.close()
    conn.close()
    
    try:
        return result[:,0], result[:,1], result[:,2]
    except:
        print("No OP-time data")
        return -1

def only_target_room(bb):
    
    bb = np.array(bb)
    target_room = ['D-01', 'D-02', 'D-03', 'D-04', 'D-05', 'D-06', 'J-03', 'K-01']
    ind = []
    for l in target_room:
        this_ind = np.where(bb[0]==l)[0]
        ind = np.append(ind, this_ind)
    
    ind = np.unique(ind).astype(int)

    bb = bb[:,ind]
    

    return bb



def get_filerecorded_data(date=''):
    conn = pymysql.connect(host=server_ip, user=user_id, password=user_passwd,
                   db=db_name, charset='utf8')
    curs = conn.cursor()
    
#     print(date)
#     date = '_221024_'
    date = '_'+date[2:]+'@_'

    if date!='':
        sql ="""select id, begin_date, end_date, file_path, file_basename from sa_api_filerecorded where file_basename like %s escape '@' and end_date != '1970-01-01' order by id  """
        curs.execute(sql,('%'+date+'%'))
#         curs.execute(sql,(str(date)+'%'))
    else:
        sql ="""select id, begin_date, end_date, file_path, file_basename from sa_api_filerecorded where end_date != '1970-01-01' order by id """
        curs.execute(sql)
    result = curs.fetchall()
    result = np.array(result)

    curs.close()
    conn.close()
    
    record_id = result[:,0]
    begin_date = result[:,1]
    end_date = result[:,2]
    file_path = result[:,3]
    file_basename = result[:,4]
    
    date = np.array([l[l.find('_')+1:l.find('_')+7] for l in file_basename])
    room = np.array([l[:l.find('_')] for l in file_basename])
    
    a = np.array([l[0] for l in date]).astype(int)
    ind = np.where(a!=7)[0]
    
    try:
        return record_id[ind], begin_date[ind], end_date[ind], file_path[ind], file_basename[ind], date[ind], room[ind]
    except:
        print("No file data")
        return -1

 
    
def error_exclusion(file_info):
    ### empty end_date
    ### 1970
    ### end_date < begin_date
    ### delta time > 24hours
    ### only for OR data
    aa = np.array(np.copy(file_info))

    bb = aa[:,aa[2]!=None]
    bb = bb[:,bb[1]>datetime.datetime(2017, 1, 1)]
    bb = bb[:,bb[2]>datetime.datetime(2017, 1, 1)]

    ### remain only OP room
    sp_room = ['ICU', 'IPACU', 'REC', 'TEST', 'Rese']
    for l in sp_room:
    #     print(l, len(np.flatnonzero(np.core.defchararray.find(bb[3].astype(str), l)!=-1)))
        ind = np.flatnonzero(np.core.defchararray.find(bb[6].astype(str), l)==-1)
        bb = bb[:,ind]

    bb = bb[:,bb[2]-bb[1]>datetime.timedelta(seconds=0)]
    bb = bb[:,bb[2]-bb[1]<datetime.timedelta(hours=24)]

#     print(len(bb[0]))

#     # find larger than 24 hours
#     ind = np.where(bb[2]-bb[1]>datetime.timedelta(hours=24))[0]
#     print(len(ind))
#     for i in ind:
#         try:
#             print(bb[1,i],'/', bb[2,i],'/', bb[3,i],'/', os.path.getsize('/srv/vital_amc/'+bb[3,i][5:])/1024/1024)
#         except:
#             print(bb[1,i],'/', bb[2,i],'/', bb[3,i],'/', 'nofile in 193')

    # find reversed file
#     ind = np.where(bb[2]-bb[1]<datetime.timedelta(seconds=0))[0]
#     print(len(ind))
#     for i in ind:
#         try:
#             print(bb[1,i],'/', bb[2,i],'/', bb[3,i],'/', os.path.getsize('/srv/vital_amc/'+bb[3,i][5:])/1024/1024)
#         except:
#             print(bb[1,i],'/', bb[2,i],'/', bb[3,i],'/', 'nofile in 193')
    
    return new_file_info




def error_exclusion_for_oneday(file_info):
    ### empty end_date
    ### 1970
    ### end_date < begin_date
    ### delta time > 24hours
    ### only for OR data
    bb = np.array(np.copy(file_info))

    ### remain only OP room
#     sp_room = ['ICU', 'IPACU', 'REC', 'TEST', 'Rese', 'OB']
#     for l in sp_room:
#     #     print(l, len(np.flatnonzero(np.core.defchararray.find(bb[3].astype(str), l)!=-1)))
#         ind = np.flatnonzero(np.core.defchararray.find(bb[6].astype(str), l)==-1)
#         bb = bb[:,ind]
    target_room = ['D-01', 'D-02', 'D-03', 'D-04', 'D-05', 'D-06', 'J-03', 'K-01', 'D-001']
    ind = []
    for l in target_room:
        this_ind = np.where(bb[6]==l)[0]
        ind = np.append(ind, this_ind)
    ind = np.unique(ind).astype(int)
    bb = bb[:,ind]

    ### end_date_missing_case
    print('End_date missing')
    print('  ', bb[3,bb[2]==None])
    bb = bb[:,bb[2]!=None]
    
    print('1970 Begin_date')
    print('  ', bb[3,bb[1]<datetime.datetime(2017, 1, 1)])
    
    print('1970 End_date')
    print('  ', bb[3,bb[2]<datetime.datetime(2017, 1, 1)])
    
    print('End_date < Begin_date')
    print('  ', bb[3,bb[2]-bb[1]<datetime.timedelta(seconds=0)])
    
    print('End_date - Begin_date > 24')
    print('  ', bb[3,bb[2]-bb[1]>datetime.timedelta(hours=24)])


    bb = bb[:,bb[1]>datetime.datetime(2017, 1, 1)]
    bb = bb[:,bb[2]>datetime.datetime(2017, 1, 1)]        
    bb = bb[:,bb[2]-bb[1]>datetime.timedelta(seconds=0)]
    bb = bb[:,bb[2]-bb[1]<datetime.timedelta(hours=24)]

    bb[6,bb[6]=='D-001']='D-01'
#     print(bb[6]=='D-001')
    
    
    return bb




def get_fileid_from_date(room,date):
    
#     date = str(date)
    filedate1 = date[2:4] + date[5:7]+ str(int(int(date[8:10])-1))
    filedate2 = date[2:4] + date[5:7]+ date[8:10]
    
    conn = pymysql.connect(host=server_ip, user=user_id, password=user_passwd, db=db_name, charset='utf8')
    curs = conn.cursor()

#     sql ="""select id from sa_api_patients_info where room = %s and begin_date like %s;"""
#     curs.execute(sql,(room,filedate+'%'))
#     sql ="""select id, begin_date, end_date, file_basename from sa_api_filerecorded 
#     where file_basename like %s or file_basename like %s order by id """
#     curs.execute(sql,(room + '_'+filedate1+'%', room + '_'+filedate2+'%'))
    sql =f"""select id, begin_date, end_date, file_basename from sa_api_filerecorded 
    where file_basename like '%{room}_{filedate2}%' order by id """
#     curs.execute(sql,('%'+room + '_'+filedate2+'%'))
    print('%'+room + '_'+filedate2+'%')
    curs.execute(sql)
    result = curs.fetchall()
    result = np.array(result)

    curs.close()
    conn.close()
    print(result)
    
    try:
        return result[:,0], result[:,1], result[:,2], result[:,3]#.flatten()
    except:
        return -1
    

    
def signal_collecting_rate(date):
    date = str(date)
    errstr1 = ''; errstr2 = ''
    try:
        file_info = error_exclusion_for_oneday(get_filerecorded_data(date))
    except:
        errstr1 = "Error in filerecorded table"
#         print(errstr)
    try:
        op_info = only_target_room(get_op_table_oneday(date))
        op_info = op_info[:,np.argsort(op_info[0])]
    except:
        errstr2 = "Error in op table"
#         print(errstr2)

    target_room = ['D-01', 'D-02', 'D-03', 'D-04', 'D-05', 'D-06', 'J-03', 'K-01']
    N_target_room = len(target_room)


    plt.rcParams["xtick.direction"] = 'in'
    plt.rcParams["ytick.direction"] = 'in'
    plt.rcParams["axes.linewidth"] = 2
    plt.rcParams["xtick.labelsize"] = 15
    plt.rcParams["ytick.labelsize"] = 15

    fig = plt.figure(figsize=(12,16))
    gs = GridSpec(N_target_room,1, hspace=0.05)#, height_ratios=[1,1,1,1,1,1])

    op_time_list = []
    dt_time_list = []
    
    for i in range(N_target_room):
        ac = plt.subplot(gs[i])
        ac.set_ylim(0,2)
        ac.set_xlim(0, 24)
        ac.axvline(6, color='gray', alpha=1, linestyle=':')
        ac.axvline(12, color='gray', alpha=1, linestyle=':')
        ac.axvline(18, color='gray', alpha=1, linestyle=':')
        ac.axhline(1, color='k', linewidth=0.5)
        ac.set_xticklabels([])
        ac.set_yticklabels([])
        ac.set_xticks([6,12,18])
        ac.set_yticks([1])
        ac.text(0.01, 0.8, target_room[i], transform=ac.transAxes, ha='left', fontsize=15)
        datestr = date[0:4]+'-'+date[4:6]+'-'+date[6:]

        try:
            ind = np.where(op_info[0]==target_room[i])[0]
        except:
            continue

        for j in ind:
            opstart = op_info[1,j]+datetime.timedelta(hours=9)
            opend = op_info[2,j]+datetime.timedelta(hours=9)
            if opstart.day==opend.day:
                op_time = [opstart.hour+opstart.minute/60 + opstart.second/3600, opend.hour+opend.minute/60 + opend.second/3600]
            else:
                op_time = [opstart.hour+opstart.minute/60 + opstart.second/3600, opend.hour+opend.minute/60 + opend.second/3600+24]
            ac.fill_between(op_time, 1,2, color='dodgerblue', alpha=0.5, edgecolor='k')

            ind1 = np.where((file_info[6]==op_info[0,j])&(file_info[1]<op_info[2,j])&(file_info[2]>op_info[1,j]))[0]

            op_spending_time = opend-opstart
            dt_acquisition_time = datetime.timedelta(seconds=0)
            
            for k in ind1:
                dtstart = file_info[1,k] + datetime.timedelta(hours=9)
                dtend = file_info[2,k] + datetime.timedelta(hours=9)
                
                if dtstart.day==dtend.day:
                    dt_time = [dtstart.hour+dtstart.minute/60 + dtstart.second/3600, dtend.hour+dtend.minute/60 + dtend.second/3600]
                else:
                    dt_time = [dtstart.hour+dtstart.minute/60 + dtstart.second/3600, dtend.hour+dtend.minute/60 + dtend.second/3600+24]
                
                dt1 = opstart if opstart>dtstart else dtstart
                dt2 = opend if opend<dtend else dtend

                dt_acquisition_time += dt2-dt1

                ac.fill_between(dt_time, 0,1, color='forestgreen', alpha=0.5, edgecolor='k')

            dtfrac = dt_acquisition_time/op_spending_time
            op_time_list = np.append(op_time_list, op_spending_time)
            dt_time_list = np.append(dt_time_list, dt_acquisition_time)
            ac.text(op_time[0]/24+0.005, 0.78, str(np.round(dtfrac*100))+'%', transform=ac.transAxes, ha='left', fontsize=15)
            ac.text(op_time[0]/24+0.005, 0.61, 'N ='+str(len(ind1)), transform=ac.transAxes, ha='left', fontsize=15)

    y_text_shift = 7.35

    if len(op_time_list)>0:
        sarnum = len(np.where(dt_time_list/op_time_list>0.7)[0])/len(op_time_list) * 100
        sarnum = 'SAR$_{NUM}$ = ' + str(np.round(sarnum,1))+'%'
        sartime = np.sum(dt_time_list)/np.sum(op_time_list) * 100    
        sartime = 'SAR$_{TIME}$ = '+str(np.round(sartime,1))+'%'
        ac.text(0.0, 1.3+y_text_shift, 'N$_{OPERATION}$ = '+ str(len(op_time_list)), transform=ac.transAxes, ha='left', fontsize=15, color='black')
        ac.text(0.0, 1.1+y_text_shift, 'T$_{OPERATION}$ = '+ str(int(np.sum(op_time_list).total_seconds()/60))+'min', transform=ac.transAxes, ha='left', fontsize=15, color='black')
        ac.text(0.27, 1.3+y_text_shift, 'N$_{DATA, f>0.7}$ = '+ str(len(np.where(dt_time_list/op_time_list>0.7)[0])), transform=ac.transAxes, ha='left', fontsize=15, color='black')
        ac.text(0.27, 1.1+y_text_shift, 'T$_{DATA}$ = '+ str(int(np.sum(dt_time_list).total_seconds()/60))+'min', transform=ac.transAxes, ha='left', fontsize=15, color='black')
        ac.text(0.5, 1.3+y_text_shift, sarnum, transform=ac.transAxes, ha='left', fontsize=15, color='black')
        ac.text(0.5, 1.1+y_text_shift, sartime, transform=ac.transAxes, ha='left', fontsize=15, color='black')
        ac.text(0.86, 1.1+y_text_shift, str((datetime.datetime.now() - datetime.timedelta(days=1)).date()) , transform=ac.transAxes, ha='left', fontsize=15, color='black')
    
    
#    ac.text(0.99, 0.8+y_text_shift, datestr, transform=ac.transAxes, ha='right', fontsize=15)
    ac.text(0.01, 0.6+y_text_shift, 'Operation', transform=ac.transAxes, ha='left', fontsize=15, color='dodgerblue')
    ac.text(0.01, 0.1+y_text_shift, 'Data', transform=ac.transAxes, ha='left', fontsize=15, color='forestgreen')
    ac.text(1, 1.3+y_text_shift, errstr1, transform=ac.transAxes, ha='right', fontsize=15, color='red')
    ac.text(1, 1.1+y_text_shift, errstr2, transform=ac.transAxes, ha='right', fontsize=15, color='red')
    ac.set_xticklabels(['6h','12h','18h'])

    plt.savefig("/mnt/md0/routine/daily_collection_mail/routine_image/Daily_OP.png", bbox_inches='tight', transparent=False)
   # plt.savefig(f"/mnt/md0/routine/op_check_img/{date}.png", bbox_inches='tight', transparent=False)

op_date = datetime.datetime.now().date() - datetime.timedelta(days=1)
op_date = str(op_date).replace("-","")
signal_collecting_rate(str(op_date))
    
