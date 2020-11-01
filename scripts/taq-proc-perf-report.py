import sys
import datetime

#job:1 runs:10 parallel:10 file:-iperf-with-markouts-4.100k.hdf markouts:-m0 function:-fNBBOPrice
#job:2 runs:10 parallel:10 file:-iperf-with-markouts-4.100k.hdf markouts:-m4 function:-fNBBOPrice,VWAP

def BuildNameValueMap(nv_list):
  nv_map = {}
  for nv in nv_list:
    if '=' in nv:
      n,v  = nv.split('=')
      nv_map[n] = v;
  return nv_map

def StringToDuration(val):
  tmp = val.split('.')
  if len(tmp) == 1:
    val += ".000000"
  elif len(tmp[1]) > 6:
    to_trim = len(tmp[1]) - 6
    val = val[:len(val)-(len(tmp[1]) - 6)]
  elif len(tmp[1]) < 6:
    val = tmp[0] + '.' + '{:<06s}'.format(tmp[1])
  dt = datetime.datetime.strptime(val, "%H:%M:%S.%f")
  return datetime.timedelta(hours=dt.hour, minutes=dt.minute, seconds=dt.second, microseconds=dt.microsecond)

def MakeReport():
  f = open(sys.argv[1], 'r') if len(sys.argv) == 2 else sys.stdin
  jobs = []
  job_times = []
  min_time = datetime.timedelta(seconds=(24*3600-1))
  max_time = datetime.timedelta(microseconds=0)
  tot_time = datetime.timedelta(microseconds=0)
  test_cnt = 0
  tdp_req = datetime.timedelta(microseconds=0)
  tdp_exec = datetime.timedelta(microseconds=0)
  tdp_res = datetime.timedelta(microseconds=0)
  while True:
    line = f.readline()
    if not line:
      break
    flds = line.rstrip("\n").split(" ")
    if line[:4] == "job:":
      name,value = flds[0].split(":")
      if len(jobs) > 0:
        avg_time = tot_time / test_cnt if test_cnt > 0 else datetime.timedelta(microseconds=0)
        tdp_req_pct = round(100*tdp_req/tot_time) if test_cnt > 0 else 0
        tdp_exec_pct = round(100*tdp_exec/tot_time) if test_cnt > 0 else 0
        tdp_res_pct = round(100*tdp_res/tot_time)  if test_cnt > 0 else 0
        job_times.append((avg_time, min_time, max_time, tdp_req_pct, tdp_exec_pct, tdp_res_pct))
      jobs.append(flds)
      min_time = datetime.timedelta(seconds=(24*3600-1))
      max_time = datetime.timedelta(microseconds=0)
      tot_time = datetime.timedelta(microseconds=0)
      test_cnt = 0
      tdp_req = datetime.timedelta(microseconds=0)
      tdp_exec = datetime.timedelta(microseconds=0)
      tdp_res = datetime.timedelta(microseconds=0)
    elif "execution=" in line:
      nv =  BuildNameValueMap(flds)
      query_time = StringToDuration(nv["query-time"])
      tdp_req = tdp_req + StringToDuration(nv["request_parsing_sorting"])
      tdp_exec = tdp_exec + StringToDuration(nv["execution"])
      tdp_res = tdp_res + StringToDuration(nv["result_merging_sorting"])
      tot_time = tot_time + query_time
      if query_time < min_time:
        min_time = query_time
      if query_time > max_time:
        max_time = query_time
      test_cnt += 1
      #print(line)
      #print("tot_time:{} test_cnt:{} req:{} exec:{} res:{}".format(tot_time, test_cnt, tdp_req, tdp_exec, tdp_res ))

  if len(jobs) > 0:
    avg_time = tot_time / test_cnt if test_cnt > 0 else datetime.timedelta(microseconds=0)
    tdp_req_pct = round(100*tdp_req/tot_time) if test_cnt > 0 else 0
    tdp_exec_pct = round(100*tdp_exec/tot_time) if test_cnt > 0 else 0
    tdp_res_pct = round(100*tdp_res/tot_time)  if test_cnt > 0 else 0
    job_times.append((avg_time, min_time, max_time, tdp_req_pct, tdp_exec_pct, tdp_res_pct))

  #job:2 runs:10 parallel:10 file:-iperf-with-markouts-4.100k.hdf markouts:-m4 function:-fNBBOPrice,VWAP
  header=["Job No.","Test Cnt.","Parallel Cnt.", "Test Data", "Markouts", "Function List",
          "Avg. Time", "Min. Time", "Max. Time", "TDP input proc.", "TDP exec.", "TDP output proc."]
  print(",".join(header))
  for i in range(len(jobs)):
    print('{},{},{},{},{},"{}"," {}"," {}"," {}",{}%,{}%,{}%'.format(
       jobs[i][0].split(":")[1], jobs[i][1].split(":")[1], jobs[i][2].split(":")[1],
       jobs[i][3].split(":")[1], jobs[i][4].split(":")[1], jobs[i][5].split(":")[1],
       job_times[i][0], job_times[i][1], job_times[i][2],
       job_times[i][3], job_times[i][4], job_times[i][5]))


if __name__ == "__main__":
  try:
    MakeReport()
  except IOError as e:
    if e.errno != 32: # broken pipe
      print("I/O error({0}): {1}".format(e.errno, e.strerror))
  except Exception as ex:
    print("err:{}".format(ex))

