"""Stable Spark readiness, native SSH tunnel and dual-ended task coordination."""
import fcntl, hashlib, json, os, pathlib, re, shlex, shutil, socket, subprocess, time, urllib.request
from decimal import Decimal
from scripts.telemetry_merge import summarize
from scripts.experiment_config import ExperimentConfig, assert_deployment, MODEL
ROOT=pathlib.Path(__file__).resolve().parent
REMOTE='/home/huanglabspk/WaiTung'
class SparkSession:
 def __init__(self,args,run_id,scratch):
  self.args=args;self.config=ExperimentConfig.build(args.experiment_profile,args.metrics_interval,args.detailed_events,args.tcp_interval,args.kv_cache_memory_bytes);self.run_id=run_id;self.scratch=pathlib.Path(scratch);self.tunnel=None;self.meta={'run_id':run_id,'ssh_tunnel':True,'model':'Qwen3.6-35B-A3B','vllm_version':'0.24.0','requested_experiment_config':self.config.as_dict()}
  self.lock_path=ROOT/'.runtime/experiment.lock';self.lock_path.parent.mkdir(parents=True,exist_ok=True);self.lock=self.lock_path.open('w');fcntl.flock(self.lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
  self.ssh=['ssh','-F',str(ROOT/'config/ssh_config'),'-o','BatchMode=yes','-o','ConnectTimeout=8','-o','ServerAliveInterval=10','-o','ServerAliveCountMax=2']
 def raw(self,command,timeout=20):
  r=subprocess.run(self.ssh+['spk',command],capture_output=True,text=True,timeout=timeout)
  if r.returncode:raise RuntimeError(f'SSH failed ({r.returncode}): {r.stderr[-1500:]} {r.stdout[-1000:]}')
  return r.stdout
 def call(self,action,task=None,timeout=25,**kw):
  cmd=['python3',REMOTE+'/scripts/spark_capture.py',action]
  if task:cmd+=['--run-id',self.run_id,'--task-id',task]
  for key,value in kw.items():cmd+=['--'+key.replace('_','-'),str(value)]
  return json.loads(self.raw(shlex.join(cmd),timeout))
 def clock(self,count=3):
  samples=[]
  for _ in range(count):
   t0=time.time_ns();m0=time.monotonic_ns();r=self.call('clock');t1=time.time_ns();m1=time.monotonic_ns();mid=(t0+t1)//2
   samples.append({'local_send_ns':t0,'local_receive_ns':t1,'remote_ns':r['realtime_ns'],'remote_minus_local_ns':r['realtime_ns']-mid,'uncertainty_bound_ns':(m1-m0)//2,'rtt_ns':m1-m0,'remote_clock_source':r['clock_source'],'local_realtime_step_ns':(t1-t0)-(m1-m0)})
  best=min(samples,key=lambda x:x['rtt_ns'])
  return {'method':'SSH midpoint offset estimate, minimum RTT sample; bound includes SSH startup/processing asymmetry, not precision synchronization','local_clock_source':pathlib.Path('/sys/devices/system/clocksource/clocksource0/current_clocksource').read_text().strip(),'samples':samples,'best':best}
 def http(self,path,data=None,timeout=10):
  req=urllib.request.Request(self.base+path,data=json.dumps(data).encode() if data is not None else None,headers={'Content-Type':'application/json'})
  with urllib.request.urlopen(req,timeout=timeout) as r:
   body=r.read();return json.loads(body) if body else {'http_status':r.status}
 def prepare(self):
  self.raw('true');state=self.call('model');self.meta['initial_model']=state
  if state['model_state']['Running']:
   # An existing Ready container is reusable ONLY if its real argv matches.
   assert_deployment(state['runtime_config'],self.config)
  if not state['model_state']['Running']:
   self.meta['launch']=self.call('launch','prepare',context_window=self.config.server_context,
                                detailed_events=int(self.config.detailed_events),
                                **({'kv_cache_memory_bytes':self.config.kv_cache_memory_bytes} if self.config.kv_cache_memory_bytes else {}));deadline=time.monotonic()+1200;seen=False
   while True:
    state=self.call('model');launch=self.call('launch-status','prepare')
    if launch.get('returncode',0)!=0:raise RuntimeError('Single model launch failed: '+str(launch))
    if state['model_state']['Running']:seen=True
    if seen and not state['model_state']['Running']:raise RuntimeError('Model stopped during preparation; no restart')
    if state['health']==200 and state['safe']:break
    if time.monotonic()>deadline:raise RuntimeError('Model preparation timeout 1200s; no retry')
    time.sleep(3)
  if not state['safe'] or state['health']!=200:raise RuntimeError('Model not safe/Ready: '+str(state))
  assert_deployment(state['runtime_config'],self.config)
  self.meta['verified_runtime_config']=state['runtime_config']
  self.meta['verified_container_id']=state['container_id']
  if not state.get('generation_proof'):self.meta['warmup']=self.call('generate',timeout=195)
  else:self.meta['warmup']={'action':'skipped','reason':'Same container already passed real Responses generation','proof':state['generation_proof']}
  # Own tunnel, independent control traffic. Never take over another process on 18000.
  sock=socket.socket()
  try:sock.bind(('127.0.0.1',self.args.tunnel_port))
  except OSError:sock.bind(('127.0.0.1',0))
  port=sock.getsockname()[1];sock.close();self.base=f'http://127.0.0.1:{port}'
  log=(self.scratch/'ssh-tunnel.log').open('wb')
  self.tunnel=subprocess.Popen(self.ssh+['-N','-o','ExitOnForwardFailure=yes','-L',f'127.0.0.1:{port}:127.0.0.1:8000','spk'],stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True);log.close()
  for _ in range(100):
   if self.tunnel.poll() is not None:raise RuntimeError('Tunnel failed: '+(self.scratch/'ssh-tunnel.log').read_text())
   try:
    models=self.http('/v1/models');break
   except Exception:time.sleep(.1)
  else:raise RuntimeError('Tunnel not Ready in 10s')
  if 'Qwen3.6-35B-A3B' not in [m['id'] for m in models['data']]:raise RuntimeError('Wrong served model ID')
  self.meta.update(base_url=self.base+'/v1',prepared_ns=time.time_ns(),ready_state=self.call('model'),model_ids=[m['id'] for m in models['data']],responses_readiness='generation proof, same container ID')
  self.meta['ssh_tunnel_identity']={'local_pid':self.tunnel.pid,'local_bind':'127.0.0.1',
    'local_port':port,'remote_target':'127.0.0.1:8000','ssh_alias':'spk',
    'purpose':'LLM HTTP/SSE via encrypted SSH port 22; distinct from SSH capture control'}
  self.meta['capture_control_identity']={'ssh_alias':'spk','remote_script':REMOTE+'/scripts/spark_capture.py',
    'connection_type':'fresh SSH exec for control; SSH_CONNECTION recorded in remote state'}
  config=self.config.update_codex((ROOT/'config/spark/config.toml').read_text(),self.base+'/v1')
  home=self.scratch/'spark-config';home.mkdir();(home/'config.toml').write_text(config);self.config_home=home
  self.config.update_catalog(ROOT/'config/spark/model_catalog.json',home/'model_catalog.json')
  self.meta['codex_config']=config;self.meta['model_catalog_sha256']=hashlib.sha256((home/'model_catalog.json').read_bytes()).hexdigest()
  self.meta['tunnel_socket_observation']=subprocess.run(['ss','-tnpH'],capture_output=True,text=True).stdout
  return self
 def task(self,task_id,task_dir):return TaskCapture(self,task_id,task_dir)
 def close(self):
  if self.tunnel and self.tunnel.poll() is None:
   self.tunnel.terminate()
   try:self.tunnel.wait(timeout=8)
   except subprocess.TimeoutExpired:self.tunnel.kill();self.tunnel.wait()
  if self.args.stop_model:self.meta['explicit_model_stop']=self.raw(REMOTE+'/scripts/stop_vllm.sh',timeout=30)
  self.lock.close()

class TaskCapture:
 def __init__(self,session,task_id,directory):
  self.s=session;self.task=task_id.replace('/','__');self.directory=pathlib.Path(directory);self.last_poll=0;self.misses=0;self.started=False;self.finished=False;self.meta={'run_id':session.run_id,'task_id':task_id,'clock_before':session.clock(),'timeline_semantics':'Codex CLI output events received locally; not network first-byte times','events':[]}
 def event(self,event):self.meta['events'].append({'event':event,'local_realtime_ns':time.time_ns(),'local_monotonic_ns':time.monotonic_ns()})
 def start(self):
  self.started=True
  interval=self.s.config.metrics_interval
  tunnel=self.s.meta.get('ssh_tunnel_identity',{})
  self.meta['metrics_sampling_interval_seconds']=interval;self.meta['tcp_sampling_interval_seconds']=self.s.config.tcp_interval;self.meta['detailed_events']=self.s.config.detailed_events
  self.meta['server_ready']=self.s.call('start',self.task,max_seconds=int(self.s.args.timeout)+180,
    metrics_interval=interval,tcp_interval=self.s.config.tcp_interval,
    detailed_events=int(self.s.config.detailed_events),tunnel_local_port=tunnel.get('local_port',0),
    tunnel_pid=tunnel.get('local_pid',0))
  self.event('server_capture_READY')
 def local_ready(self,proc,pcap,log):
  for _ in range(100):
   if proc.poll() is not None:raise RuntimeError('Local dumpcap exited before READY')
   if pcap.exists() and pcap.stat().st_size>=28 and 'File:' in log.read_text(errors='replace'):break
   time.sleep(.1)
  else:raise RuntimeError('Local capture READY timeout')
  self.event('local_capture_READY');self.marker('begin');self.poll(force=True)
 def marker(self,phase):
  before=time.time_ns();self.s.http(f'/health?measurement_marker={self.s.run_id}-{self.task}-{phase}',timeout=5);self.meta[phase+'_marker']={'local_before_ns':before,'local_after_ns':time.time_ns(),'path':f'/health?measurement_marker={self.s.run_id}-{self.task}-{phase}'}
 def agent_begin(self):self.event('agent_start');self.meta['agent_start_ns']=time.time_ns()
 def agent_end(self):
  if 'agent_end_ns' not in self.meta:self.meta['agent_end_ns']=time.time_ns();self.event('agent_end')
 def poll(self,force=False):
  if not force and time.monotonic()-self.last_poll<5:return
  self.last_poll=time.monotonic()
  try:
   r=self.s.call('status',self.task,timeout=12);self.misses=0
  except Exception:
   self.misses+=1
   if self.misses>=2:raise RuntimeError('Two SSH status failures; abort task, remote lease limits orphan lifetime')
   return
  self.meta['last_server_status']=r
  if r['state']!='READY' or not r['model']['safe']:raise RuntimeError('Server capture/model/watchdog unsafe: '+str(r))
 def before_local_stop(self):
  self.agent_end()
  try:self.marker('end')
  except Exception as e:self.meta['end_marker_error']=repr(e)
 def local_stopped(self):self.event('local_capture_STOPPED')
 def collect(self):
  if self.finished:return self.meta
  self.finished=True
  if not self.started:return self.meta
  errors=[]
  try:
   self.meta['server_stopped']=self.s.call('stop',self.task,timeout=30);self.event('server_capture_STOPPED')
   info=self.s.call('collect',self.task,timeout=150);self.meta['server_collection']=info
   # Remote original observation files remain on Spark for audit/recovery.
   # Only canonical server artifacts are downloaded to the final task directory.
   for name in ['server_traffic.pcapng','server_timeline.jsonl']:
    dest=self.directory/name;remote=f'{REMOTE}/logs/agent-capture/{self.s.run_id}/{self.task}/{name}'
    # SCP transport is bounded and uses the same dedicated, project-native SSH config.
    cmd=['scp','-F',str(ROOT/'config/ssh_config'),'-o','BatchMode=yes','-o','ConnectTimeout=8',f'spk:{remote}',str(dest)+'.partial']
    subprocess.run(cmd,check=True,capture_output=True,timeout=120)
    partial=dest.with_name(dest.name+'.partial')
    if partial.stat().st_size!=info['files'][name]['bytes']:raise RuntimeError('Server file size mismatch: '+name)
    with partial.open('rb') as f:
     actual=hashlib.file_digest(f,'sha256').hexdigest();os.fsync(f.fileno())
    if actual!=info['files'][name].get('sha256'):raise RuntimeError('Server file SHA256 missing or mismatch: '+name)
    os.replace(partial,dest)
   self.meta['clock_after']=self.s.clock();self.meta['coverage']=self.coverage(info)
   a=info['analysis']
   self.meta['parser_version']=a.get('parser_version','unavailable')
   self.meta['metrics_coverage']=a.get('metric_coverage',{})
   self.meta['metrics_sampling']={k:a.get(k) for k in
     ('metrics_status','metrics_sample_count','metrics_scrape_failures','metrics_names_observed',
      'metrics_counter_resets','metrics_service_restarts','metrics_family_types_observed')}
   self.meta['telemetry']={**(a.get('v3_raw_capture') or {}),'event_count':a.get('v3_event_count'),'events_by_source':a.get('v3_events_by_source'),'collection_status':a.get('v3_event_status'),'integrity':a.get('telemetry_integrity')}
   self.meta['metrics']=a.get('sampling',{}).get('prometheus',{})
   self.meta['tcp']=a.get('sampling',{}).get('tcp',{})
   self.meta['correlation']=a.get('correlation',{})
   self.meta['availability']=a.get('availability',{})
   self.meta['data_quality']={
     'parser_quality':a.get('data_quality',{}),
     'pcapng_readable':a.get('pcapng_readable',False),
     'zero_loss_verified':a.get('zero_loss_verified',False),
     'packet_count_matches':a.get('packet_count_matches',False),
     'http_timeline_complete':a.get('http_timeline_complete',False),
     'reassembly_issue_count':len(a.get('reassembly_issues',[])),
     'request_count':a.get('http_requests'),'response_count':a.get('http_responses'),
     'sse_count':a.get('sse_events'),
     'network_and_application_times_distinguished':True}
   self.validate_server_timeline(info)
   with (self.directory/'server_timeline.jsonl').open() as timeline:summarize([json.loads(line) for line in timeline],self.meta)
   # Preserve collected artifacts on quality failure; record explicit reasons.
   quality=a.get('data_quality',{})
   failures=[name for name in ('http_message_complete',
     'http_request_response_match_complete','sse_reconstruction_complete')
     if quality.get(name) is not True]
   if a.get('canonical_interface')!='docker0':failures.append('canonical_interface_not_docker0')
   if a.get('telemetry_integrity',{}).get('parse_errors') or a.get('telemetry_integrity',{}).get('collection_error'):failures.append('telemetry_collection_or_parse_failed')
   if self.s.config.detailed_events and not a.get('v3_event_count'):failures.append('enabled_telemetry_missing')
   if any(v.get('errors') for v in info.get('sampling_worker_stats',{}).values()):failures.append('sampling_worker_failed')
   if any(a.get(k,0) for k in ('metrics_scrape_failures','tcp_parse_failures')):failures.append('sampling_failed')
   self.meta['data_quality']['failure_reasons']=failures
   self.meta['data_quality']['quality_passed']=not failures
   local=subprocess.run(['tshark','-n','-r',str(self.directory/'raw_traffic.pcapng'),'-T','fields','-e','frame.time_epoch'],capture_output=True,text=True,timeout=60)
   if local.returncode:raise RuntimeError('Local PCAP unreadable: '+local.stderr)
   times=[int(Decimal(x)*1_000_000_000) for x in local.stdout.splitlines() if x.strip()]
   self.meta['coverage']['local_first_packet_ns']=min(times) if times else None;self.meta['coverage']['local_last_packet_ns']=max(times) if times else None
   self.meta['coverage']['local_covers_agent']=bool(times and min(times)<=self.meta.get('agent_start_ns',0) and max(times)>=self.meta.get('agent_end_ns',2**64))
   self.meta['server_record_complete']=bool(info['state']=='STOPPED' and info.get('dumpcap_exit_code')==0 and not info.get('stop_reason') and info['analysis']['zero_loss_verified'] and info['analysis']['packet_count_matches'] and self.meta['data_quality']['quality_passed'] and info['analysis']['http_timeline_complete'] and self.meta['coverage']['server_covers_agent_with_clock_bound'] and self.meta['coverage']['begin_marker_observed'] and self.meta['coverage']['end_marker_observed'])
  except BaseException as e:errors.append(repr(e));self.meta['data_quality_failure_reason']=repr(e);self.meta['server_record_complete']=False
  self.meta['errors']=errors;return self.meta
 def validate_server_timeline(self,info):
  # Validate count/schema/identities rather than trusting only file sizes.
  from collections import Counter
  counts=Counter();ids=set();responses=[];sse_with_id=0;parsed=0
  with (self.directory/'server_timeline.jsonl').open() as f:
   for n,line in enumerate(f):
    try:e=json.loads(line)
    except json.JSONDecodeError as exc:raise RuntimeError(f'Invalid server timeline JSON line {n+1}: {exc}')
    if e.get('run_id')!=self.s.run_id or e.get('task_id')!=self.task:
     raise RuntimeError('Server timeline run/task mismatch at line '+str(n+1))
    kind=e.get('type');counts[kind]+=1;parsed+=1
    if kind=='http_request_start' and e.get('model_request_id'):
     if e['model_request_id'] in ids:raise RuntimeError('Duplicate model_request_id in server timeline')
     ids.add(e['model_request_id'])
    if kind=='http_response_start':responses.append(e)
    if kind=='sse_event_observed' and e.get('model_request_id'):sse_with_id+=1
  a=info['analysis']
  for kind,expected in (('http_request_start',a['http_requests']),
                        ('http_response_start',a['http_responses']),
                        ('sse_event_observed',a['sse_events'])):
   if counts[kind]!=expected:raise RuntimeError(f'Timeline {kind} count mismatch: {counts[kind]} != {expected}')
  if len(ids)!=a['http_requests'] or sse_with_id!=a['sse_events']:
   raise RuntimeError('Missing request/SSE correlation IDs in server timeline')
  # An orphan response is never validated merely because total counts agree.
  response_ids=[e.get('model_request_id') for e in responses if e.get('status',200)>=200 or e.get('status')==101]
  if any(r not in ids for r in response_ids) or len(response_ids)!=len(set(response_ids)):
   raise RuntimeError('Unmatched or repeatedly linked final HTTP response')
  q=a.get('data_quality',{})
  if q and (q.get('matched')!=len(response_ids) or
            q.get('unmatched') or q.get('ambiguous')):
   raise RuntimeError('HTTP transaction match quality inconsistent with timeline')
  self.meta['data_quality']['timeline_json_lines_checked']=parsed
  self.meta['data_quality']['unique_request_ids']=len(ids)
  self.meta['data_quality']['linked_sse_events']=sse_with_id
 def coverage(self,info):
  before=self.meta['clock_before']['best'];after=self.meta['clock_after']['best'];offset=(before['remote_minus_local_ns']+after['remote_minus_local_ns'])//2;bound=max(before['uncertainty_bound_ns'],after['uncertainty_bound_ns'])+abs(before['remote_minus_local_ns']-after['remote_minus_local_ns'])//2
  a=info['analysis'];start=self.meta.get('agent_start_ns');end=self.meta.get('agent_end_ns');first=a.get('first_packet_ns');last=a.get('last_packet_ns')
  events=[]
  for line in (self.directory/'server_timeline.jsonl').read_text().splitlines():events.append(json.loads(line))
  begin=[e for e in events if e.get('path')==self.meta.get('begin_marker',{}).get('path')];finish=[e for e in events if e.get('path')==self.meta.get('end_marker',{}).get('path')]
  return {'offset_remote_minus_local_ns':offset,'uncertainty_bound_ns':bound,'clock_drift_estimate_ns':after['remote_minus_local_ns']-before['remote_minus_local_ns'],'server_first_packet_ns':first,'server_last_packet_ns':last,'agent_start_local_ns':start,'agent_end_local_ns':end,'server_covers_agent_with_clock_bound':bool(start and end and first and last and first-offset+bound<=start and last-offset-bound>=end),'begin_marker_observed':bool(begin),'end_marker_observed':bool(finish),'notice':'Offset estimates use SSH RTT, no claim of precise clock synchronization; packet first/last coverage additionally checked with conservative bound.'}
