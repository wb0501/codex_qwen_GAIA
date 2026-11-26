#!/usr/bin/env python3
"""GAIA Codex + Spark runner; local Parquet, mandatory filesystem isolation, dual capture."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import re
import selectors
import shutil
import signal
import socket
import subprocess
import struct
import sys
import tempfile
import threading
import time
import tomllib
import traceback
import shlex
import errno
import zipfile
import ipaddress
from collections import Counter
from datetime import datetime, timezone
from contextlib import contextmanager
import importlib
import mimetypes
import string
from dataclasses import dataclass, field
SparkSession = None
from urllib.parse import parse_qsl, urlparse

from exa_tool_gateway import ToolNameGateway

GAIA_ROOT = Path(__file__).resolve().parent
ROOT = GAIA_ROOT  # reassigned to actual shared measurement root before running
LOCAL_ANALYSIS_TIMEOUT = 180
_interrupt_requested = False


WITHHELD_ANSWERS = {'', '?', 'not available', 'unknown', 'none', 'n/a', 'null', 'nan'}


@dataclass(frozen=True)
class GaiaTask:
    task_id: str
    question: str = field(repr=False)
    level: int
    split: str
    attachment: Path | None = field(repr=False)
    expected_answer: str | None = field(repr=False)

    @property
    def name(self) -> str:
        return f'{self.split}__{self.task_id}'


def load_tasks(dataset_root: Path, year: str, split: str) -> list[GaiaTask]:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError('Parquet support requires: python -m pip install pyarrow') from exc
    dataset_root = dataset_root.resolve(strict=True)
    if not re.fullmatch(r'[0-9]{4}',year) or split not in ('validation','test'):
        raise ValueError('Invalid split/year')
    if (dataset_root / year).is_symlink() or (dataset_root / year / split).is_symlink():
        raise ValueError('Symlink split path is forbidden')
    split_dir = (dataset_root / year / split).resolve(strict=True)
    if not split_dir.is_relative_to(dataset_root):
        raise ValueError('Split directory must stay within the dataset root')
    metadata = split_dir / 'metadata.parquet'
    if metadata.is_symlink() or not metadata.is_file():
        raise ValueError('Missing or symlink metadata.parquet')
    # Do not concatenate metadata.levelN.parquet: those repeat the same tasks.
    columns = ['task_id', 'Question', 'Level', 'Final answer', 'file_name', 'file_path']
    names = set(pq.ParquetFile(metadata).schema_arrow.names)
    if split == 'test' and 'Final answer' not in names:
        columns.remove('Final answer')
    if set(columns)-names:
        raise ValueError('Missing required Parquet columns: '+str(sorted(set(columns)-names)))
    rows = pq.read_table(metadata, columns=columns).to_pylist()
    tasks = []
    seen = set()
    for row in rows:
        task_id = str(row['task_id']).strip()
        # The original test split also contains a non-UUID ID (0-0-0-0-0).
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}', task_id):
            raise ValueError('Task ID must be a safe filename component')
        if task_id in seen:
            raise ValueError(f'Duplicate task ID: {task_id}')
        seen.add(task_id)
        question = row['Question']
        if not isinstance(question, str) or not question.strip():
            raise ValueError(f'Missing question for task {task_id}')
        level = int(row['Level'])
        if level not in (1, 2, 3):
            raise ValueError(f'Invalid difficulty for task {task_id}')
        attachment = None
        file_name = str(row['file_name'] or '').strip()
        file_path = str(row['file_path'] or '').strip()
        if file_name:
            if Path(file_name).name != file_name or ':' in file_name or '\\' in file_name or file_name in ('.','..'):
                raise ValueError(f'Invalid attachment filename for task {task_id}')
            declared = split_dir / file_name
            if declared.is_symlink():
                raise ValueError(f'Symlink attachment is forbidden: task {task_id}')
            attachment = declared.resolve(strict=True)
            if not attachment.is_relative_to(split_dir) or not attachment.is_file():
                raise ValueError(f'Attachment outside task split: {task_id}')
            if attachment.suffix and not re.fullmatch(r'\.[A-Za-z0-9]{1,16}',attachment.suffix):
                raise ValueError('Unsafe attachment extension: '+task_id)
            if attachment.name.lower().startswith('metadata') or attachment.suffix.lower() in {'.parquet', '.jsonl'}:
                raise ValueError(f'Dataset metadata cannot be an attachment: {task_id}')
            if file_path and (Path(file_path).is_absolute() or '..' in Path(file_path).parts or (dataset_root / file_path).resolve() != attachment):
                raise ValueError(f'Conflicting attachment paths for task {task_id}')
        elif file_path:
            raise ValueError(f'Attachment path without filename: task {task_id}')
        answer = str(row.get('Final answer') if row.get('Final answer') is not None else '').strip()
        expected = None if answer.lower() in WITHHELD_ANSWERS else answer
        tasks.append(GaiaTask(task_id, question, level, split, attachment, expected))
    return tasks


def task_prompt(task: GaiaTask) -> str:
    attachment_name = None
    if task.attachment is not None:
        attachment_name = 'attachment' + task.attachment.suffix
    prompt = (
        'Solve the following question using the available tools.\n'
        'Use evidence and calculations where appropriate. Do not search for benchmark '
        'answer keys or use evaluation metadata.\n'
        'Your last line must be FINAL ANSWER: followed by only the requested answer, '
        'with no explanation on that line. Follow the question\'s requested formatting.\n\n'
        'Question:\n' + task.question.strip() + '\n'
    )
    if attachment_name:
        prompt += f'\nThe supplied attachment is available at /work/{attachment_name}.\n'
    return prompt


def stage_task(task, work):
    if task.attachment is not None:
        if task.attachment.is_symlink() or not task.attachment.is_file():
            raise ValueError('Attachment changed after loading')
        shutil.copyfile(task.attachment, work / ('attachment'+task.attachment.suffix))
    return task_prompt(task)

def normalize_number(value: str) -> float | None:
    try:
        return float(value.replace(',', '').replace('$', '').replace('%', '').strip())
    except ValueError:
        return None


def normalize_text(value: str, remove_punctuation: bool = True) -> str:
    value = ''.join(value.split()).lower()
    return value.translate(str.maketrans('', '', string.punctuation)) if remove_punctuation else value


def score_answer(predicted: str | None, expected: str | None) -> bool | None:
    """GAIA's numeric/list/text comparison, without an LLM judge or gold output."""
    if expected is None:
        return None
    if predicted is None:
        return False
    try:
        target_number = float(expected)
    except ValueError:
        target_number = None
    if target_number is not None:
        return normalize_number(predicted) == target_number
    if ',' in expected or ';' in expected:
        targets = re.split('[,;]', expected)
        answers = re.split('[,;]', predicted)
        if len(targets) != len(answers):
            return False
        for answer, target in zip(answers, targets):
            try:
                number = float(target)
            except ValueError:
                if normalize_text(answer, False) != normalize_text(target, False):
                    return False
            else:
                if normalize_number(answer) != number:
                    return False
        return True
    return normalize_text(predicted) == normalize_text(expected)




def select_tasks(tasks, args):
    selected = [t for t in tasks if not args.level or t.level in args.level]
    if args.task:
        exact = [t for t in selected if args.task in (t.task_id, t.name)]
        selected = exact or [t for t in selected if t.task_id.startswith(args.task)]
        if len(selected) != 1:
            raise ValueError('--task must identify exactly one task after level filtering')
    if args.range:
        start, end = args.range
        if not 1 <= start <= end <= len(selected):
            raise ValueError('--range must be within level-filtered task count')
        selected = selected[start-1:end]
    if args.limit is not None:
        if args.limit < 1:
            raise ValueError('--limit must be positive')
        selected = selected[:args.limit]
    if not selected:
        raise ValueError('No tasks match selection')
    return selected


def task_directory(split, task_id):
    if split not in ('validation','test') or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}',task_id):
        raise ValueError('Invalid task path')
    return f'{split}__{task_id}'


def server_task_id(split, task_id):
    name = task_directory(split, task_id)
    return name if len(name) <= 100 else name[:34]+'-'+hashlib.sha256(name.encode()).hexdigest()


def extract_answer(output):
    text = output.get('final_message')
    if not isinstance(text, str):
        return None
    matches = re.findall(r'^FINAL ANSWER:[ \t]*(.+)$', text, flags=re.MULTILINE)
    if matches and matches[-1].strip():
        return matches[-1].strip()
    stripped = text.strip()
    return stripped if stripped and '\n' not in stripped else None


def score_result(output, task):
    output = output if isinstance(output,dict) else {}
    text = output.get('final_message')
    predicted = extract_answer(output)
    final_line = text.strip().splitlines()[-1] if isinstance(text,str) and text.strip() else ''
    valid = bool(re.fullmatch(r'FINAL ANSWER:[ \t]*\S.*', final_line))
    available = task.expected_answer is not None
    completed = output.get('succeeded') is True and not output.get('timed_out') and not output.get('interrupted')
    passed = (bool(completed and valid and score_answer(predicted, task.expected_answer)) if available else None)
    return {'predicted_answer':predicted, 'passed':passed, 'scoring_available':available,
            'format_valid':valid, 'status':('agent_incomplete' if not completed else 'format_invalid' if not valid else 'unscored' if not available else 'correct' if passed else 'incorrect'),
            'method':'GAIA legacy numeric/list/text comparison; strict final-line marker; no LLM judge'}


def attachment_metadata(task, work):
    if task.attachment is None:
        return None
    dest = work / ('attachment'+task.attachment.suffix)
    with dest.open('rb') as stream:
        digest = hashlib.file_digest(stream,'sha256').hexdigest()
    return {'original_name':task.attachment.name, 'sandbox_path':'/work/'+dest.name,
            'bytes':dest.stat().st_size,'sha256':digest,'suffix':dest.suffix,
            'mime_guess':mimetypes.guess_type(dest.name)[0], 'mime_source':'filename extension, not content detection'}


def load_measurement(root):
    """Load the measurement module vendored beside this GAIA runner."""
    root = Path(root).resolve(strict=True)
    for path in ('spark_measurement.py','scripts/experiment_config.py','scripts/telemetry_merge.py'):
        if not (root/path).is_file():
            raise RuntimeError('Shared measurement file missing: '+str(root/path))
    for name, module in list(sys.modules.items()):
        if name == 'spark_measurement' or name == 'scripts' or name.startswith('scripts.'):
            source = getattr(module,'__file__',None)
            paths = list(getattr(module,'__path__',[]))
            if source and not Path(source).resolve().is_relative_to(root):
                raise RuntimeError('Conflicting loaded measurement module: '+name)
            if paths and not any(Path(x).resolve().is_relative_to(root) for x in paths):
                raise RuntimeError('Conflicting scripts package')
    sys.path.insert(0,str(root))
    importlib.invalidate_caches()
    module = importlib.import_module('spark_measurement')
    if Path(module.__file__).resolve() != root/'spark_measurement.py' or Path(module.ROOT).resolve() != root:
        raise RuntimeError('Wrong Spark measurement module loaded')
    return module


def existing_model_session(module):
    """Keep shared lifecycle/lock; refuse implicit startup of a missing model."""
    class ExistingModelSession(module.SparkSession):
        def call(self, action, *args, **kwargs):
            if action == 'launch':
                raise RuntimeError('GAIA requires the existing ready Spark model; implicit launch disabled')
            return super().call(action,*args,**kwargs)
        def http(self,path,data=None,timeout=10):
            # Do not let the supervisor's inherited proxy change tunnel traffic.
            import urllib.request
            req = urllib.request.Request(self.base+path,data=json.dumps(data).encode() if data is not None else None,headers={'Content-Type':'application/json'})
            with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req,timeout=timeout) as response:
                body=response.read()
                return json.loads(body) if body else {'http_status':response.status}
    return ExistingModelSession


def check_paths(dataset, results, measurement):
    for path in (dataset, results, measurement):
        path = Path(path).resolve()
        for exposed in ('/usr','/bin','/sbin','/lib','/lib64','/etc/ssl/certs'):
            if path.is_relative_to(Path(exposed).resolve()):
                raise ValueError('Sensitive path lies under a sandbox system mount: '+str(path))
    dataset, results, measurement = map(lambda p:Path(p).resolve(), (dataset, results, measurement))
    if results.is_relative_to(dataset) or dataset.is_relative_to(results) or results.is_relative_to(measurement) or measurement.is_relative_to(results):
        raise ValueError('GAIA result directory must be separate from dataset and shared measurement tree')


def probe_code(dataset, measurement, results, task, model_url):
    hidden = [str(Path(x).resolve()) for x in (dataset, measurement, results)] + ['/root/.codex','/home/claude-GAIA',str(GAIA_ROOT/'DATASET')]
    expected = ['attachment'+task.attachment.suffix] if task.attachment else []
    return ('import os, pathlib, subprocess, shutil, json, urllib.request; '
            'assert all(not os.path.exists(p) for p in '+repr(hidden)+'); '
            'assert os.getcwd()=="/work"; '
            'assert sorted(os.listdir("/work"))=='+repr(expected)+'; '
            'assert all(os.access(p,os.R_OK) for p in '+repr(expected)+'); '
            'assert os.access("/opt/bin/codex-code-mode-host",os.X_OK); '
            'assert subprocess.run(["/bin/sh","-c","true"]).returncode==0; '
            'assert urllib.request.build_opener(urllib.request.ProxyHandler({})).open('+repr(model_url.removesuffix('/v1')+'/health')+',timeout=5).status==200; '
            'print(json.dumps({"isolation":"confirmed","tunnel_health":200,"utilities":{x:shutil.which(x) for x in '+repr(['python3','sh','curl','getent','file','pdftotext','pdfinfo','tesseract','ffmpeg','unzip','libreoffice','wget'])+'}}))')


class CaptureObserver:
    """Bound local disk and retain shared lease polling even after CLI stdout EOF."""
    def __init__(self, remote, capture, spool, destination, reserve_mib, runtime=None):
        self.remote=remote;self.capture=capture;self.spool=spool;self.destination=destination;self.reserve=reserve_mib*1024**2;self.last=0;self.runtime=runtime
    def poll(self):
        if self.capture.poll() is not None:
            raise RuntimeError('Local dumpcap stopped during Agent execution')
        if self.remote is not None:self.remote.poll()
        if time.monotonic()-self.last>=5:
            self.last=time.monotonic()
            # Destination needs room for spool copy plus temporary TLS rewrite.
            size=self.spool.stat().st_size if self.spool.exists() else 0
            if (shutil.disk_usage(self.spool.parent).free<self.reserve or shutil.disk_usage(self.destination).free<self.reserve+2*size
                    or (self.runtime is not None and shutil.disk_usage(self.runtime).free<self.reserve)):
                raise RuntimeError('Local capture disk reserve exhausted; preserve existing sample')


EXA_MCP_SERVER_NAME = 'exa'
EXA_MCP_TOOL_NAME = 'web_search_exa'
EXA_MCP_HOST = 'mcp.exa.ai'
EXA_MCP_URL = 'https://mcp.exa.ai/mcp?tools=web_search_exa'
GAIA_MCP_CONFIG_VERSION = 'exa-streamable-http-v1'


def safe_proxy_endpoint(value):
    """Return proxy endpoint metadata without userinfo, query, or fragment secrets."""
    if not value:
        return None
    parsed = urlparse(value if '://' in value else 'http://' + value)
    if not parsed.hostname:
        return {'configured': True, 'parse_status': 'unavailable'}
    try:
        port = parsed.port
    except ValueError:
        port = None
    return {'scheme': parsed.scheme or 'http', 'host': parsed.hostname,
            'port': port or (443 if parsed.scheme == 'https' else 80)}


def gaia_codex_config_text(base_text):
    """Append Exa MCP only to the per-task CODEX_HOME config, never shared config."""
    parsed = tomllib.loads(base_text)
    if parsed.get('mcp_servers'):
        raise ValueError('Shared Codex config already defines MCP servers; refusing ambiguous merge')
    addition = (
        '\n\n# GAIA-only remote MCP; isolated in this task CODEX_HOME.\n'
        '[mcp_servers.exa]\n'
        f'url = "{EXA_MCP_URL}"\n'
        'enabled = true\n'
        'required = false\n'
        'enabled_tools = ["web_search_exa"]\n'
    )
    merged = base_text.rstrip() + addition
    checked = tomllib.loads(merged)
    exa = checked.get('mcp_servers', {}).get(EXA_MCP_SERVER_NAME, {})
    if (exa.get('url') != EXA_MCP_URL or exa.get('enabled') is not True
            or exa.get('enabled_tools') != [EXA_MCP_TOOL_NAME]):
        raise ValueError('Generated GAIA Exa MCP config failed validation')
    return merged


def tool_metadata(args):
    external = (os.environ.get('GAIA_UPSTREAM_PROXY')
                or getattr(args, 'upstream_proxy', None))
    return {
        'native_web_search': 'not relied upon; model catalog may report supports_search_tool=false',
        'mcp': {'server': EXA_MCP_SERVER_NAME, 'url': EXA_MCP_URL,
                'transport': 'streamable_http', 'enabled_tools': [EXA_MCP_TOOL_NAME],
                'auth': 'anonymous_exa_free_tier; no OAuth or API key injected',
                'scope': 'per-task CODEX_HOME only', 'config_version': GAIA_MCP_CONFIG_VERSION},
        'network_boundary': 'all-interface capture; Exa MCP is direct while other external web traffic uses local TLS inspection and the WSL upstream proxy',
        'tls_mode': 'disabled' if args.no_tls_inspection else 'enabled_for_supported_external_clients',
        'external_proxy': safe_proxy_endpoint(external),
        'proxy_bypass': 'localhost,127.0.0.1,::1,model-tunnel-host,192.168.3.125,mcp.exa.ai'}


def plugin_resource_findings(config_home):
    """Document the native paths, independently of installed plugin state."""
    snapshot = config_home / '.tmp/plugins'
    revision = config_home / '.tmp/plugins.sha'
    return {
        'reuse_enabled': True,
        'status': 'verified_resources_only; warm_network_validation_pending',
        'native_resource_directory': '$CODEX_HOME/.tmp/plugins',
        'native_revision_file': '$CODEX_HOME/.tmp/plugins.sha',
        'native_sync_lock': '$CODEX_HOME/.tmp/plugins.sync.lock',
        'host_manifest_present': (snapshot / '.agents/plugins/marketplace.json').is_file(),
        'host_revision': revision.read_text().strip() if revision.exists() else None,
        'observed_download_chain': ['GitHub repository/ref/zipball',
                                    'chatgpt.com/backend-api/plugins/export/curated',
                                    'files.openai.com/content (codex-plugins.zip)'],
        'policy': 'Only archive-verified static files plus the observed native revision are reused. '
                  'Every task receives private writable copies; the shared cache is never mounted. '
                  'Unverified host snapshots, installed plugins, sync locks, auth, and session state are not imported. '
                  'Native startup synchronization and all network requests remain enabled.',
    }


def sha256_file(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def plugin_tree_inventory(root):
    """List resource paths and sizes; reject links and special files."""
    if not root.is_dir() or root.is_symlink():
        raise ValueError('No regular native plugin resource directory')
    inventory = {}
    for directory, dirs, files in os.walk(root, followlinks=False):
        parent = Path(directory)
        if parent == root:
            dirs[:] = [name for name in dirs if name != '.git']
        for name in dirs:
            if (parent / name).is_symlink():
                raise ValueError('Symlink in plugin resource tree')
        for name in files:
            if parent == root and name == '.git':
                continue
            path = parent / name
            if path.is_symlink() or not path.is_file():
                raise ValueError('Non-regular plugin resource file')
            inventory[path.relative_to(root).as_posix()] = path.stat().st_size
    return inventory


def observe_native_plugin_snapshot(home):
    """Called after CLI exit; never persist these task-derived bytes directly."""
    try:
        if home.is_symlink() or (home / '.tmp').is_symlink():
            raise ValueError('Symlink in native plugin cache ancestry')
        marker = home / '.tmp/plugins.sha'
        if marker.is_symlink() or marker.stat().st_size > 128:
            raise ValueError('Invalid native plugin revision marker')
        revision = marker.read_text().strip()
        if not re.fullmatch('[0-9a-f]{40}', revision):
            raise ValueError('Native revision is not a Git SHA-1; refusing to guess its meaning')
        return {'revision': revision, 'files': plugin_tree_inventory(home / '.tmp/plugins')}
    except (OSError, ValueError) as exc:
        return {'error': str(exc)}


def unpack_plugin_archive(archive, destination):
    """Accept only a bounded, regular-file marketplace tree from the ZIP."""
    with zipfile.ZipFile(archive) as bundle:
        members = bundle.infolist()
        if len(members) > 20000 or sum(m.file_size for m in members) > 256 * 1024 * 1024:
            raise ValueError('Plugin archive exceeds resource limits')
        manifests = [m.filename for m in members
                     if m.filename.endswith('/.agents/plugins/marketplace.json')]
        if len(manifests) != 1:
            raise ValueError('Expected exactly one native marketplace manifest')
        prefix = manifests[0][:-len('.agents/plugins/marketplace.json')]
        seen = set()
        for member in members:
            name = member.filename
            if ('\\' in name or name.startswith('/') or '..' in name.split('/')
                    or ':' in name or '\x00' in name or not name.startswith(prefix)):
                raise ValueError('Unsafe or out-of-root plugin archive path')
            relative = name[len(prefix):].rstrip('/')
            if not relative:
                continue
            if relative.startswith('/') or any(part in ('', '.', '..') for part in relative.split('/')):
                raise ValueError('Non-canonical plugin resource path')
            if relative.split('/')[0] == '.git':
                # Server export ZIPs contain Git config, logs and objects. None
                # of these belong in the reusable static resource snapshot.
                continue
            if '.git' in relative.split('/'):
                raise ValueError('Git metadata is not a static plugin resource')
            mode = member.external_attr >> 16
            kind = mode & 0o170000
            if kind not in (0, 0o040000, 0o100000):
                raise ValueError('Symlinks and special archive entries are forbidden')
            path = destination / relative
            if member.is_dir():
                path.mkdir(parents=True, exist_ok=True)
                continue
            if relative in seen:
                raise ValueError('Duplicate archive file')
            seen.add(relative)
            path.parent.mkdir(parents=True, exist_ok=True)
            with bundle.open(member) as incoming, path.open('xb') as output:
                shutil.copyfileobj(incoming, output)
            path.chmod(0o755 if mode & 0o111 else 0o644)
        manifest = json.loads((destination / '.agents/plugins/marketplace.json').read_text())
        if not isinstance(manifest.get('plugins'), list):
            raise ValueError('Invalid native marketplace manifest')


def plugin_archive_revision(archive):
    """Read only Git HEAD/ref text, never execute or retain Git configuration.

    The curated export can be an older backup than GitHub main. Native 0.160.1
    explicitly reads the backup archive's Git HEAD; using main's SHA for this
    export would falsely label its contents as a newer revision.
    """
    with zipfile.ZipFile(archive) as bundle:
        names = set(bundle.namelist())
        heads = [name for name in names if name.endswith('/.git/HEAD')]
        if not heads:
            return None
        if len(heads) != 1 or bundle.getinfo(heads[0]).file_size > 4096:
            raise ValueError('Ambiguous archive Git HEAD')
        head = bundle.read(heads[0]).decode('ascii').strip()
        if re.fullmatch('[0-9a-f]{40}', head):
            return head
        if not head.startswith('ref: '):
            raise ValueError('Invalid archive Git HEAD')
        reference = head[5:]
        if not re.fullmatch(r'refs/[A-Za-z0-9_./-]+', reference) or '..' in reference.split('/'):
            raise ValueError('Unsafe archive Git ref')
        prefix = heads[0][:-len('HEAD')]
        ref_path = prefix + reference
        if ref_path in names:
            if bundle.getinfo(ref_path).file_size > 4096:
                raise ValueError('Oversized archive Git ref')
            revision = bundle.read(ref_path).decode('ascii').strip()
            if re.fullmatch('[0-9a-f]{40}', revision):
                return revision
        packed = prefix + 'packed-refs'
        if packed in names and bundle.getinfo(packed).file_size < 1024 * 1024:
            for line in bundle.read(packed).decode('ascii').splitlines():
                parts = line.split()
                if len(parts) == 2 and parts[1] == reference and re.fullmatch('[0-9a-f]{40}', parts[0]):
                    return parts[0]
        raise ValueError('Cannot resolve archive Git HEAD')


def restore_plugin_resources(cache, home, binary_hash):
    """Restore copies into this task's fresh CODEX_HOME, with no shared mount."""
    result = {'hit': False, 'cache_directory': str(cache), 'status': 'cold'}
    pointer = cache / 'current'
    if not pointer.exists():
        return result
    try:
        identity = pointer.read_text().strip()
        if not re.fullmatch('[0-9a-f]{64}', identity):
            raise ValueError('Invalid plugin cache object id')
        snapshot = cache / identity
        metadata = json.loads((snapshot / 'manifest.json').read_text())
        if metadata.get('schema') != 1 or metadata.get('binary_sha256') != binary_hash:
            raise ValueError('Cache manifest or CLI identity mismatch')
        revision = (snapshot / 'plugins.sha').read_text().strip()
        if revision != metadata['revision'] or not re.fullmatch('[0-9a-f]{40}', revision):
            raise ValueError('Cache revision mismatch')
        # Check resource types only; cached file contents are not hashed.
        files = plugin_tree_inventory(snapshot / 'plugins')
        target = home / '.tmp'
        target.mkdir(parents=True, exist_ok=True)
        shutil.copytree(snapshot / 'plugins', target / 'plugins')
        (target / 'plugins.sha').write_text(revision + '\n')
        result.update(hit=True, status='restored_private_copy', revision=revision,
                      object_id=identity, file_count=len(files))
    except (OSError, ValueError, KeyError, TypeError) as exc:
        # A broken cache must not leave a partial snapshot masquerading as warm.
        shutil.rmtree(home / '.tmp/plugins', ignore_errors=True)
        (home / '.tmp/plugins.sha').unlink(missing_ok=True)
        result.update(status='rejected', error=str(exc))
    return result


def publish_plugin_resources(cache, binary_hash, native, archives, observed_revisions):
    """Publish server ZIP resources with the native revision, paths and sizes.

    Session/task files can never enter the cache: the payload is extracted from
    the server ZIP, not copied back from the task home. Different paths/sizes
    or missing/unknown native markers prevent publication.
    """
    if 'error' in native:
        return {'published': False, 'reason': native['error']}
    if not archives:
        return {'published': False, 'reason': 'No complete, CRC-valid upstream plugin ZIP was recovered'}
    cache.mkdir(mode=0o700, parents=True, exist_ok=True)
    failures = []
    for archive in archives:
        try:
            archive_revision = plugin_archive_revision(archive)
            if archive_revision is not None:
                if native.get('revision') != archive_revision:
                    raise ValueError('Native revision differs from the curated export Git HEAD')
            elif native.get('revision') not in observed_revisions:
                raise ValueError('Native revision is not corroborated by archive or upstream requests')
            with tempfile.TemporaryDirectory(prefix='.building-', dir=cache) as temporary:
                stage = Path(temporary)
                unpack_plugin_archive(archive, stage / 'plugins')
                files = plugin_tree_inventory(stage / 'plugins')
                if files != native['files']:
                    raise ValueError('Downloaded archive differs from native plugin tree; refusing task-state export')
                metadata = {'schema': 1, 'binary_sha256': binary_hash, 'revision': native['revision'],
                            'archive_revision': archive_revision,
                            'files': files}
                identity = hashlib.sha256(json.dumps(metadata, sort_keys=True).encode()).hexdigest()
                (stage / 'plugins.sha').write_text(native['revision'] + '\n')
                write_json(stage / 'manifest.json', metadata)
                with (cache / '.lock').open('a') as lock:
                    fcntl.flock(lock, fcntl.LOCK_EX)
                    if not (cache / identity).exists():
                        os.rename(stage, cache / identity)
                    # Use a unique temporary pointer for concurrent independent runs.
                    with tempfile.NamedTemporaryFile(mode='w', prefix='.current-', dir=cache, delete=False) as pointer:
                        pointer.write(identity + '\n')
                        pointer_path = Path(pointer.name)
                    os.replace(pointer_path, cache / 'current')
                return {'published': True, 'object_id': identity, 'revision': native['revision'],
                        'file_count': len(files), 'archive_bytes': archive.stat().st_size}
        except (OSError, ValueError, zipfile.BadZipFile, KeyError) as exc:
            failures.append(str(exc))
    return {'published': False, 'reason': '; '.join(failures)}


def plugin_stream_objects(pcap, key_options, diagnostics, stream):
    subset = diagnostics / f'plugin-stream-{stream}.pcapng'
    export = diagnostics / f'plugin-objects-{stream}'
    if export.exists():
        return export
    export.mkdir()
    selected = subprocess.run(['tshark', '-r', str(pcap), '-Y', f'tcp.stream == {stream}',
                               '-w', str(subset)], capture_output=True, text=True, timeout=LOCAL_ANALYSIS_TIMEOUT)
    if selected.returncode:
        raise RuntimeError('Cannot isolate plugin stream: ' + selected.stderr[-1000:])
    exported = subprocess.run(['tshark', '-r', str(subset), *key_options, '-q',
                               '--export-objects', f'http,{export}'],
                              capture_output=True, text=True, timeout=LOCAL_ANALYSIS_TIMEOUT)
    if exported.returncode:
        raise RuntimeError('Cannot export plugin response: ' + exported.stderr[-1000:])
    return export


def plugin_download_evidence(pcap, keys, diagnostics):
    """Measure server and proxy legs separately; CRC-check exported ZIPs.

    Exports and temporary packet subsets never become task artifacts. Signed
    URLs, cookies, headers, and non-resource bodies are never cached.
    """
    result = {'downloads': [], 'complete_external_zip_count': 0,
              'complete_zip_observed': False, 'status': 'not_observed',
              'notice': 'Local proxy and external legs are the same download. '
                        'A missing exported body means unknown, not zero bytes. '
                        'Wire bytes include protocol overhead and retransmissions.'}
    if not pcap.exists() or not pcap.stat().st_size:
        result['status'] = 'no_capture'
        return result, [], set()
    key_options = ['-o', f'tls.keylog_file:{keys}'] if keys.exists() else []
    fields = ['tcp.stream', 'http2.streamid', 'http2.headers.authority', 'http2.headers.path',
              'http2.headers.status', 'http2.headers.content_type', 'http2.headers.content_disposition',
              'tcp.dstport', 'ip.dst']
    cmd = ['tshark', '-r', str(pcap), *key_options, '-Y',
           'http2.headers.authority || http2.headers.status', '-T', 'fields']
    for field in fields:
        cmd += ['-e', field]
    read = subprocess.run(cmd, capture_output=True, text=True, timeout=LOCAL_ANALYSIS_TIMEOUT)
    if read.returncode:
        raise RuntimeError('Cannot inspect plugin requests: ' + read.stderr[-1000:])
    rows = [dict(zip(fields, line.split('\t'))) for line in read.stdout.splitlines()]
    revisions, streams, metadata_streams, request_urls = set(), {}, set(), {}
    request_counts = {}
    def url_identity(url):
        parsed = urlparse(url)
        return parsed.hostname, parsed.path, tuple(sorted(parse_qsl(parsed.query, keep_blank_values=True)))
    for row in rows:
        host, path = row.get('http2.headers.authority'), row.get('http2.headers.path', '').split('?')[0]
        if host:
            request_counts[row['tcp.stream']] = request_counts.get(row['tcp.stream'], 0) + 1
        match = re.fullmatch('/repos/openai/plugins/zipball/([0-9a-f]{40})', path)
        if host == 'api.github.com' and match:
            revisions.add(match[1])
        if host == 'chatgpt.com' and path == '/backend-api/plugins/export/curated':
            metadata_streams.add(row['tcp.stream'])
        if host == 'files.openai.com' and path == '/content':
            stream = row['tcp.stream']
            request_urls[stream] = url_identity('https://files.openai.com' + row['http2.headers.path'])
            streams[stream] = {'tcp_stream': int(stream), 'host': host, 'path': path,
                               'leg': 'external' if row.get('tcp.dstport') == '443' else 'local_proxy',
                               'status_code': None, 'zip_complete': None,
                               'exported_body_bytes': None, 'wire_inbound_bytes': 0}
    for row in rows:
        stream = row['tcp.stream']
        if stream in streams and row.get('http2.headers.status'):
            entry = streams[stream]
            entry.update(status_code=row['http2.headers.status'],
                         content_type=row.get('http2.headers.content_type'),
                         content_disposition=row.get('http2.headers.content_disposition'))
    trusted_urls = set()
    for stream in metadata_streams:
        if request_counts.get(stream) != 1:
            # The object exporter does not expose HTTP/2 stream IDs. Do not
            # associate a body from a different multiplexed request with this API.
            continue
        for body in plugin_stream_objects(pcap, key_options, diagnostics, stream).iterdir():
            if not body.is_file() or body.stat().st_size > 128 * 1024:
                continue
            try:
                metadata = json.loads(body.read_bytes())
                if isinstance(metadata, dict) and set(metadata) == {'download_url'}:
                    url = metadata['download_url']
                    if isinstance(url, str) and urlparse(url).scheme == 'https' and urlparse(url).hostname == 'files.openai.com':
                        trusted_urls.add(url_identity(url))
            except (ValueError, UnicodeError):
                pass
    archives = []
    for stream, entry in streams.items():
        entry['curated_metadata_url_verified'] = request_urls[stream] in trusted_urls
        entry['request_count_on_connection'] = request_counts.get(stream, 0)
        sizes = subprocess.run(['tshark', '-r', str(pcap), '-Y', f'tcp.stream == {stream}',
                                '-T', 'fields', '-e', 'tcp.srcport', '-e', 'frame.len'],
                               capture_output=True, text=True, timeout=LOCAL_ANALYSIS_TIMEOUT)
        if sizes.returncode:
            raise RuntimeError('Cannot measure plugin TCP stream: ' + sizes.stderr[-1000:])
        entry['wire_inbound_bytes'] = sum(int(parts[1]) for line in sizes.stdout.splitlines()
                                          if len(parts := line.split('\t')) == 2 and parts[0] == '443')
        if entry['status_code'] != '200' or 'codex-plugins.zip' not in (entry.get('content_disposition') or ''):
            continue
        export = plugin_stream_objects(pcap, key_options, diagnostics, stream)
        for body in export.iterdir():
            if not body.is_file():
                continue
            # This filtered connection must contain only the single content request.
            with body.open('rb') as prefix:
                is_zip_body = prefix.read(4) == b'PK\x03\x04'
            if not is_zip_body:
                continue
            entry['exported_body_bytes'] = max(entry['exported_body_bytes'] or 0, body.stat().st_size)
            entry['zip_complete'] = False
            try:
                with zipfile.ZipFile(body) as archive:
                    if (len(archive.infolist()) > 20000
                            or sum(x.file_size for x in archive.infolist()) > 256 * 1024 * 1024):
                        continue
                    if archive.testzip() is not None:
                        continue
                    entry.update(zip_complete=True, zip_members=len(archive.infolist()),
                                 zip_sha256=sha256_file(body))
                    if entry['curated_metadata_url_verified'] and request_counts.get(stream) == 1:
                        archives.append(body)
            except (zipfile.BadZipFile, OSError, RuntimeError, EOFError):
                pass
    result['downloads'] = list(streams.values())
    result['complete_external_zip_count'] = sum(x['zip_complete'] is True and x['leg'] == 'external'
                                               for x in streams.values())
    result['complete_zip_observed'] = any(x['zip_complete'] is True for x in streams.values())
    result['status'] = 'observed' if streams else 'not_observed'
    return result, archives, revisions


def pcapng_capture_counters(path):
    """Read packet counts and final Interface Statistics Blocks before editcap.

    Some rewrite tools discard ISBs. Missing ifdrop is unknown, never zero.
    Only a single capture section is expected from this runner's dumpcap.
    """
    result = {'packet_blocks': None, 'interface_statistics': [], 'parse_error': None}
    if path is None or not path.exists() or not path.stat().st_size:
        return result
    endian, section, packets, statistics = '<', -1, 0, {}
    try:
        with path.open('rb') as stream:
            while True:
                header = stream.read(12)
                if not header:
                    break
                if len(header) != 12:
                    raise ValueError('Truncated PCAPNG block header')
                if header[:4] == b'\x0a\x0d\x0d\x0a':
                    if header[8:12] not in (b'\x4d\x3c\x2b\x1a', b'\x1a\x2b\x3c\x4d'):
                        raise ValueError('Invalid PCAPNG byte order')
                    endian = '<' if header[8:12] == b'\x4d\x3c\x2b\x1a' else '>'
                    section += 1
                kind, size = struct.unpack_from(endian + 'II', header)
                if section < 0 or size < 12 or size % 4:
                    raise ValueError('Invalid PCAPNG block length or missing section header')
                if kind == 5:
                    if size > 1024 * 1024:
                        raise ValueError('Oversized interface statistics block')
                    block = header + stream.read(size - 12)
                    if len(block) != size:
                        raise ValueError('Truncated PCAPNG statistics block')
                    interface = struct.unpack_from(endian + 'I', block, 8)[0]
                    entry = {'section': section, 'interface': interface}
                    offset = 20
                    while offset + 4 <= size - 4:
                        code, length = struct.unpack_from(endian + 'HH', block, offset)
                        offset += 4
                        if code == 0:
                            break
                        if offset + length > size - 4:
                            raise ValueError('Truncated PCAPNG statistics option')
                        if code in (4, 5, 7) and length == 8:
                            key = {4: 'received_packets', 5: 'dropped_packets', 7: 'os_dropped_packets'}[code]
                            entry[key] = struct.unpack_from(endian + 'Q', block, offset)[0]
                        offset += (length + 3) // 4 * 4
                    statistics[(section, interface)] = entry
                    trailer = block[-4:]
                else:
                    stream.seek(size - 16, 1) if size >= 16 else None
                    trailer = stream.read(4) if size >= 16 else header[-4:]
                    if kind in (2, 3, 6):
                        packets += 1
                if len(trailer) != 4 or struct.unpack(endian + 'I', trailer)[0] != size:
                    raise ValueError('Invalid or truncated PCAPNG block trailer')
        result.update(packet_blocks=packets, interface_statistics=list(statistics.values()))
    except (OSError, ValueError, struct.error) as exc:
        result['parse_error'] = str(exc)
    return result


def capture_statistics(log, pcap, args, exit_code=None):
    counters = pcapng_capture_counters(pcap)
    captured_matches = re.findall(r'Packets captured:\s*(\d+)', log)
    captured = int(captured_matches[-1]) if captured_matches else counters['packet_blocks']
    interfaces = {}
    for match in re.finditer(r"Packets received/dropped on interface '([^']+)':\s*(\d+)/(\d+)([^\n]*)", log):
        name, received, dropped, tail = match.groups()
        interfaces[name] = {'interface': name, 'received_packets': int(received),
                            'dropped_packets': int(dropped),
                            'breakdown': {k: int(v) for k, v in re.findall(
                                r'(pcap|dumpcap|flushed|ps_ifdrop):(\d+)', tail)}}
    source, dropped = None, None
    if interfaces:
        dropped, source = sum(x['dropped_packets'] for x in interfaces.values()), 'dumpcap_final_summary'
    elif counters['interface_statistics'] and all('dropped_packets' in x for x in counters['interface_statistics']):
        dropped = sum(x['dropped_packets'] for x in counters['interface_statistics'])
        source = 'pcapng_interface_statistics'
    denominator = captured + dropped if captured is not None and dropped is not None else None
    rate = dropped / denominator if denominator else None
    warnings = []
    if dropped is None:
        warnings.append('Dropped packets were not reported; zero loss is NOT established.')
    elif dropped:
        warnings.append(f'Capture lost {dropped} packets ({rate * 100:.6f}%). Zero-loss target NOT met.'
                        if rate is not None else f'Capture lost {dropped} packets. Zero-loss target NOT met.')
    if not captured:
        warnings.append('No captured packets were confirmed.')
    if exit_code != 0:
        warnings.append('dumpcap did not complete normally, or capture never started.')
    if counters['parse_error']:
        warnings.append('Capture file parse error: ' + counters['parse_error'])
    if (captured_matches and counters['packet_blocks'] is not None
            and captured != counters['packet_blocks']):
        warnings.append('Captured packet summary differs from packet blocks written to disk.')
    quality_ok = (dropped == 0 and captured is not None and captured > 0 and not warnings)
    return {'captured_packets': captured, 'dropped_packets': dropped, 'drop_rate': rate,
            'drop_rate_percent': rate * 100 if rate is not None else None,
            'drop_rate_formula': 'dropped / (captured + dropped)',
            'statistics_source': source, 'zero_loss_verified': quality_ok,
            'status': 'zero_loss' if quality_ok else 'packet_loss' if dropped else 'unverified',
            'warnings': warnings, 'interfaces': list(interfaces.values()),
            'pcapng_counters_before_tls_embedding': counters, 'dumpcap_exit_code': exit_code,
            'interface': 'any', 'capture_filter': None,
            'kernel_buffer_mib_requested': args.capture_buffer_mib,
            'kernel_buffer_mib_effective': None,
            'userspace_queue_mib': args.capture_queue_mib,
            'write_strategy': 'temporary spool; copy to result after dumpcap stops',
            'spool_directory': str(args.capture_spool_dir)}


def now():
    return datetime.now(timezone.utc).isoformat()


def write_json(path, value):
    """Replace resume metadata atomically; never retain a partial JSON file."""
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


@contextmanager
def finishing():
    """Let the first Ctrl+C stop work; defer further ones during evidence cleanup."""
    def remember_interrupt(signum, frame):
        global _interrupt_requested
        _interrupt_requested = True
    previous = signal.signal(signal.SIGINT, remember_interrupt)
    try:
        yield
    finally:
        signal.signal(signal.SIGINT, previous)


def stop(proc):
    """Stop a child session, escalating only when its leader does not exit."""
    if proc is None:
        return
    for sig, timeout in [(signal.SIGINT, 5), (signal.SIGTERM, 2), (signal.SIGKILL, 2)]:
        try:
            os.killpg(proc.pid, sig)
        except ProcessLookupError:
            proc.wait(timeout=timeout)
            return
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            continue
        # Remove any remaining members after the session leader exits.
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        return
    raise RuntimeError(f'Process cleanup did not finish: {proc.pid}')


def stop_task_processes(proxy, capture):
    errors=[]
    for fn,proc in ((stop,proxy),(stop,capture)):
        try: fn(proc)
        except Exception as exc: errors.append(str(exc))
    if errors: raise RuntimeError('Task process cleanup failed: '+'; '.join(errors))


@contextmanager
def task_runtime(diagnostics, stop_capture):
    with tempfile.TemporaryDirectory(prefix='gaia-codex-') as temp:
        runtime = Path(temp)
        try:
            yield runtime
        finally:
            with finishing():
                try:
                    stop_capture()
                finally:
                    keys = runtime / 'home/codex_tls_keys.log'
                    if keys.exists():
                        shutil.copyfile(keys, diagnostics / 'codex_tls_keys.log')


def completed_task(saved, task_dir):
    """A wrong solution is complete; interrupted executions are retryable."""
    try:
        if not isinstance(saved, dict):
            return False
        output = json.loads((task_dir / 'codex_output.json').read_text())
        if saved.get('experiment', {}).get('capture_server'):
            measurement = saved.get('measurement', {})
            if (not measurement.get('server_record_complete') or not measurement.get('coverage', {}).get('local_covers_agent')
                    or measurement.get('quality_passed') is False
                    or (measurement.get('data_quality') or {}).get('quality_passed') is False):
                return False
            for name in ['server_traffic.pcapng', 'server_timeline.jsonl']:
                path = task_dir / name
                if not path.is_file() or path.stat().st_size != measurement['server_collection']['files'][name]['bytes']:
                    return False
        capture = saved.get('capture', {})
        traffic = saved.get('traffic', {})
        counters = pcapng_capture_counters(task_dir / 'raw_traffic.pcapng')
        return (saved.get('status', 'completed') == 'completed'
                and saved.get('setup_ok') and not saved.get('error')
                and output.get('succeeded') is True and output.get('exit_code') == 0
                and not output.get('timed_out') and not output.get('interrupted')
                and isinstance(saved.get('verification'), dict)
                and (saved['verification'].get('passed') is True or (saved.get('scoring_available') is False and saved.get('agent_status') == 'unscored'))
                and saved.get('quality_passed', True) is True
                and saved.get('cleanup_status', 'confirmed') == 'confirmed'
                and capture.get('zero_loss_verified') is True
                and not counters['parse_error']
                and (counters['packet_blocks'] or 0) > 0
                and counters['packet_blocks'] == capture.get('captured_packets')
                and traffic.get('transport_verified') is True
                and (not saved.get('experiment', {}).get('tls_inspection', True)
                     or traffic.get('tls_keys_embedded') is True)
                and all((task_dir / name).is_file() for name in
                        ['task_prompt.txt', 'codex_timeline.jsonl', 'raw_traffic.pcapng'])
                and (task_dir / 'raw_traffic.pcapng').stat().st_size > 0)
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        return False


def infrastructure_error(result, reason, exc=None):
    """Keep failure domains independent; only this domain blocks a batch."""
    result.setdefault('infrastructure_errors', []).append(str(reason))
    result['error'] = result.get('error') or str(reason)
    if exc is not None:
        result.setdefault('tracebacks', []).append(traceback.format_exc())
        print(result['tracebacks'][-1], file=sys.stderr, flush=True)


def task_outcome(result, args, output):
    """Classify without changing any packet/SSE completeness predicate."""
    measurement = result.get('measurement') or {}
    capture = result.get('capture') or {}
    traffic = result.get('traffic') or {}
    if not isinstance(output, dict):
        output = {}
        result.setdefault('quality_issues', []).append('invalid_codex_output')
    interrupted = (result.get('status') == 'interrupted' or output.get('interrupted')
                   or _interrupt_requested)
    if interrupted:
        agent_status = 'interrupted'
    elif output.get('timed_out'):
        agent_status = 'timed_out'
    elif not result.get('setup_ok') or output.get('status') == 'not_started':
        agent_status = 'not_started'
    elif not output.get('succeeded'):
        agent_status = 'failed'
    elif result.get('verify_passed') is False:
        agent_status = 'answer_format_invalid' if result.get('verification', {}).get('format_valid') is False else 'verification_failed'
    elif result.get('verify_passed') is True:
        agent_status = 'passed'
    else:
        agent_status = 'unscored' if result.get('scoring_available') is False and result.get('verification', {}).get('format_valid') else 'answer_format_invalid'
    quality = []
    if not capture.get('zero_loss_verified'):
        quality.append('local_capture_quality')
    if not traffic.get('transport_verified'):
        quality.append('transport_unverified')
    if (not args.no_tls_inspection and not traffic.get('tls_keys_embedded')
            and traffic.get('tls_inspection_status') not in ('not_applicable','proxy_not_observed')):
        quality.append('tls_keys_unverified')
    if args.capture_server:
        if not measurement.get('server_record_complete'):
            quality.append('server_record_incomplete')
        if not measurement.get('coverage', {}).get('local_covers_agent'):
            quality.append('local_agent_coverage_unverified')
        if (measurement.get('quality_passed') is False
                or (measurement.get('data_quality') or {}).get('quality_passed') is False):
            quality.append('server_quality_failed')
    if result.get('quality_passed') is False:
        quality.append('existing_quality_failed')
    quality.extend(result.get('quality_issues', []))
    if result.get('error') and agent_status in ('passed','unscored') and not result.get('infrastructure_errors') and not quality:
        quality.append('existing_task_error')
    result.update(agent_status=agent_status, quality_passed=not quality,
                  quality_issues=list(dict.fromkeys(quality)))
    if interrupted:
        status = 'interrupted'
    elif result.get('infrastructure_errors') or result.get('cleanup_status') != 'confirmed':
        status = 'infrastructure_failed'
    elif agent_status not in ('passed','unscored') or quality:
        status = 'failed'
    else:
        status = 'completed'
    result.update(status=status, completed=(status == 'completed'))
    if status != 'completed' and not result.get('error'):
        result['error'] = ('Interrupted by user' if interrupted else
                           '; '.join(result.get('infrastructure_errors', [])) or
                           (agent_status if agent_status not in ('passed','unscored') else ', '.join(quality)))
    if status == 'interrupted':
        result['interruption_reason'] = 'user_interrupt'
    elif result.get('error'):
        result['interruption_reason'] = result['error']
    return status


def resume_outcome(saved, directory):
    """Read both new and legacy results without modifying them."""
    if not isinstance(saved, dict):
        return 'unfinished'
    if completed_task(saved, directory):
        return 'completed'
    if saved.get('status') in ('running', 'interrupted', 'infrastructure_failed'):
        return saved['status']
    if (saved.get('infrastructure_errors') or not saved.get('setup_ok')
            or saved.get('cleanup_status') == 'unconfirmed'):
        return 'infrastructure_failed'
    # Legacy records lack a dedicated cleanup flag. Require positive evidence
    # before treating one as an ordinary failed attempt that may be skipped.
    if 'cleanup_status' not in saved:
        if saved.get('capture', {}).get('dumpcap_exit_code') != 0:
            return 'unfinished'
        if saved.get('experiment', {}).get('capture_server'):
            if saved.get('measurement', {}).get('server_stopped', {}).get('state') != 'STOPPED':
                return 'unfinished'
    if saved.get('ended_at') and saved.get('status') in ('completed', 'failed'):
        return 'failed'
    return 'unfinished'


def archive_attempt(run_root, task_dir):
    """Move a whole old attempt atomically, never truncate its seven files."""
    history = run_root / '.attempt-history' / task_dir.name
    if history.is_symlink() or history.parent.is_symlink(): raise RuntimeError('Unsafe attempt history symlink')
    history.mkdir(mode=0o700, parents=True, exist_ok=True)
    destination = history / (datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S') + '-' + str(time.time_ns()))
    if destination.exists():
        raise FileExistsError(destination)
    task_dir.rename(destination)
    for parent in (history, history.parent, run_root):
        fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    return str(destination.relative_to(run_root))


def batch_checkpoint(run_root, experiment, task_id=None, state=None, result=None):
    progress = experiment['progress']
    if task_id is not None:
        entry = progress[task_id]
        if state == 'running':
            for key in ('agent_status', 'quality_passed', 'cleanup_status', 'error', 'infrastructure_errors', 'quality_issues'):
                entry.pop(key, None)
        entry.update(state=state, updated_at=now(), scheduled=state not in ('completed', 'failed'))
        if result is not None:
            for key in ('agent_status', 'quality_passed', 'cleanup_status', 'error',
                        'infrastructure_errors', 'quality_issues'):
                if key in result:
                    entry[key] = result[key]
    experiment['next_task_id'] = next((key for key, entry in progress.items()
                                      if entry.get('scheduled', True)), None)
    write_json(run_root / 'experiment.json', experiment)


def server_cleanup_check(spark, task_measurement, result):
    """Evaluate ownership/safety AFTER capture shutdown, not SSE completeness."""
    confirmed = True
    if task_measurement is not None:
        measurement = result.get('measurement') or {}
        stopped = measurement.get('server_stopped') or {}
        if getattr(task_measurement, 'started', True) and stopped.get('state') != 'STOPPED':
            confirmed = False
            infrastructure_error(result, 'Remote capture stop not confirmed: ' + str(stopped))
        if stopped.get('stop_reason') == 'model/watchdog resource safety failed':
            infrastructure_error(result, stopped['stop_reason'])
        last = measurement.get('last_server_status') or {}
        if last.get('model', {}).get('safe') is False:
            infrastructure_error(result, 'Model/watchdog unsafe during task')
    if spark is not None and result.get('status') != 'interrupted' and not _interrupt_requested:
        try:
            if spark.tunnel is None or spark.tunnel.poll() is not None:
                raise RuntimeError('SSH inference tunnel is not running')
            state = spark.call('model', timeout=25)
            result['post_task_model_health'] = state
            if not state.get('safe') or state.get('health') != 200:
                raise RuntimeError('Spark model/watchdog not safe or HTTP health not 200')
            old_id = (spark.meta.get('ready_state') or {}).get('container_id')
            if old_id and old_id != state.get('container_id'):
                raise RuntimeError('Spark model container changed during batch')
            # Outside both capture intervals: do not add traffic to any sample.
            spark.http('/health', timeout=5)
        except Exception as exc:
            infrastructure_error(result, 'Post-task Spark health check: ' + str(exc), exc)
    return confirmed


def recover_server_raw(spark, task_measurement, task_dir, result):
    """Parser failure must not prevent recovery of the original remote PCAPNG.

    Use the existing SSH config and result path. Never change remote files.
    """
    measurement = result.get('measurement') or {}
    if (measurement.get('server_stopped') or {}).get('state') != 'STOPPED':
        return
    dest = task_dir / 'server_traffic.pcapng'
    reported = (measurement.get('server_collection') or {}).get('files', {}).get('server_traffic.pcapng', {}).get('bytes')
    if dest.is_file() and dest.stat().st_size > 0 and (reported is None or dest.stat().st_size == reported):
        return
    from spark_measurement import REMOTE
    remote = f'{REMOTE}/logs/agent-capture/{spark.run_id}/{task_measurement.task}/server_traffic.pcapng'
    try:
        expected = int(reported) if reported is not None else int(spark.raw('stat -c %s -- ' + shlex.quote(remote), timeout=20).strip())
        subprocess.run(['scp', '-F', str(ROOT / 'config/ssh_config'), '-o', 'BatchMode=yes',
                        '-o', 'ConnectTimeout=8', f'spk:{remote}', str(dest)+'.recovery-partial'],
                       check=True, capture_output=True, timeout=120)
        recovered = dest.with_name(dest.name+'.recovery-partial')
        if expected <= 0 or recovered.stat().st_size != expected:
            raise RuntimeError('Recovered server PCAPNG size mismatch')
        digest = (measurement.get('server_collection') or {}).get('files', {}).get('server_traffic.pcapng', {}).get('sha256')
        if digest and sha256_file(recovered) != digest: raise RuntimeError('Recovered server PCAPNG hash mismatch')
        os.replace(recovered,dest)
        measurement['raw_capture_recovery'] = {'status': 'recovered', 'bytes': expected, 'remote': remote}
    except Exception as exc:
        measurement['raw_capture_recovery'] = {'status': 'failed', 'remote': remote, 'error': str(exc)}
        # The stopped session remains on Spark. Do not confuse a collection/
        # parsing failure with an unsafe worker. A separate health check follows.
        result.setdefault('quality_issues', []).append('server_raw_recovery_failed')
        result.setdefault('tracebacks', []).append(traceback.format_exc())
        print('[CAPTURE WARNING] Cannot recover server raw capture: ' + str(exc), file=sys.stderr, flush=True)
        if isinstance(exc, OSError) and exc.errno in (errno.ENOSPC, errno.EDQUOT):
            infrastructure_error(result, 'Disk space exhausted during raw capture recovery', exc)


def native_codex():
    executable = shutil.which('codex')
    if not executable:
        raise RuntimeError('codex is not in PATH')
    path = Path(executable).resolve()
    # Follow installed shell wrappers as data, never source their auth loaders.
    for _ in range(5):
        if not path.is_file() or path.read_bytes()[:2] != b'#!':
            break
        match = re.search(r'^\s*exec\s+(/[A-Za-z0-9_./-]+)', path.read_text(errors='replace'), re.M)
        if not match or not Path(match.group(1)).is_file():
            break
        candidate = Path(match.group(1)).resolve()
        if candidate == path:
            break
        path = candidate
    def is_elf(candidate):
        with candidate.open('rb') as handle:
            return handle.read(4) == b'\x7fELF'
    if is_elf(path):
        return path
    # npm installs a Node launcher next to a platform-specific native binary.
    package = path.parent.parent
    for candidate in package.glob('node_modules/@openai/codex-*/vendor/*/bin/codex'):
        if is_elf(candidate):
            return candidate
    for candidate in package.glob('vendor/*/bin/codex'):
        if is_elf(candidate):
            return candidate
    raise RuntimeError('Cannot locate the native Codex ELF binary')


def code_mode_host(binary, override=None):
    """Find the native helper matching the installed Codex CLI.

    Code-mode-only models cannot run tools without this executable. The CLI
    discovers the helper beside its own binary, so we must bind it to the same
    path inside bubblewrap instead of mounting only /opt/bin/codex.
    """
    if override is not None:
        candidates = [override.expanduser()]
    else:
        candidates = [binary.with_name('codex-code-mode-host')]
        launcher = shutil.which('codex')
        if launcher:
            launcher_dir = Path(launcher).resolve().parent
            candidates.extend([launcher_dir / 'codex-code-mode-host',
                               launcher_dir / 'bin' / 'codex-code-mode-host'])
        helper_on_path = shutil.which('codex-code-mode-host')
        if helper_on_path:
            candidates.append(Path(helper_on_path))
    for candidate in candidates:
        path = candidate.resolve()
        if not path.is_file() or not os.access(path, os.X_OK):
            continue
        with path.open('rb') as stream:
            if stream.read(4) == b'\x7fELF':
                return path
    raise RuntimeError(
        'Missing codex-code-mode-host (required for code-mode-only models). '
        f'Codex binary: {binary}. Install the helper from the SAME Codex '
        'CLI release, or pass --code-mode-host /absolute/path/to/codex-code-mode-host. '
        'Do not disable code_mode_host as a workaround: code-mode-only models '
        'would still be unable to call tools.'
    )


def isolated(runtime, binary, command, host_binary=None):
    cmd = ['bwrap', '--unshare-all', '--share-net', '--die-with-parent',
           '--new-session', '--cap-drop', 'ALL']
    for path in ['/usr', '/bin', '/sbin', '/lib', '/lib64', '/etc/ssl/certs',
                 '/etc/resolv.conf', '/etc/hosts', '/etc/nsswitch.conf', '/etc/os-release']:
        if Path(path).exists():
            cmd += ['--ro-bind', path, path]
    venv = os.environ.get('GAIA_AGENT_VENV')
    if venv and Path(venv).is_dir():
        cmd += ['--ro-bind', venv, venv]
    cmd += ['--ro-bind', str(binary), '/opt/bin/codex']
    if host_binary is not None:
        cmd += ['--ro-bind', str(host_binary), '/opt/bin/codex-code-mode-host']
    cmd += ['--bind', str(runtime / 'home'), '/home/runner',
            '--bind', str(runtime / 'work'), '/work',
            '--proc', '/proc', '--dev', '/dev', '--tmpfs', '/tmp',
            '--chdir', '/work']
    catalog = runtime / 'home/.codex/model_catalog.json'
    if catalog.is_file():
        cmd += ['--ro-bind', str(catalog), '/home/runner/.codex/model_catalog.json']
    return cmd + ['--'] + command


def start_proxy(run_root, args):
    if args.no_tls_inspection:
        return None, None, None
    executable = args.mitmdump or shutil.which('mitmdump')
    fallback = Path('/root/.local/share/claude-tls/venv/bin/mitmdump')
    if not executable and fallback.is_file():
        executable = str(fallback)
    if not executable:
        raise RuntimeError('TLS inspection requires mitmdump; install mitmproxy into .venv')
    try:
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
    except OSError as exc:
        raise RuntimeError(f'TLS inspection proxy cannot open a local listening socket: {exc}') from exc
    confdir = run_root / 'tls_proxy_ca'
    env = {'PATH':os.environ.get('PATH','/usr/bin:/bin'), 'HOME':str(run_root), 'LANG':'C.UTF-8'}
    env['SSLKEYLOGFILE'] = str(run_root / 'proxy_tls_keys.log')
    upstream = os.environ.get('GAIA_UPSTREAM_PROXY')
    command = [str(executable), '--listen-host', '127.0.0.1',
        '--listen-port', str(port), '--set', f'confdir={confdir}',
        '--set', 'connection_strategy=lazy', '--set', 'flow_detail=0',
        '--set', 'http2=false']
    if upstream:
        command += ['--mode', 'upstream:' + upstream.rstrip('/')]
    log = (run_root / 'tls_proxy.log').open('wb')
    try:
        proc = subprocess.Popen(command, env=env,
            stdout=log, stderr=log, start_new_session=True)
    finally:
        log.close()
    ca = confdir / 'mitmproxy-ca-cert.pem'
    try:
        for _ in range(100):
            if proc.poll() is not None:
                raise RuntimeError('TLS proxy exited; see experiment.diagnostics in result.json')
            if ca.exists():
                try:
                    with socket.create_connection(('127.0.0.1', port), .2):
                        return proc, f'http://127.0.0.1:{port}', ca
                except OSError:
                    pass
            time.sleep(.1)
        raise RuntimeError('TLS proxy startup timed out')
    except BaseException:
        stop(proc)
        raise


def solver_env(runtime, provider, proxy, ca):
    allowed = {'PATH', 'LANG', 'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'NO_PROXY',
               'http_proxy', 'https_proxy', 'all_proxy', 'no_proxy',
               'SSL_CERT_FILE', 'SSL_CERT_DIR', 'CODEX_CA_CERTIFICATE'}
    allowed.add(provider.get('env_key', 'OPENAI_API_KEY'))
    allowed.update(provider.get('env_http_headers', {}).values())
    env = {key: value for key, value in os.environ.items() if key in allowed}
    venv = os.environ.get('GAIA_AGENT_VENV')
    path = '/opt/bin:/usr/bin:/bin'
    if venv and (Path(venv) / 'bin').is_dir():
        path = str(Path(venv) / 'bin') + ':' + path
    env.update(HOME='/home/runner', CODEX_HOME='/home/runner/.codex',
               PATH=path, TMPDIR='/tmp',
               SSLKEYLOGFILE='/home/runner/codex_tls_keys.log')
    if venv:env['GAIA_AGENT_VENV'] = venv
    external_proxy = os.environ.get('GAIA_UPSTREAM_PROXY')
    for key in ('HTTP_PROXY','HTTPS_PROXY','ALL_PROXY','http_proxy','https_proxy','all_proxy','NO_PROXY','no_proxy','SSL_CERT_FILE','SSL_CERT_DIR','CODEX_CA_CERTIFICATE','OPENAI_API_KEY'):
        env.pop(key, None)
    # Model inference stays direct on the loopback tunnel; only external traffic uses a proxy.
    host = urlparse(provider.get('base_url', '')).hostname
    # Exa MCP is deliberately direct: the Windows upstream proxy cannot complete
    # its Streamable HTTP TLS/SSE handshake. Other external traffic keeps the proxy.
    bypass = ','.join(filter(None, ['localhost','127.0.0.1','::1',host,'192.168.3.125',EXA_MCP_HOST]))
    env['NO_PROXY'] = env['no_proxy'] = bypass
    if not proxy:
        env.pop('SSLKEYLOGFILE',None)
        # Explicit opt-in process-level upstream proxy for public Internet access.
        if external_proxy:
            env['HTTP_PROXY'] = env['http_proxy'] = external_proxy
            env['HTTPS_PROXY'] = env['https_proxy'] = external_proxy
    if proxy:
        env['HTTPS_PROXY'] = env['https_proxy'] = proxy
        env['HTTP_PROXY'] = env['http_proxy'] = proxy
        destination = runtime / 'home' / 'inspection-ca.pem'
        destination.write_bytes(Path('/etc/ssl/certs/ca-certificates.crt').read_bytes()
                                + b'\n' + ca.read_bytes())
        env['CODEX_CA_CERTIFICATE'] = env['SSL_CERT_FILE'] = '/home/runner/inspection-ca.pem'
        env['CURL_CA_BUNDLE'] = env['REQUESTS_CA_BUNDLE'] = env['SSL_CERT_FILE']
    return env



def shell_command_class(command):
    """Classify observed local command intent without claiming packet ownership."""
    text = command if isinstance(command, str) else ' '.join(map(str, command or []))
    try:
        tokens = shlex.split(text)
    except ValueError:
        tokens = text.split()
    lowered = text.lower()
    if any(token.rsplit('/', 1)[-1] in ('curl', 'wget') for token in tokens):
        return 'WEB_FETCH'
    if any(marker in lowered for marker in (
            'urllib.request', 'requests.get(', 'requests.post(', 'httpx.get(',
            'httpx.post(', 'aiohttp', 'https://', 'http://')):
        return 'WEB_FETCH'
    return 'LOCAL_TOOL'


def json_content_bytes(value):
    return len(json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode('utf-8'))


class CodexTimelineRecorder:
    """Preserve raw Codex JSONL and append derived lifecycle records to the same timeline.

    tool_call_id is Codex ThreadItem.id, not the MCP JSON-RPC request id.
    All timestamps are CLI event receipt times, not packet or remote execution times.
    """
    def __init__(self, trace, task_id=None, run_id=None):
        self.trace = trace
        self.task_id = task_id
        self.run_id = run_id
        self.turn = 0
        self.active = {}
        self.event_count = 0
        self.tool_call_count = 0
        self.errors = []
        self.final_message = None
        self.summary_events = []

    def _write(self, event, source='codex_cli_raw'):
        stamp = now()
        record = {'timestamp_utc': stamp, 'unix_ns': time.time_ns(),
                  'source': source, 'timestamp_semantics': 'codex_cli_event_received',
                  'task_id': self.task_id, 'run_id': self.run_id,
                  'agent_turn': self.turn, 'event': event}
        self.trace.write(json.dumps(record, ensure_ascii=False) + '\n')
        self.trace.flush()

    def _record_unsupported_tool_error(self, message):
        match = re.search(r'unsupported call\s*:\s*([A-Za-z0-9_.-]+)', message, re.I)
        if not match:
            return
        self._write({
            'event_type': 'tool_dispatch_error',
            'communication_class': 'CODEX_TOOL_DISPATCH',
            'tool_name': match.group(1),
            'error_message': message,
            'task_id': self.task_id, 'run_id': self.run_id,
            'agent_turn': self.turn,
            'timestamp_semantics': 'Codex CLI error event receipt time; no network request implied',
        }, 'codex_cli_derived')

    def _abort_active_calls(self, event_type, event):
        """Close outstanding CLI tool items when their Agent turn fails before item.completed."""
        error = event.get('message', event.get('error', event_type))
        for item_id, active in list(self.active.items()):
            record = active.get('record') or {}
            kind = active.get('kind')
            elapsed = ((time.monotonic() - active['mono']) * 1000
                       if active.get('mono') is not None else None)
            if kind == 'mcp':
                terminal = {
                    'event_type': 'mcp_tool_aborted',
                    'communication_class': record.get('communication_class', 'MCP'),
                    'server': record.get('server'), 'tool': record.get('tool'),
                    'tool_call_id': item_id,
                    'tool_call_id_source': 'Codex ThreadItem.id; not the MCP JSON-RPC request id',
                }
            else:
                terminal = {
                    'event_type': 'command_execution_aborted',
                    'communication_class': record.get('communication_class', 'LOCAL_TOOL'),
                    'codex_item_id': item_id, 'command': record.get('command'),
                }
            terminal.update({
                'tool_status': 'failed',
                'error_message': str(error),
                'call_started_at': active.get('wall'),
                'call_ended_at': now(),
                'tool_duration_ms': elapsed,
                'task_id': self.task_id, 'run_id': self.run_id,
                'agent_turn': self.turn, 'started_agent_turn': active.get('turn'),
                'termination_reason': 'agent_turn_terminated_before_item_completed',
                'timestamp_semantics': 'CLI turn.failed/error receipt time; synthesized terminal cleanup, not an observed item.completed',
            })
            self._write(terminal, 'codex_cli_derived')
            self.active.pop(item_id, None)

    def record(self, line):
        try:
            event = json.loads(line)
            if not isinstance(event, dict):
                raise ValueError('Non-object event')
        except ValueError:
            event = {'type': 'unparsed', 'text': line.decode(errors='replace')}
        self.event_count += 1
        event_type = event.get('type')
        if event_type == 'turn.started':
            self.turn += 1

        # Raw event is always retained before any derived interpretation.
        self._write(event, 'codex_cli_raw')

        if event_type in ('thread.started', 'turn.completed', 'turn.failed', 'error'):
            self.summary_events.append(event)
        if event_type in ('error', 'turn.failed'):
            message = str(event.get('message', event.get('error', event_type)))
            self.errors.append(message)
            self._record_unsupported_tool_error(message)

        if event_type in ('turn.started', 'turn.completed', 'turn.failed'):
            self._write({
                'event_type': 'llm_turn_request' if event_type == 'turn.started'
                              else 'llm_turn_response' if event_type == 'turn.completed'
                              else 'llm_turn_failed',
                'communication_class': 'LLM',
                'task_id': self.task_id, 'run_id': self.run_id,
                'agent_turn': self.turn,
                'timestamp_semantics': 'Codex CLI lifecycle event; not network send/receive time',
                'usage': event.get('usage'),
                'error': event.get('error'),
            }, 'codex_cli_derived')

        item = event.get('item') or {}
        if not isinstance(item, dict):
            item = {}
        item_type = item.get('type')
        item_id = item.get('id')
        if item_type == 'agent_message':
            self.final_message = item.get('text')
        if item_type == 'error':
            message = str(item.get('message', item.get('error', 'Codex emitted an error item')))
            self.errors.append(message)
            if event_type not in ('error', 'turn.failed'):
                self._record_unsupported_tool_error(message)

        if item_type not in ('mcp_tool_call', 'command_execution'):
            if event_type in ('turn.failed', 'error') and self.active:
                self._abort_active_calls(event_type, event)
            # Retain server lifecycle/initialization errors in the raw stream and add
            # a compact searchable event when Codex exposes an MCP-specific event.
            if isinstance(event_type, str) and 'mcp' in event_type.lower():
                self._write({'event_type': 'mcp_lifecycle_event',
                             'communication_class': 'MCP',
                             'codex_event_type': event_type,
                             'task_id': self.task_id, 'run_id': self.run_id,
                             'agent_turn': self.turn, 'raw_event': event},
                            'codex_cli_derived')
            return

        if event_type == 'item.started':
            self.tool_call_count += 1
            started_wall = now()
            started_ns = time.time_ns()
            started_mono = time.monotonic()
            if item_type == 'mcp_tool_call':
                server = item.get('server')
                tool = item.get('tool')
                communication = ('MCP_SEARCH' if str(server).lower() == 'exa'
                                  and tool == 'web_search_exa' else 'MCP')
                record = {
                    'event_type': 'mcp_search_request' if communication == 'MCP_SEARCH' else 'mcp_tool_request',
                    'communication_class': communication,
                    'server': server, 'tool': tool,
                    'tool_call_id': item_id,
                    'tool_call_id_source': 'Codex ThreadItem.id; not the MCP JSON-RPC request id',
                    'arguments': item.get('arguments'),
                    'call_started_at': started_wall, 'call_started_unix_ns': started_ns,
                    'task_id': self.task_id, 'run_id': self.run_id,
                    'agent_turn': self.turn, 'tool_status': item.get('status', 'in_progress'),
                    'timestamp_semantics': 'CLI item.started receipt time',
                }
                self.active[item_id] = {'kind': 'mcp', 'wall': started_wall,
                                        'mono': started_mono, 'record': record,
                                        'turn': self.turn}
            else:
                command = item.get('command', '')
                communication = shell_command_class(command)
                record = {
                    'event_type': 'web_fetch_request' if communication == 'WEB_FETCH' else 'local_tool_request',
                    'communication_class': communication,
                    'codex_item_id': item_id,
                    'command': command,
                    'call_started_at': started_wall, 'call_started_unix_ns': started_ns,
                    'task_id': self.task_id, 'run_id': self.run_id,
                    'agent_turn': self.turn, 'tool_status': item.get('status', 'in_progress'),
                    'timestamp_semantics': 'CLI item.started receipt time',
                }
                self.active[item_id] = {'kind': 'command', 'wall': started_wall,
                                        'mono': started_mono, 'record': record,
                                        'turn': self.turn}
            self._write(record, 'codex_cli_derived')

        elif event_type == 'item.updated':
            # item.updated is an intermediate snapshot, never a terminal tool response.
            active = self.active.get(item_id)
            kind = active.get('kind') if active else ('mcp' if item_type == 'mcp_tool_call' else 'command')
            started_wall = active.get('wall') if active else None
            started_turn = active.get('turn') if active else None
            if kind == 'mcp':
                server = item.get('server') or ((active or {}).get('record') or {}).get('server')
                tool = item.get('tool') or ((active or {}).get('record') or {}).get('tool')
                communication = ('MCP_SEARCH' if str(server).lower() == 'exa'
                                  and tool == 'web_search_exa' else 'MCP')
                partial_result = item.get('result')
                self._write({
                    'event_type': 'mcp_tool_progress',
                    'communication_class': communication,
                    'server': server, 'tool': tool,
                    'tool_call_id': item_id,
                    'tool_call_id_source': 'Codex ThreadItem.id; not the MCP JSON-RPC request id',
                    'progress_status': item.get('status', 'in_progress'),
                    'progress': item.get('progress'),
                    'partial_result_bytes': json_content_bytes(partial_result) if partial_result is not None else 0,
                    'call_started_at': started_wall,
                    'task_id': self.task_id, 'run_id': self.run_id,
                    'agent_turn': self.turn, 'started_agent_turn': started_turn,
                    'start_observed': active is not None,
                    'timestamp_semantics': 'CLI item.updated receipt time; progress only, not final response',
                }, 'codex_cli_derived')
            else:
                output = item.get('aggregated_output', '')
                output_bytes = len(output.encode('utf-8')) if isinstance(output, str) else json_content_bytes(output)
                command = item.get('command') or ((active or {}).get('record') or {}).get('command', '')
                communication = shell_command_class(command)
                self._write({
                    'event_type': 'command_execution_progress',
                    'communication_class': communication,
                    'codex_item_id': item_id, 'command': command,
                    'progress_status': item.get('status', 'in_progress'),
                    'output_bytes': output_bytes,
                    'exit_code_observed': item.get('exit_code'),
                    'call_started_at': started_wall,
                    'task_id': self.task_id, 'run_id': self.run_id,
                    'agent_turn': self.turn, 'started_agent_turn': started_turn,
                    'start_observed': active is not None,
                    'timestamp_semantics': 'CLI item.updated receipt time; progress only, not final response',
                }, 'codex_cli_derived')

        elif event_type == 'item.completed':
            active = self.active.get(item_id)
            if active is None:
                # Do not invent a start time for a call whose start event was not observed.
                active = {'kind': 'mcp' if item_type == 'mcp_tool_call' else 'command',
                          'wall': None, 'mono': None, 'record': {}, 'turn': self.turn}
            elapsed = ((time.monotonic() - active['mono']) * 1000
                       if active.get('mono') is not None else None)
            ended_wall = now()
            ended_ns = time.time_ns()
            status = item.get('status', 'unknown')
            if item.get('error') is not None:
                status = 'failed'
            elif status in ('unknown', 'in_progress', None):
                # item.completed is the authoritative terminal lifecycle event.
                status = 'completed'
            if active['kind'] == 'mcp':
                result = item.get('result')
                error = item.get('error')
                communication = ('MCP_SEARCH' if str(item.get('server')).lower() == 'exa'
                                  and item.get('tool') == 'web_search_exa' else 'MCP')
                result_bytes = json_content_bytes(result) if result is not None else 0
                record = {
                    'event_type': 'mcp_search_response' if communication == 'MCP_SEARCH' else 'mcp_tool_response',
                    'communication_class': communication,
                    'server': item.get('server'), 'tool': item.get('tool'),
                    'tool_call_id': item_id,
                    'tool_call_id_source': 'Codex ThreadItem.id; not the MCP JSON-RPC request id',
                    'arguments': item.get('arguments'),
                    'result': result,
                    'returned_content': (result.get('content') if isinstance(result, dict) else result),
                    'result_bytes': result_bytes,
                    'result_bytes_semantics': 'UTF-8 byte length of compact JSON serialization of Codex result; not MCP wire bytes',
                    'returned_content_bytes': json_content_bytes(result.get('content')) if isinstance(result, dict) and result.get('content') is not None else 0,
                    'returned_content_bytes_semantics': 'UTF-8 byte length of compact JSON serialization of result.content; not wire bytes',
                    'error': error,
                    'error_message': (error.get('message') if isinstance(error, dict) else str(error) if error is not None else None),
                    'tool_status': status,
                    'call_started_at': active.get('wall'), 'call_ended_at': ended_wall,
                    'tool_duration_ms': elapsed,
                    'tool_duration_semantics': 'monotonic interval between Codex CLI item.started and item.completed receipt; not server execution time',
                    'task_id': self.task_id, 'run_id': self.run_id,
                    'agent_turn': self.turn, 'started_agent_turn': active.get('turn'),
                    'timestamp_semantics': 'CLI item.completed receipt time; terminal response',
                }
            else:
                output = item.get('aggregated_output', '')
                output_bytes = len(output.encode('utf-8')) if isinstance(output, str) else json_content_bytes(output)
                communication = shell_command_class(item.get('command', ''))
                record = {
                    'event_type': 'web_fetch_response' if communication == 'WEB_FETCH' else 'local_tool_response',
                    'communication_class': communication,
                    'codex_item_id': item_id, 'command': item.get('command'),
                    'aggregated_output': output, 'output_bytes': output_bytes,
                    'exit_code': item.get('exit_code'), 'tool_status': status,
                    'call_started_at': active.get('wall'), 'call_ended_at': ended_wall,
                    'tool_duration_ms': elapsed,
                    'tool_duration_semantics': 'monotonic interval between Codex CLI item.started and item.completed receipt; not server execution time',
                    'task_id': self.task_id, 'run_id': self.run_id,
                    'agent_turn': self.turn, 'started_agent_turn': active.get('turn'),
                    'timestamp_semantics': 'CLI item.completed receipt time; terminal response',
                }
            self._write(record, 'codex_cli_derived')
            if event_type == 'item.completed':
                self.active.pop(item_id, None)


def run_codex(command, env, prompt, task_dir, timeout, diagnostics, observer=None,
              task_id=None, run_id=None):
    started, timed_out, interrupted = now(), False, False
    proc, buffer, failure = None, b'', None
    with (task_dir / 'codex_timeline.jsonl').open('w', buffering=1) as trace, \
         (diagnostics / 'codex_stderr.log').open('wb') as stderr:
        recorder = CodexTimelineRecorder(trace, task_id=task_id, run_id=run_id)
        selector = selectors.DefaultSelector()
        try:
            proc = subprocess.Popen(command, env=env, stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE, stderr=stderr, start_new_session=True)
            proc.stdin.write(prompt.encode())
            proc.stdin.close()
            os.set_blocking(proc.stdout.fileno(), False)
            selector.register(proc.stdout, selectors.EVENT_READ)
            deadline = time.monotonic() + timeout
            exit_seen = None
            while selector.get_map():
                if observer is not None:
                    observer.poll()
                if time.monotonic() >= deadline:
                    timed_out = True
                    recorder.errors.append('Codex execution timed out')
                    break
                for key, _ in selector.select(.2):
                    chunk = os.read(key.fd, 65536)
                    if not chunk:
                        selector.unregister(key.fileobj)
                    buffer += chunk
                    while b'\n' in buffer:
                        line, buffer = buffer.split(b'\n', 1)
                        recorder.record(line)
                        if recorder.errors:
                            break
                    if recorder.errors:
                        break
                if recorder.errors:
                    break
                if proc.poll() is not None:
                    exit_seen = exit_seen or time.monotonic()
                    if time.monotonic() - exit_seen >= 1:
                        break
            if not recorder.errors and not timed_out:
                while proc.poll() is None:
                    if observer is not None: observer.poll()
                    if time.monotonic() >= deadline:
                        timed_out = True
                        recorder.errors.append('Codex execution timed out')
                        break
                    time.sleep(.2)
        except KeyboardInterrupt as exc:
            interrupted, failure = True, exc
            recorder.errors.append('Interrupted by user (Ctrl+C)')
        except Exception as exc:
            failure = exc
            recorder.errors.append(f'{type(exc).__name__}: {exc}')
            recorder.errors.append(traceback.format_exc())
        finally:
            with finishing():
                try:
                    stop(proc)
                except Exception as exc:
                    recorder.errors.append('Process cleanup: ' + str(exc))
                    failure = failure or exc
                # Preserve bytes already received, including a final line without newline.
                if proc is not None and proc.stdout:
                    os.set_blocking(proc.stdout.fileno(), False)
                    while True:
                        try:
                            chunk = os.read(proc.stdout.fileno(), 65536)
                        except (BlockingIOError, OSError):
                            break
                        if not chunk:
                            break
                        buffer += chunk
                while b'\n' in buffer:
                    line, buffer = buffer.split(b'\n', 1)
                    recorder.record(line)
                if buffer:
                    recorder.record(buffer)
                selector.close()
                if proc is not None:
                    if proc.stdout:
                        proc.stdout.close()
                    if proc.stdin and not proc.stdin.closed:
                        proc.stdin.close()
                output = {'final_message': recorder.final_message, 'events': recorder.summary_events, 'errors': recorder.errors,
                          'exit_code': proc.returncode if proc else None,
                          'timed_out': timed_out, 'interrupted': interrupted,
                          'event_count': recorder.event_count, 'tool_call_count': recorder.tool_call_count,
                          'started_at': started, 'ended_at': now()}
                if _interrupt_requested and not interrupted:
                    interrupted = output['interrupted'] = True
                    recorder.errors.append('Interrupted by user during process cleanup')
                output['succeeded'] = (proc is not None and proc.returncode == 0
                                       and not timed_out and not interrupted and not failure
                                       and any(e.get('type') == 'turn.completed' for e in recorder.summary_events)
                                       and not recorder.errors)
                write_json(task_dir / 'codex_output.json', output)
    if failure:
        raise failure
    return output


def tshark_rows(pcap, fields, options=()):
    """Disk-backed TSV decoding: avoid materializing packet rows in RAM."""
    command=['tshark','-n','-r',str(pcap),*options,'-T','fields','-E','occurrence=f']
    for field in fields:command+=['-e',field]
    with tempfile.TemporaryFile(mode='w+t',encoding='utf-8',errors='replace',dir=pcap.parent) as stream:
        proc=subprocess.run(command,stdout=stream,stderr=subprocess.PIPE,text=True,timeout=LOCAL_ANALYSIS_TIMEOUT)
        if proc.returncode:raise RuntimeError('tshark failed: '+proc.stderr[-2000:])
        stream.seek(0)
        for line in stream:
            yield dict(zip(fields,line.rstrip('\n').split('\t')))


def traffic_report(task_dir, run_root, provider, mode, diagnostics):
    report={'requested_mode':mode,'transport_verified':False,'tls_decryption_verified':False,
            'openai_tls_decryption_verified':False,'notice':'All-interface capture can include unrelated processes; domain alone is not task attribution.'}
    pcap=task_dir/'raw_traffic.pcapng'
    if not pcap.is_file() or not pcap.stat().st_size:
        report['error']='No capture available';return report
    keys=diagnostics/'tls_keys.log'
    keys.write_bytes(b''.join(p.read_bytes()+b'\n' for p in [run_root/'proxy_tls_keys.log',diagnostics/'codex_tls_keys.log'] if p.exists()))
    fields=['frame.number','tcp.stream','ip.src','ip.dst','tcp.dstport','http.host','http.request.method','http.request.uri','http.response.code','http2.headers.authority','http2.headers.method','http2.headers.path','tls.handshake.extensions_server_name','tls.handshake.type','websocket.opcode','dns.qry.name']
    port=urlparse(provider['base_url']).port
    options=['-Y','http.request || http.response || http2.headers.method || tls.handshake || websocket || dns.qry.name','-d',f'tcp.port=={port},http']
    plain=set();hosts=set();llm=set();control=set();upgrades=set();ws=set();tls_streams=set();count=0;http_post=False;openai=False
    def is_request(row):return bool(row.get('http.request.method') or row.get('http2.headers.method'))
    for row in tshark_rows(pcap,fields,options):
        if is_request(row):plain.add(row['frame.number'])
        if row.get('tls.handshake.type') and row.get('tcp.stream'):tls_streams.add(row['tcp.stream'])
    for row in tshark_rows(pcap,fields,options+(['-o',f'tls.keylog_file:{keys}'] if keys.stat().st_size else [])):
        stream=row.get('tcp.stream');path=row.get('http.request.uri','')
        if is_request(row) and row['frame.number'] not in plain:
            count+=1
            host=(row.get('http.host') or row.get('http2.headers.authority','')).split(':')[0].lower()
            openai |= any(host==d or host.endswith('.'+d) for d in ('openai.com','chatgpt.com','oaistatic.com','oaiusercontent.com'))
        for k in ('http.host','http2.headers.authority','tls.handshake.extensions_server_name','dns.qry.name'):
            if row.get(k):hosts.add(row[k])
        if stream and row.get('ip.src')==row.get('ip.dst')=='127.0.0.1' and row.get('tcp.dstport')==str(port):
            if path.split('?')[0]=='/v1/responses':
                llm.add(stream);http_post |= row.get('http.request.method')=='POST'
            elif path.startswith('/health?measurement_marker='):control.add(stream)
        if stream and row.get('http.response.code')=='101':upgrades.add(stream)
        if stream and row.get('websocket.opcode'):ws.add(stream)
    ws_frames=bool(llm&upgrades&ws)
    report.update(tls_decryption_verified=bool(count),openai_tls_decryption_verified=openai,decrypted_request_count=count,observed_hosts=sorted(hosts),tls_handshake_tcp_stream_count=len(tls_streams),http_responses_post_seen=http_post,websocket_frames_seen=ws_frames,transport_verified=(ws_frames and not http_post) if mode=='ws' else http_post,llm_http_streams=sorted(llm-control),measurement_marker_streams=sorted(control-llm),mixed_streams=sorted(llm&control))
    return report


def tuple_key(a,b):
    try:
        def endpoint(value):
            address=ipaddress.ip_address(value[0].split('%')[0]);address=getattr(address,'ipv4_mapped',None) or address
            return str(address),int(value[1])
        return tuple(sorted((endpoint(a),endpoint(b))))
    except (ValueError,TypeError,IndexError,AttributeError):return None


def tunnel_tuple(meta):
    pid=(meta.get('ssh_tunnel_identity') or {}).get('local_pid')
    if not pid:return None
    for line in meta.get('tunnel_socket_observation','').splitlines():
        if not re.search(r'\bpid='+str(pid)+r'(?:,|\))',line):continue
        fields=line.split(); addresses=[]
        for field in fields[:6]:
            host,sep,port=field.rpartition(':')
            if sep and port.isdigit():addresses.append((host.strip('[]'),port))
        if len(addresses)==2:return tuple_key(*addresses)
    return None



def _is_loopback_address(value):
    try:
        return ipaddress.ip_address(value).is_loopback
    except (ValueError, TypeError):
        return False


def _host_service_class(hosts):
    normalized = set()
    for host in hosts:
        value = str(host).strip().lower().rstrip('.')
        if not value:
            continue
        try:
            ipaddress.ip_address(value)
            continue
        except ValueError:
            normalized.add(value)
    if 'mcp.exa.ai' in normalized:
        return 'MCP_SEARCH'
    if normalized:
        return 'WEB_FETCH'
    return None


def packet_role_for_flow(src, dst, src_port, dst_port, proxy_port=None,
                         upstream_endpoint=None, is_control=False, is_llm=False,
                         is_tunnel=False, auxiliary=None, hosts=()):
    """Return a packet role and separate service hint.

    Local inspection-proxy and upstream-proxy legs take precedence over hostname
    hints. A hostname identifies the endpoint/service only; it does not prove
    that the flow belongs to this task.
    """
    auxiliary = auxiliary or None
    src_loop = _is_loopback_address(src)
    dst_loop = _is_loopback_address(dst)
    ports = {str(src_port), str(dst_port)}
    local_proxy = bool(src_loop and dst_loop and proxy_port and str(proxy_port) in ports)
    upstream_proxy = False
    if upstream_endpoint:
        host = str(upstream_endpoint.get('host') or '')
        port = str(upstream_endpoint.get('port') or '')
        peer_matches = (src == host or dst == host) if host else False
        upstream_proxy = bool(port and port in ports and peer_matches)
    service = _host_service_class(hosts)
    if is_control:
        return 'Measurement Control: HTTP marker', service
    if local_proxy:
        return 'PROXY: local TLS inspection ingress', service
    if upstream_proxy:
        return 'PROXY: WSL-to-upstream proxy egress', service
    if is_llm:
        return 'LLM: loopback HTTP', service
    if is_tunnel:
        return 'LLM: physical SSH tunnel', service
    if auxiliary:
        return 'Auxiliary: verified plugin ' + str(auxiliary), service
    if service == 'MCP_SEARCH':
        return 'MCP_SEARCH: Exa endpoint (task attribution unverified)', service
    if service == 'WEB_FETCH':
        return 'WEB_FETCH: external HTTP(S) endpoint (task attribution unverified)', service
    return 'Unknown: remaining packets', None


def packet_role_report(pcap,traffic,result,spark_meta,keys=None):
    """Classify proxy legs before service hints; never count proxy legs as target bytes."""
    tools=result.get('tools') or {}
    proxy_port=urlparse(tools.get('proxy_url') or '').port
    upstream_endpoint=tools.get('upstream_proxy_endpoint')
    llm=set(traffic.get('llm_http_streams',[]))
    control=set(traffic.get('measurement_marker_streams',[]))
    plugin=(result.get('plugin_resources') or {}).get('downloads') or {}
    aux={str(e['tcp_stream']):e.get('leg','unknown') for e in plugin.get('downloads',[])
         if e.get('curated_metadata_url_verified') and e.get('zip_complete') is True
         and e.get('request_count_on_connection')==1}
    tunnel=tunnel_tuple(spark_meta)
    counts=Counter();sizes=Counter();proxy_seen=False;stream_roles={}
    fields=['frame.len','tcp.stream','ip.src','ip.dst','ipv6.src','ipv6.dst',
            'tcp.srcport','tcp.dstport','http.host','http2.headers.authority',
            'tls.handshake.extensions_server_name']
    options=['-o',f'tls.keylog_file:{keys}'] if keys and Path(keys).is_file() and Path(keys).stat().st_size else []
    rows=list(tshark_rows(pcap,fields,options))
    stream_hosts=Counter()
    for row in rows:
        stream=row.get('tcp.stream')
        for field_name in ('http.host','http2.headers.authority','tls.handshake.extensions_server_name'):
            host=row.get(field_name)
            if host:
                stream_hosts[stream, str(host).split(':')[0].lower().rstrip('.')] += 1
    stream_services={}
    for (stream,host),_count in stream_hosts.items():
        stream_services.setdefault(stream,set()).add(host)
    for row in rows:
        stream=row.get('tcp.stream')
        src=row.get('ip.src') or row.get('ipv6.src')
        dst=row.get('ip.dst') or row.get('ipv6.dst')
        src_port=row.get('tcp.srcport');dst_port=row.get('tcp.dstport')
        key=tuple_key((src,src_port),(dst,dst_port))
        local_proxy=bool(_is_loopback_address(src) and _is_loopback_address(dst)
                         and proxy_port and str(proxy_port) in (str(src_port),str(dst_port)))
        proxy_seen |= local_proxy
        role,service=packet_role_for_flow(
            src,dst,src_port,dst_port,proxy_port,upstream_endpoint,
            is_control=stream in control,is_llm=stream in llm,
            is_tunnel=bool(key and key==tunnel),auxiliary=aux.get(str(stream)),
            hosts=stream_services.get(stream,()))
        counts[role]+=1
        sizes[role]+=int(row.get('frame.len') or 0)
        stream_roles[stream]=role
        if service:
            stream_services.setdefault(stream,set()).add(service)
    service_rows={}
    for stream,values in stream_services.items():
        services=sorted(x for x in values if x in ('MCP_SEARCH','WEB_FETCH'))
        hosts=sorted(x for x in values if x not in ('MCP_SEARCH','WEB_FETCH'))
        if services:
            service_rows[str(stream)]={'service_classes':services,'observed_hosts':hosts,
                'network_role':stream_roles.get(stream),
                'task_attribution':'unverified; SNI/HTTP authority identifies service endpoint, not task ownership'}
    return {
        'status':'observed_endpoint_accounting',
        'roles':{k:{'packet_observations':v,'frame_bytes':sizes[k]} for k,v in sorted(counts.items())},
        'stream_service_hints':service_rows,
        'proxy_ingress_observed':proxy_seen,
        'tunnel_tuple_identified':tunnel is not None,
        'upstream_proxy_endpoint':upstream_endpoint,
        'classification_order':['measurement_control','local_proxy_ingress','upstream_proxy_egress',
                                'llm','ssh_tunnel','verified_auxiliary','exa_mcp','external_web','unknown'],
        'scope':'All-interface packet observations, not unique wire bytes. Proxy ingress and upstream proxy legs are PROXY; SNI/HTTP authority service hints are separate and do not add target-site bytes. When traffic traverses a Windows upstream proxy, the WSL capture sees the proxy TCP peer, not the proxy service onward connection. Domain/SNI does not prove task ownership.',
    }

def embed_tls_keys(pcap, keys):
    """Keep Wireshark decryption material inside the single requested PCAPNG."""
    if not pcap.exists() or not pcap.stat().st_size or not keys.exists() or not keys.stat().st_size:
        return False
    with tempfile.TemporaryDirectory(prefix='gaia-pcap-', dir=pcap.parent) as directory:
        output = Path(directory) / 'capture.pcapng'
        proc = subprocess.run(['editcap', '--inject-secrets', f'tls,{keys}', str(pcap), str(output)],
                              capture_output=True, text=True, timeout=LOCAL_ANALYSIS_TIMEOUT)
        if proc.returncode:
            raise RuntimeError('Cannot embed TLS keys: ' + proc.stderr[-2000:])
        output.chmod(0o600)
        os.replace(output, pcap)
    return True


def initialize_task(task_dir, task):
    task_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    prompt = task_prompt(task['gaia'])
    (task_dir / 'task_prompt.txt').write_text(prompt)
    (task_dir / 'codex_timeline.jsonl').write_text('')
    (task_dir / 'raw_traffic.pcapng').write_bytes(b'')
    write_json(task_dir / 'codex_output.json', {'status': 'not_started'})
    return prompt


def check_tools(args):
    """Opt-in capability probe; no inference. Optional tunnel health check contacts an existing service."""
    binary = native_codex()
    host = code_mode_host(binary, args.code_mode_host)
    report = tool_metadata(args)
    report['codex_version'] = subprocess.run([str(binary),'--version'],capture_output=True,text=True,timeout=10,check=True).stdout.strip()
    tunnel = args.check_tunnel_url
    if tunnel and not re.fullmatch(r'http://127\.0\.0\.1:[0-9]{1,5}(?:/v1)?/?',tunnel):
        raise ValueError('Only an existing 127.0.0.1 HTTP tunnel may be checked')
    with tempfile.TemporaryDirectory(prefix='gaia-probe-') as temp:
        runtime = Path(temp)
        (runtime/'home').mkdir(); (runtime/'work').mkdir()
        proxy = None
        try:
            proxy, url, ca = start_proxy(runtime, args)
            env = solver_env(runtime, {'base_url':'http://127.0.0.1:18000/v1'}, url, ca)
            script = ('python3 --version; command -v curl; command -v wget; '
                      '(getent hosts example.com || echo DNS_LOOKUP_UNAVAILABLE); '
                      'curl --fail --silent --show-error --location --http1.1 --tls-max 1.2 '
                      '--retry 5 --retry-all-errors --retry-delay 1 --max-time 30 '
                      'https://example.com/ -o /tmp/probe.html; '
                      'test -s /tmp/probe.html; wc -c /tmp/probe.html; '
                      'WGET_CA=""; [ -n "$SSL_CERT_FILE" ] && WGET_CA="--ca-certificate=$SSL_CERT_FILE"; '
                      'WGET_OK=0; for i in 1 2 3 4 5; do '
                      'wget $WGET_CA --ciphers NORMAL:-VERS-TLS1.3 --quiet --timeout=30 --tries=1 '
                      '-O /tmp/probe-wget.html https://example.com/ && { WGET_OK=1; break; }; sleep 1; done; '
                      'if [ $WGET_OK = 1 ] && [ -s /tmp/probe-wget.html ]; then wc -c /tmp/probe-wget.html; else echo WGET_FAILED; fi; '
                      'PY_OK=0; for i in 1 2 3 4 5 6 7 8 9 10; do python3 -c "import ssl,urllib.request as u; '
                      'c=ssl.create_default_context(); c.maximum_version=ssl.TLSVersion.TLSv1_2; '
                      'r=u.urlopen(\'https://example.com/\',timeout=30,context=c); '
                      'print(\'python_http\',r.status,len(r.read(1000)))" && { PY_OK=1; break; }; sleep 2; done; '
                      'test $PY_OK = 1; '
                      'if [ -n "$GAIA_AGENT_VENV" ]; then python3 -c "import pyarrow,pandas,openpyxl,docx,bs4,requests,PIL,pypdf; '
                      'print(\'agent_deps_ok\')"; fi')
            command = ['/bin/sh','-ec',script]
            if tunnel:
                command[-1] = command[-1].rstrip(' ;') + '; curl --noproxy 127.0.0.1 --fail --silent --show-error --max-time 5 '+shlex.quote(tunnel.rstrip('/').removesuffix('/v1')+'/health')
            result = subprocess.run(isolated(runtime,binary,command,host),env=env,capture_output=True,text=True,timeout=180)
            print(json.dumps({'tools':report,'probe_exit':result.returncode,'stdout':result.stdout,'stderr':result.stderr},ensure_ascii=False,indent=2))
            return result.returncode
        finally:
            stop(proxy)


def wsl_default_gateway():
    """Read the WSL NAT default gateway without changing routes or DNS."""
    executable = shutil.which('ip') or '/usr/sbin/ip'
    try:
        result = subprocess.run([executable, 'route', 'show', 'default'],
                                capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            match = re.search(r'default\s+via\s+(\S+)', result.stdout)
            if match:
                return match.group(1)
    except (OSError, subprocess.SubprocessError):
        pass
    return None


def configure_runtime_environment(args):
    """Configure process-local proxy/venv defaults; never touch global network state."""
    gateway = wsl_default_gateway()
    env_gaia = os.environ.get('GAIA_UPSTREAM_PROXY')
    env_generic = next((os.environ.get(key) for key in
                        ('HTTPS_PROXY','https_proxy','HTTP_PROXY','http_proxy')
                        if os.environ.get(key)), None)
    if args.upstream_proxy:
        upstream = args.upstream_proxy.strip()
        source = 'explicit_argument'
    elif env_gaia:
        upstream = env_gaia.strip()
        source = 'environment_GAIA_UPSTREAM_PROXY'
    elif env_generic:
        upstream = env_generic.strip()
        source = 'environment_http_proxy'
    elif gateway:
        upstream = f'http://{gateway}:{args.proxy_port}'
        source = 'auto_wsl_default_gateway'
    else:
        upstream = None
        source = 'unavailable'
    if upstream:
        if not upstream.startswith(('http://','https://')):
            upstream = 'http://' + upstream
        upstream = upstream.rstrip('/')
    venv = (Path(args.agent_venv) if args.agent_venv else
            Path(os.environ.get('GAIA_AGENT_VENV','/home/codex_qwen_GAIA/.venv'))).expanduser()
    model_bypass = ['localhost','127.0.0.1','::1','192.168.3.125']
    mcp_bypass = [EXA_MCP_HOST]
    existing = os.environ.get('NO_PROXY') or os.environ.get('no_proxy') or ''
    parts = []
    for item in existing.split(',') + model_bypass + mcp_bypass:
        item = item.strip()
        if item and item not in parts:
            parts.append(item)
    bypass = ','.join(parts)
    if upstream:
        for key in ('HTTP_PROXY','http_proxy','HTTPS_PROXY','https_proxy','ALL_PROXY','all_proxy'):
            os.environ[key] = upstream
        os.environ['GAIA_UPSTREAM_PROXY'] = upstream
    os.environ['GAIA_AGENT_VENV'] = str(venv)
    os.environ['NO_PROXY'] = os.environ['no_proxy'] = bypass
    args.upstream_proxy = upstream
    args.agent_venv = str(venv)
    args.wsl_default_gateway = gateway
    args.network_environment = {
        'upstream_proxy': safe_proxy_endpoint(upstream),
        'proxy_source': source,
        'proxy_port': args.proxy_port,
        'wsl_default_gateway': gateway,
        'no_proxy': bypass,
        'agent_venv': str(venv),
        'model_bypass': model_bypass,
        'mcp_direct_bypass': mcp_bypass,
        'model_path': f'127.0.0.1:{args.tunnel_port} -> SSH -> Spark vLLM',
        'mcp_path': 'Codex -> WSL network -> mcp.exa.ai (no mitmproxy, no Windows proxy)',
        'tls_inspection': not args.no_tls_inspection,
        'scope': 'current process and child processes only',
        'public_dns': 'upstream proxy when proxied; local getent may fail',
        'capture_notice': 'Exa MCP is direct; other external traffic uses local mitm and the WSL-to-Windows-proxy leg',
    }
    return args.network_environment


def main():
    global _interrupt_requested, ROOT, SparkSession, LOCAL_ANALYSIS_TIMEOUT
    _interrupt_requested = False
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--limit', type=int)
    parser.add_argument('--range', nargs=2, type=int, metavar=('START', 'END'))
    parser.add_argument('--mode', '--mode-', choices=['http', 'ws'], default='http')
    parser.add_argument('--provider', choices=['spark'], default='spark')
    capture = parser.add_mutually_exclusive_group()
    capture.add_argument('--capture-server', dest='capture_server', action='store_true')
    capture.add_argument('--no-capture-server', dest='capture_server', action='store_false')
    parser.set_defaults(capture_server=True)
    parser.add_argument('--experiment-profile', choices=['baseline-8k','context-16k','context-32k','context-64k'], default='context-32k')
    parser.add_argument('--metrics-interval', type=float, choices=[0,.5,1,2,5], default=1.)
    parser.add_argument('--tcp-interval', type=float, choices=[0,.5,1,2,5], default=1.)
    parser.add_argument('--no-detailed-events', action='store_false', dest='detailed_events')
    parser.add_argument('--kv-cache-memory-bytes', type=int, default=None)
    parser.add_argument('--stop-model', action='store_true', help='Explicitly stop our Spark service at exit')
    parser.add_argument('--tunnel-port', type=int, default=18000)
    parser.add_argument('--all', action='store_true', help='Explicitly select all tasks')
    parser.add_argument('--model')
    parser.add_argument('--effort', choices=['low', 'medium', 'middle', 'high', 'xhigh', 'max'], default=None)
    parser.add_argument('--dataset-root', type=Path, default=GAIA_ROOT / 'DATASET/GAIA')
    parser.add_argument('--measurement-root', type=Path, default=GAIA_ROOT)
    parser.add_argument('--year', default='2023')
    parser.add_argument('--split', choices=['validation','test'], default='validation')
    parser.add_argument('--level', type=int, nargs='+', choices=[1,2,3])
    parser.add_argument('--task')
    parser.add_argument('--check-tools', action='store_true', help='Local isolated capability probe only; no Spark or Agent')
    parser.add_argument('--check-tunnel-url', help='Optional existing loopback HTTP tunnel base, only with --check-tools; health check only')
    parser.add_argument('--results-root', type=Path, default=GAIA_ROOT / 'results_codex_gaia')
    parser.add_argument('--resume-dir', type=Path)
    parser.add_argument('--resume-unattempted', action='store_true',
                        help='On resume, keep finalized non-infrastructure failures; run pending/interrupted tasks only')
    parser.add_argument('--timeout', type=float, default=3600)
    parser.add_argument('--sudo-dumpcap', action='store_true')
    parser.add_argument('--disk-reserve-mib', type=int, default=1024)
    parser.add_argument('--remote-disk-reserve-mib', type=int, default=4096)
    parser.add_argument('--local-analysis-max-mib', type=int, default=2048, help='Defer expensive local analysis above this PCAP size, preserve raw evidence')
    parser.add_argument('--local-analysis-timeout', type=int, default=180)
    parser.add_argument('--capture-buffer-mib', type=int, default=128,
                        help='Requested libpcap kernel capture buffer (MiB); default 128')
    parser.add_argument('--capture-queue-mib', type=int, default=64,
                        help='dumpcap threaded userspace queue limit (MiB); default 64')
    parser.add_argument('--capture-spool-dir', type=Path, default=Path(tempfile.gettempdir()),
                        help='Temporary capture storage, copied after stop; default /tmp')
    parser.add_argument('--mitmdump')
    parser.add_argument('--upstream-proxy',
                        help='Explicit HTTP(S) upstream proxy; overrides GAIA_UPSTREAM_PROXY and auto gateway')
    parser.add_argument('--proxy-port', type=int, default=int(os.environ.get('GAIA_PROXY_PORT','7897')),
                        help='Auto-built upstream proxy port on the WSL default gateway (default 7897)')
    parser.add_argument('--agent-venv', type=Path,
                        help='GAIA venv exposed read-only inside bubblewrap; default /home/codex_qwen_GAIA/.venv')
    parser.add_argument('--code-mode-host', type=Path,
                        help='Codex code mode helper from the same CLI release')
    auth = parser.add_mutually_exclusive_group()
    auth.add_argument('--with-auth-json', action='store_true',
                      help='Opt in to copying host auth.json; default does NOT copy it')
    auth.add_argument('--no-auth-json', action='store_true',
                      help='Explicitly select the default: do not copy host auth.json')
    parser.add_argument('--no-tls-inspection', action='store_true')
    parser.add_argument('--list-tasks', action='store_true')
    args = parser.parse_args()
    if args.resume_unattempted and not args.resume_dir:
        parser.error('--resume-unattempted requires --resume-dir')
    if not args.list_tasks and not args.check_tools and not args.all and not args.range and args.limit is None and not args.task:
        parser.error('Specify --task, --range, --limit, or --all')
    if sum(bool(x) for x in (args.all, args.range, args.limit is not None, args.task)) > 1:
        parser.error('--all cannot be combined with task subsets')
    if (args.capture_server or args.stop_model) and args.provider != 'spark':
        parser.error('--capture-server/--stop-model require --provider spark')
    args.model = args.model or ('Qwen3.6-35B-A3B' if args.provider == 'spark' else 'gpt-6.1-sol')
    args.effort = args.effort or ('low' if args.provider == 'spark' else 'medium')
    if args.provider == 'spark':
        if args.mode != 'http' or args.with_auth_json or args.model != 'Qwen3.6-35B-A3B':
            parser.error('Spark uses verified HTTP Qwen model, isolated unauthenticated local provider')
        if args.experiment_profile != "context-32k" or args.kv_cache_memory_bytes is not None or not args.detailed_events:
            parser.error("GAIA requires the existing context-32k detailed-events deployment")
    if args.limit is not None and args.limit < 1:
        parser.error('--limit must be positive')
    if args.range and (args.limit is not None or not 1 <= args.range[0] <= args.range[1]):
        parser.error('--range requires 1 <= START <= END and cannot be combined with --limit')
    if not 0 < args.timeout <= 86220:
        parser.error('Timeouts must be positive')
    if args.capture_buffer_mib < 1 or args.capture_queue_mib < 1 or args.disk_reserve_mib < 256:
        parser.error('Capture buffer and queue sizes must be positive')
    args.capture_spool_dir = args.capture_spool_dir.resolve()
    copy_auth_json = args.with_auth_json
    if args.effort == 'middle':
        args.effort = 'medium'
    if not re.fullmatch(r'[0-9]{4}', args.year):
        parser.error('--year must be four digits')
    if not 1 <= args.local_analysis_timeout <= 3600 or args.local_analysis_max_mib < 1 or args.remote_disk_reserve_mib < 1024:
        parser.error('Invalid analysis timeout/limit or remote disk reserve')
    LOCAL_ANALYSIS_TIMEOUT = args.local_analysis_timeout
    if not 1 <= args.proxy_port <= 65535:
        parser.error('--proxy-port must be between 1 and 65535')
    try:
        configure_runtime_environment(args)
    except (OSError, ValueError) as exc:
        parser.error('Cannot configure runtime proxy/venv defaults: '+str(exc))
    if args.check_tunnel_url and not args.check_tools:
        parser.error('--check-tunnel-url requires --check-tools')
    if args.check_tools:
        try: return check_tools(args)
        except (OSError,ValueError,RuntimeError,subprocess.SubprocessError) as exc:
            print('Tool check failed: '+str(exc),file=sys.stderr,flush=True); return 1
    try:
        args.dataset_root = args.dataset_root.resolve(strict=True)
        selected = select_tasks(load_tasks(args.dataset_root, args.year, args.split), args)
        tasks = [{'task_id': t.task_id, 'gaia': t} for t in selected]
    except (OSError, ValueError, RuntimeError) as exc:
        parser.error(str(exc))
    if args.list_tasks:
        for task in selected:
            print(f'{task.task_id} level={task.level} attachment={task.attachment.suffix if task.attachment else "none"}')
        return 0
    try:
        ROOT = args.measurement_root.resolve(strict=True)
        SparkSession = existing_model_session(load_measurement(ROOT))
        args.tool_config = tool_metadata(args)
        args.dataset_metadata_sha256 = sha256_file(args.dataset_root/args.year/args.split/'metadata.parquet')
    except (OSError,ValueError,RuntimeError,ImportError) as exc:
        parser.error(str(exc))
    run_root = (args.resume_dir.resolve() if args.resume_dir else
                args.results_root.resolve() / datetime.now().strftime('%Y%m%d-%H%M%S-%f'))
    check_paths(args.dataset_root, run_root, ROOT)
    if args.resume_dir:
        if not run_root.is_dir():
            parser.error('--resume-dir must be an existing experiment directory')
    else:
        run_root.mkdir(parents=True, mode=0o700)
    root_fd = os.open(run_root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        try:
            fcntl.flock(root_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(f'Another batch owns {run_root}', file=sys.stderr, flush=True)
            return 1
        try:
            return run_batch(args, tasks, run_root, copy_auth_json, parser)
        except KeyboardInterrupt:
            return 130
    finally:
        os.close(root_fd)


def run_batch(args, tasks, run_root, copy_auth_json, parser):
    previous = None
    if args.resume_dir:
        old_metadata = run_root / 'experiment.json'
        if old_metadata.exists():
            previous = json.loads(old_metadata.read_text())
        else:
            saved_results = sorted(run_root.glob('*/result.json'))
            if not saved_results:
                parser.error('No result.json found in --resume-dir')
            for saved_path in saved_results:
                try:
                    candidate = json.loads(saved_path.read_text()).get('experiment')
                except (OSError, ValueError, AttributeError):
                    continue
                if isinstance(candidate, dict):
                    previous = candidate
                    break
            if previous is None:
                parser.error('No readable experiment metadata in --resume-dir')
        expected = {'dataset':'GAIA', 'year':args.year, 'split':args.split, 'tools':args.tool_config, 'measurement_root':str(ROOT), 'dataset_root':str(args.dataset_root), 'disk_reserve_mib':args.disk_reserve_mib, 'remote_disk_reserve_mib':args.remote_disk_reserve_mib, 'local_analysis_max_mib':args.local_analysis_max_mib, 'local_analysis_timeout':args.local_analysis_timeout, 'dataset_metadata_sha256':args.dataset_metadata_sha256, 'task_ids': [t['task_id'] for t in tasks], 'model': args.model,
                    'effort': args.effort, 'mode': args.mode, 'tls_inspection': not args.no_tls_inspection,
                    'copy_auth_json': copy_auth_json}
        expected['capture_settings'] = {'buffer_mib': args.capture_buffer_mib,
                                        'queue_mib': args.capture_queue_mib,
                                        'spool_directory': str(args.capture_spool_dir),
                                        'interface': 'any', 'filter': None}
        if args.provider == 'spark':
            expected.update(provider='spark', capture_server=args.capture_server,
                            experiment_profile=args.experiment_profile,
                            metrics_interval=args.metrics_interval,tcp_interval=args.tcp_interval,
                            detailed_events=args.detailed_events,kv_cache_memory_bytes=args.kv_cache_memory_bytes)
        if 'timeout' in previous and previous['timeout'] != args.timeout:
            parser.error('Resume timeout does not match experiment.json')
        if any(previous.get(k) != v for k, v in expected.items()):
            parser.error('Resume selection/model/effort/mode/TLS settings do not match experiment.json')
    print(f'Experiment dir: {run_root}', flush=True)
    experiment = {'dataset':'GAIA', 'year':args.year, 'split':args.split, 'tools':args.tool_config, 'measurement_root':str(ROOT), 'dataset_root':str(args.dataset_root), 'disk_reserve_mib':args.disk_reserve_mib, 'remote_disk_reserve_mib':args.remote_disk_reserve_mib, 'local_analysis_max_mib':args.local_analysis_max_mib, 'local_analysis_timeout':args.local_analysis_timeout, 'dataset_metadata_sha256':args.dataset_metadata_sha256, 'created_at': now(), 'task_ids': [t['task_id'] for t in tasks],
                  'model': args.model, 'effort': args.effort, 'mode': args.mode,
                  'status': 'initializing', 'provider': args.provider, 'timeout': args.timeout,
                   'tls_inspection': not args.no_tls_inspection,
                  'timeline_notice': 'CLI event receipt times, not wire-level request timestamps',
                  'isolation': 'bubblewrap; dataset, references and previous results are not mounted',
                  'auxiliary_traffic': 'No telemetry-disable settings are added; all interfaces captured',
                   'copy_auth_json': copy_auth_json}
    experiment['capture_server'] = args.capture_server
    experiment['mcp_configuration'] = args.tool_config['mcp']
    if args.provider=='spark':
        from scripts.experiment_config import ExperimentConfig
        experiment['experiment_profile']=args.experiment_profile
        experiment['requested_context']=ExperimentConfig.build(args.experiment_profile,args.metrics_interval,args.detailed_events,args.tcp_interval,args.kv_cache_memory_bytes).as_dict()
        experiment.update(metrics_interval=args.metrics_interval,tcp_interval=args.tcp_interval,
                          detailed_events=args.detailed_events,kv_cache_memory_bytes=args.kv_cache_memory_bytes)
    experiment['run_id'] = 'gaia-' + hashlib.sha256(str(run_root).encode()).hexdigest()[:24] + '-' + str(time.time_ns())
    experiment['capture_settings'] = {'buffer_mib': args.capture_buffer_mib,
                                      'queue_mib': args.capture_queue_mib,
                                      'spool_directory': str(args.capture_spool_dir),
                                      'interface': 'any', 'filter': None}
    experiment['network_environment'] = args.network_environment
    if previous:
        experiment['created_at'] = previous.get('created_at', experiment['created_at'])
        experiment['resumed_at'] = now()
    completed = {}
    experiment['progress'] = {t['task_id']: {'state': 'pending', 'scheduled': True} for t in tasks}
    if args.resume_dir:
        for task in tasks:
            directory = run_root / task_directory(args.split, task['task_id'])
            if directory.is_symlink(): parser.error('Refusing symlink task directory: '+str(directory))
            try:
                saved = json.loads((directory / 'result.json').read_text())
            except (OSError, ValueError):
                saved = None
            if isinstance(saved, dict) and saved.get('task_id') != task['task_id']:
                saved = None
            state = resume_outcome(saved, directory) if directory.exists() else 'pending'
            entry = experiment['progress'][task['task_id']]
            entry['state'] = state
            if isinstance(saved, dict):
                for key in ('agent_status', 'quality_passed', 'cleanup_status', 'error',
                            'infrastructure_errors', 'quality_issues'):
                    if key in saved:
                        entry[key] = saved[key]
                # Old records may lack the independent status fields.
                if state in ('completed', 'failed'):
                    entry.setdefault('cleanup_status', 'confirmed')
                    if state == 'completed':
                        entry.setdefault('agent_status', 'passed')
                        entry.setdefault('quality_passed', True)
                    else:
                        legacy_measurement = saved.get('measurement') or {}
                        if (legacy_measurement.get('server_record_complete') is False
                                or saved.get('capture', {}).get('zero_loss_verified') is False
                                or legacy_measurement.get('quality_passed') is False):
                            entry.setdefault('quality_passed', False)
                        if saved.get('codex_timed_out'):
                            entry.setdefault('agent_status', 'timed_out')
                        elif saved.get('verify_passed') is False:
                            entry.setdefault('agent_status', 'verification_failed')
                        elif saved.get('verify_passed') is True:
                            entry.setdefault('agent_status', 'passed')
                entry.setdefault('agent_status', 'legacy_unknown')
            if state == 'completed' or (args.resume_unattempted and state == 'failed'):
                completed[task['task_id']] = saved
                entry['scheduled'] = False
            history = run_root / '.attempt-history' / directory.name
            if history.is_dir():
                entry['prior_attempts'] = [str(x.relative_to(run_root)) for x in sorted(history.iterdir()) if x.is_dir()]
        if len(completed) == len(tasks):
            for task in tasks:
                print(f"SKIP {experiment['progress'][task['task_id']]['state']}: {task['task_id']}", flush=True)
            experiment['status'] = 'completed_with_failures' if any(e['state'] == 'failed' for e in experiment['progress'].values()) else 'completed'
            experiment['ended_at'] = now()
            batch_checkpoint(run_root, experiment)
            return 0
    batch_checkpoint(run_root, experiment)
    scratch = tempfile.TemporaryDirectory(prefix='gaia-diagnostics-')
    scratch_root = Path(scratch.name)
    proxy_proc, rows = None, list(completed.values())
    spark = None
    gateway = None
    try:
        for cmd in ['bwrap', 'dumpcap', 'tshark', 'editcap']:
            if not shutil.which(cmd):
                raise RuntimeError(f'Required command missing: {cmd}')
        binary = native_codex()
        host_binary = code_mode_host(binary, args.code_mode_host)
        if args.provider == 'spark':
            spark = SparkSession(args, experiment['run_id'], scratch_root)
            experiment['spark'] = spark.meta
            spark.prepare()
            # vLLM 0.24 passes Responses namespace tools to Qwen as a function
            # named only after the namespace (mcp__exa), while Codex routes on
            # separate namespace/name fields.  Keep the model tunnel on
            # args.tunnel_port and put a loopback-only flattener in front of it.
            gateway = ToolNameGateway(args.tunnel_port + 1, args.tunnel_port)
            gateway.start()
            config_path = spark.config_home / 'config.toml'
            config_text = config_path.read_text()
            tunnel_url = f'http://127.0.0.1:{args.tunnel_port}/v1'
            gateway_url = f'http://127.0.0.1:{gateway.listen_port}/v1'
            if tunnel_url not in config_text:
                raise RuntimeError('Spark provider URL not found in temporary Codex config')
            config_path.write_text(config_text.replace(tunnel_url, gateway_url, 1))
            experiment['spark']['tool_gateway_local_port'] = gateway.listen_port
            experiment['network_environment']['model_path'] = (
                f'127.0.0.1:{gateway.listen_port} (MCP tool-name gateway) -> '
                f'127.0.0.1:{args.tunnel_port} -> SSH -> Spark vLLM')
            experiment['network_environment']['tool_gateway_port'] = gateway.listen_port
            experiment['network_environment']['model_tunnel_port'] = args.tunnel_port
            experiment['network_environment']['tool_gateway'] = (
                'flatten Responses namespace tools to mcp__server__tool; '
                'split function_call back to namespace/name')
            experiment['tool_name_gateway'] = {
                'listen_port': gateway.listen_port,
                'upstream_port': args.tunnel_port,
                'scope': 'loopback only; no model or vLLM changes',
            }
        config_home = spark.config_home if spark else Path(os.environ.get('CODEX_HOME', str(Path.home() / '.codex')))
        experiment['plugin_resources'] = plugin_resource_findings(config_home)
        config_text = (config_home / 'config.toml').read_text()
        config = tomllib.loads(config_text)
        provider_name = config.get('model_provider', 'openai')
        provider = config.get('model_providers', {}).get(provider_name, {})
        if provider_name!='spark' or provider.get('env_key') or provider.get('env_http_headers') or provider.get('http_headers') or provider.get('requires_openai_auth') or config.get('mcp_servers'):
            raise RuntimeError('Unexpected credential-bearing provider or MCP configuration; refusing to expose it')
        base_url=urlparse(provider.get('base_url',''))
        if base_url.scheme!='http' or base_url.hostname!='127.0.0.1' or not base_url.port or base_url.path!='/v1':
            raise RuntimeError('Spark provider is not the expected loopback HTTP tunnel')
        catalog=json.loads((config_home/'model_catalog.json').read_text())
        model_entry=next((m for m in catalog.get('models',[]) if m.get('slug')==args.model),None)
        if not model_entry or model_entry.get('input_modalities')!=['text']:
            raise RuntimeError('Shared catalog does not match the expected text-only model')
        version = subprocess.run([str(binary), '--version'], capture_output=True, text=True, timeout=15, check=True).stdout.strip()
        if not re.search(r'\b0\.169\.0\b',version):
            print('[WARNING] Actual Codex version differs from 0.169.0: '+version,flush=True)
        experiment['model_catalog_capabilities']={'input_modalities':model_entry['input_modalities'],'supports_search_tool':model_entry.get('supports_search_tool')}
        experiment.update(codex_version=version, code_mode_host=str(host_binary), provider=provider_name,
                          base_url=provider.get('base_url'), status='running')
        binary_hash = sha256_file(binary)
        plugin_cache = args.results_root.resolve() / '.plugin_resources' / binary_hash
        experiment['plugin_resources'].update(cache_directory=str(plugin_cache),
                                              binary_sha256=binary_hash,
                                              cache_available=(plugin_cache / 'current').is_file())
        proxy, ca = None, None
        batch_checkpoint(run_root, experiment)
        for index, task in enumerate(tasks, 1):
            if _interrupt_requested:
                raise KeyboardInterrupt
            task_dir = run_root / task_directory(args.split, task['task_id'])
            if task['task_id'] in completed:
                print(f"SKIP {experiment['progress'][task['task_id']]['state']}: {task['task_id']}", flush=True)
                continue
            if task_dir.exists():
                archived = archive_attempt(run_root, task_dir)
                experiment['progress'][task['task_id']].setdefault('prior_attempts', []).append(archived)
                print(f'PRESERVE prior attempt: {archived}', flush=True)
            task_dir.mkdir(mode=0o700, exist_ok=False)
            print(f"[{index}/{len(tasks)}] {task['task_id']} mode={args.mode}", flush=True)
            result = {'dataset':'GAIA','year':args.year,'split':args.split,'level':task['gaia'].level,
                      'task_id': task['task_id'], 'server_task_id': server_task_id(args.split, task['task_id']),
                      'attachment_metadata': None, 'predicted_answer':None,
                      'scoring_available':task['gaia'].expected_answer is not None, 'started_at': now(), 'setup_ok': False,
                      'verify_passed': None, 'error': None, 'mode': args.mode,
                      'status': 'running', 'interruption_reason': None,
                      'infrastructure_errors': [], 'cleanup_status': 'unconfirmed',
                      'quality_issues': []}
            rows.append(result)
            # A crash before finalization is distinguishable from an unattempted task.
            result['experiment'] = {k: v for k, v in experiment.items() if k != 'progress'}
            write_json(task_dir / 'result.json', result)
            batch_checkpoint(run_root, experiment, task['task_id'], 'running')
            prompt = initialize_task(task_dir, task)
            diagnostics_temp = tempfile.TemporaryDirectory(prefix='gaia-task-diagnostics-')
            diagnostics = Path(diagnostics_temp.name)
            capture = None
            task_measurement = None
            capture_command = None
            spool_temp = None
            local_cleanup_ok = True
            copy_error = None
            output = {}
            capture_spool = None
            capture_exit_code = None
            native_plugins = {'error': 'Codex did not start'}
            plugin_cache_state = {'hit': False, 'status': 'not_started'}
            try:
                with task_runtime(diagnostics, lambda: stop_task_processes(proxy_proc, capture)) as runtime:
                    (runtime / 'home/.codex').mkdir(parents=True)
                    (runtime / 'work').mkdir()
                    stage_task(task['gaia'], runtime / 'work')
                    result['attachment_metadata'] = attachment_metadata(task['gaia'], runtime / 'work')
                    # Retain user routing, telemetry and other harness configuration.
                    # Host files referenced by that configuration remain outside the mount namespace.
                    (runtime / 'home/.codex/config.toml').write_text(gaia_codex_config_text(config_text))
                    if spark:
                        shutil.copyfile(spark.config_home / 'model_catalog.json', runtime / 'home/.codex/model_catalog.json')
                    plugin_cache_state = restore_plugin_resources(
                        plugin_cache, runtime / 'home/.codex', binary_hash)
                    if copy_auth_json and (config_home / 'auth.json').exists():
                        shutil.copyfile(config_home / 'auth.json', runtime / 'home/.codex/auth.json')
                    proxy_proc, proxy, ca = start_proxy(diagnostics, args)
                    env = solver_env(runtime, provider, proxy, ca)
                    result['tools'] = {**args.tool_config, 'proxy_url':proxy, 'model_url':provider.get('base_url'),
                                        'upstream_proxy_endpoint': (safe_proxy_endpoint(args.upstream_proxy)
                                            if args.upstream_proxy else
                                            {'scheme':'http','host':args.wsl_default_gateway,'port':args.proxy_port}
                                            if args.wsl_default_gateway else None)}
                    probe = subprocess.run(isolated(runtime, binary, ['/usr/bin/python3', '-c',
                        probe_code(args.dataset_root, ROOT, run_root, task['gaia'],provider['base_url'])], host_binary),
                        env=env, capture_output=True, text=True, timeout=15)
                    if probe.returncode:
                        raise RuntimeError('Filesystem isolation or tunnel probe failed: ' + probe.stderr.strip())
                    result['tools']['sandbox_probe'] = json.loads(probe.stdout)
                    if result['tools']['sandbox_probe'].get('isolation')!='confirmed' or result['tools']['sandbox_probe'].get('tunnel_health')!=200:
                        raise RuntimeError('Filesystem isolation or tunnel probe not confirmed')
                    free_bytes = min(shutil.disk_usage(args.capture_spool_dir).free,
                                     shutil.disk_usage(task_dir).free)
                    if free_bytes < args.disk_reserve_mib * 1024 * 1024:
                        raise RuntimeError('Capture disk reserve insufficient; refusing incomplete capture')
                    # Keep the spool recoverable if copying to the result directory fails.
                    spool_temp = Path(tempfile.mkdtemp(prefix='gaia-capture-', dir=args.capture_spool_dir))
                    capture_spool = spool_temp / 'capture.pcapng'
                    result['capture_spool_path'] = str(capture_spool)
                    write_json(task_dir / 'result.json', result)
                    if spark and args.capture_server:
                        from spark_measurement import REMOTE
                        # Read-only disk check outside both capture windows.
                        remote_dir=REMOTE+'/logs/agent-capture'
                        disk_command='df -Pk -- "$(if [ -d '+shlex.quote(remote_dir)+' ]; then printf %s '+shlex.quote(remote_dir)+'; else printf %s '+shlex.quote(REMOTE)+'; fi)"'+" | tail -1 | awk '{print $4}'"
                        remote_free=int(spark.raw(disk_command,timeout=20).strip())*1024
                        result['remote_disk_free_before_capture'] = remote_free
                        if remote_free < args.remote_disk_reserve_mib*1024**2:
                            raise RuntimeError('Spark capture filesystem disk reserve insufficient')
                        task_measurement = spark.task(result['server_task_id'], task_dir)
                        result['measurement'] = task_measurement.meta
                        task_measurement.start()
                    capture_command = (['sudo', '-n'] if args.sudo_dumpcap else []) + [
                        'dumpcap', '-q', '-i', 'any', '-B', str(args.capture_buffer_mib),
                        '-t', '-C', str(args.capture_queue_mib * 1024 * 1024),
                        '-N', '262144', '-w', str(capture_spool)]
                    with (diagnostics / 'capture.log').open('wb') as log:
                        capture = subprocess.Popen(capture_command,
                            stdout=log, stderr=log, start_new_session=True)
                    time.sleep(.5)
                    if capture.poll() is not None:
                        raise RuntimeError('Packet capture failed; see diagnostics in result.json')
                    if task_measurement:
                        task_measurement.local_ready(capture, capture_spool, diagnostics / 'capture.log')
                    result['setup_ok'] = True
                    cmd = ['/opt/bin/codex', '-c', 'approval_policy="never"',
                            '-c', 'features.code_mode_host=true', 'exec',
                           '--json', '--ephemeral', '--skip-git-repo-check', '--color', 'never',
                           '--dangerously-bypass-approvals-and-sandbox', '--model', args.model,
                           '-c', f'model_reasoning_effort="{args.effort}"',
                           '-c', f'model_providers.{provider_name}.supports_websockets={str(args.mode == "ws").lower()}', '-']
                    if task_measurement:
                        task_measurement.agent_begin()
                    output = run_codex(isolated(runtime, binary, cmd, host_binary), env, prompt, task_dir,
                                          args.timeout, diagnostics,
                                          CaptureObserver(task_measurement,capture,capture_spool,task_dir,args.disk_reserve_mib,runtime),
                                          task_id=task['task_id'], run_id=experiment['run_id'])
                    if task_measurement:
                        task_measurement.agent_end()
                    stop(proxy_proc)
                    proxy_proc = None
                    if task_measurement:
                        task_measurement.before_local_stop()
                    result.update(codex_exit_code=output['exit_code'], codex_timed_out=output['timed_out'],
                                  codex_event_count=output['event_count'], codex_tool_call_count=output['tool_call_count'],
                                  codex_errors=output['errors'])
                    time.sleep(.5)
                    stop(capture)
                    capture_exit_code = capture.returncode
                    if task_measurement:
                        task_measurement.local_stopped()
                    capture = None
                    keyfile = runtime / 'home/codex_tls_keys.log'
                    if keyfile.exists():
                        shutil.copyfile(keyfile, diagnostics / 'codex_tls_keys.log')
                    native_plugins = observe_native_plugin_snapshot(runtime / 'home/.codex')
                    if not output['succeeded']:
                        result['error'] = 'Codex failed or timed out: ' + ('; '.join(output['errors'][:2])
                                           or 'see codex_output.json and diagnostics in result.json')
                    else:
                        verification = score_result(output, task['gaia'])
                        result['verification'] = verification
                        result['verify_passed'] = verification['passed']
                        result['predicted_answer'] = verification['predicted_answer']
            except KeyboardInterrupt:
                result.update(status='interrupted', error='Interrupted by user (Ctrl+C)',
                              interruption_reason='user_interrupt')
            except Exception as exc:
                # Nonzero CLI exits/timeouts are returned normally by run_codex.
                # An exception here instead means setup, observer or cleanup failed.
                infrastructure_error(result, f'{type(exc).__name__}: {exc}', exc)
            finally:
                with finishing():
                    try:
                        stop_task_processes(proxy_proc,None)
                        proxy_proc = None
                    except Exception as exc:
                        local_cleanup_ok = False
                        infrastructure_error(result, "Proxy cleanup failed: " + str(exc), exc)
                    try:
                        stop(capture)
                    except Exception as exc:
                        local_cleanup_ok = False
                        infrastructure_error(result, 'Capture cleanup failed: ' + str(exc), exc)
                    if capture is not None:
                        capture_exit_code = capture.returncode
                        if task_measurement:
                            try:
                                task_measurement.agent_end()
                                if local_cleanup_ok:
                                    task_measurement.local_stopped()
                            except Exception as exc:
                                infrastructure_error(result, 'Capture lifecycle bookkeeping: ' + str(exc), exc)
                    try:
                        if capture_spool is not None and capture_spool.exists():
                            shutil.copyfile(capture_spool, task_dir / 'raw_traffic.pcapng')
                    except OSError as exc:
                        copy_error = f'Cannot retain capture spool: {exc}'
                        infrastructure_error(result, copy_error, exc)
                        result['retained_capture_spool'] = str(capture_spool)
                    log_path = diagnostics / 'capture.log'
                    try:
                        capture_log = log_path.read_text(errors='replace') if log_path.exists() else ''
                        capture_info = capture_statistics(capture_log, capture_spool, args, capture_exit_code)
                    except Exception as exc:
                        capture_info = {'zero_loss_verified': False, 'status': 'unverified',
                                        'warnings': ['Capture statistics failed: ' + str(exc)]}
                        result.setdefault('tracebacks', []).append(traceback.format_exc())
                    capture_info['command'] = capture_command
                    if copy_error:
                        capture_info['zero_loss_verified'] = False
                        capture_info['status'] = 'unverified'
                        capture_info['warnings'].append(copy_error)
                    result['capture'] = capture_info
                    if task_measurement:
                        try:
                            result['measurement'] = task_measurement.collect()
                        except Exception as exc:
                            result['measurement'] = task_measurement.meta
                            result['measurement']['server_record_complete'] = False
                            result['measurement'].setdefault('errors', []).append(repr(exc))
                            result.setdefault('tracebacks', []).append(traceback.format_exc())
                        recover_server_raw(spark, task_measurement, task_dir, result)
                        if not result['measurement'].get('server_record_complete') or not result['measurement'].get('coverage', {}).get('local_covers_agent'):
                            result['error'] = result['error'] or 'Dual capture incomplete: ' + str(result['measurement'].get('errors', []))
                    remote_cleanup_ok = server_cleanup_check(spark, task_measurement, result)
                    result['cleanup_status'] = ('confirmed' if local_cleanup_ok and remote_cleanup_ok else 'unconfirmed')
                    for warning in capture_info['warnings']:
                        print('[CAPTURE WARNING] ' + warning, file=sys.stderr, flush=True)
                    if not capture_info['zero_loss_verified'] and not result['error']:
                        result['error'] = 'Capture quality requirement not met: ' + '; '.join(capture_info['warnings'])
                    try:
                        if (task_dir/'raw_traffic.pcapng').stat().st_size > args.local_analysis_max_mib*1024**2:
                            raise RuntimeError('Local analysis deferred: raw PCAP exceeds configured analysis size cap')
                        traffic = traffic_report(task_dir, diagnostics, provider, args.mode, diagnostics)
                        try:
                            downloads, archives, revisions = plugin_download_evidence(
                                task_dir / 'raw_traffic.pcapng', diagnostics / 'tls_keys.log', diagnostics)
                            publication = publish_plugin_resources(
                                plugin_cache, binary_hash, native_plugins, archives, revisions)
                            # Warm runs with no download retain the immutable cache unchanged.
                            result['plugin_resources'] = {'restore': plugin_cache_state,
                                                          'downloads': downloads,
                                                          'publication': publication}
                            if not plugin_cache_state['hit'] and not publication['published']:
                                print('[PLUGIN CACHE WARNING] No reusable snapshot published: '
                                      + publication['reason'], file=sys.stderr, flush=True)
                            if plugin_cache_state['hit'] and downloads['downloads']:
                                print('[PLUGIN CACHE WARNING] Plugin ZIP requested despite cache hit; '
                                      'download reduction is not established.', file=sys.stderr, flush=True)
                        except Exception as exc:
                            result['plugin_resources'] = {'restore': plugin_cache_state,
                                                          'validation_error': str(exc)}
                            print('[PLUGIN CACHE WARNING] ' + str(exc), file=sys.stderr, flush=True)
                    except Exception as exc:
                        result.setdefault('tracebacks', []).append(traceback.format_exc())
                        if isinstance(exc, OSError) and exc.errno in (errno.ENOSPC, errno.EDQUOT):
                            infrastructure_error(result, 'Disk space exhausted during traffic analysis', exc)
                        traffic = {'error': str(exc), 'transport_verified': False,
                                   'tls_decryption_verified': False, 'openai_tls_decryption_verified': False}
                    keys = diagnostics / 'tls_keys.log'
                    try:
                        keys.write_bytes(b''.join(p.read_bytes() + b'\n' for p in
                            [diagnostics / 'proxy_tls_keys.log', diagnostics / 'codex_tls_keys.log'] if p.exists()))
                        if keys.stat().st_size and (task_dir/'raw_traffic.pcapng').stat().st_size > args.local_analysis_max_mib*1024**2:
                            raise RuntimeError('TLS embedding deferred above local analysis size cap; task secrets retained in result.json')
                        traffic['tls_keys_embedded'] = False if args.no_tls_inspection else embed_tls_keys(task_dir / 'raw_traffic.pcapng', keys)
                    except Exception as exc:
                        result.setdefault('tracebacks', []).append(traceback.format_exc())
                        if isinstance(exc, OSError) and exc.errno in (errno.ENOSPC, errno.EDQUOT):
                            infrastructure_error(result, 'Disk space exhausted during TLS key embedding', exc)
                        traffic['tls_keys_embedded'] = False
                        result['error'] = result['error'] or 'TLS key embedding failed: ' + str(exc)
                    if not traffic.get('transport_verified'):
                        result['error'] = result['error'] or 'Inference transport could not be verified'
                    # TLS usability is evaluated after observing the actual proxy leg; raw encrypted traffic is always retained.
                    try:
                        partial = json.loads((task_dir / 'codex_output.json').read_text())
                        if not isinstance(partial, dict):
                            raise ValueError('Codex output is not a JSON object')
                        output = partial
                        for source, target in [('exit_code', 'codex_exit_code'), ('timed_out', 'codex_timed_out'),
                                               ('event_count', 'codex_event_count'), ('tool_call_count', 'codex_tool_call_count'),
                                               ('errors', 'codex_errors')]:
                            if source in partial:
                                result[target] = partial[source]
                    except (OSError, ValueError) as exc:
                        result.setdefault('quality_issues', []).append('codex_output_unreadable')
                        result.setdefault('tracebacks', []).append(traceback.format_exc())
                    if _interrupt_requested:
                        result.update(status='interrupted', error=result['error'] or 'Interrupted by user during cleanup',
                                      interruption_reason='user_interrupt')
                    traffic['attribution'] = {
                        'LLM':'configured loopback HTTP port and physical SSH tunnel; Codex turn events are CLI lifecycle observations',
                        'MCP_SEARCH':'Codex MCP item lifecycle for exa/web_search_exa plus observed mcp.exa.ai endpoint hints',
                        'WEB_FETCH':'Codex curl/wget/Python HTTP command lifecycle plus observed external HTTP(S) endpoint hints',
                        'LOCAL_TOOL':'other Codex command_execution events; command execution is not packet traffic',
                        'PROXY':'local mitmproxy ingress and WSL-to-upstream proxy egress; classify before host hints',
                        'Auxiliary':'verified plugin evidence only',
                        'Measurement Control':'HTTP markers; physical SSH control not uniquely identified remains Unknown',
                        'Unknown':'other packets; SNI/hostname does not prove task ownership',
                        'proxy_legs':'count proxy ingress/egress separately; service hints do not add target-site bytes'}
                    result.update(ended_at=now(), traffic=traffic)
                    result['verification'] = score_result(output, task['gaia'])
                    result['predicted_answer'] = result['verification']['predicted_answer']
                    result['verify_passed'] = result['verification']['passed']
                    result['tls_inspection_enabled'] = not args.no_tls_inspection
                    try:
                        if traffic.get('error'): raise RuntimeError('Local decoding unavailable')
                        traffic['role_accounting'] = packet_role_report(task_dir/'raw_traffic.pcapng',traffic,result,spark.meta if spark else {},diagnostics/'tls_keys.log')
                    except Exception as exc:
                        traffic['role_accounting'] = {'status':'unavailable','error':str(exc)}
                        result['quality_issues'].append('local_role_accounting_unavailable')
                        if isinstance(exc,OSError) and exc.errno in (errno.ENOSPC,errno.EDQUOT):
                            infrastructure_error(result,'Disk space exhausted in local role accounting',exc)
                    if args.no_tls_inspection:
                        traffic['tls_inspection_status']='disabled'
                    elif traffic.get('tls_keys_embedded'):
                        traffic['tls_inspection_status']='keys_embedded_partial_or_unknown' if traffic.get('tls_decryption_verified') else 'keys_embedded_decryption_not_verified'
                    elif traffic['role_accounting'].get('proxy_ingress_observed') is False:
                        traffic['tls_inspection_status']='not_applicable' if traffic.get('tls_handshake_tcp_stream_count')==0 else 'proxy_not_observed'
                    else:
                        traffic['tls_inspection_status']='unverified'
                    traffic['tls_scope_notice']='Direct encrypted traffic retained; embedded secrets do not prove all connections decryptable.'
                    try:
                        result['diagnostics'] = {p.name: p.read_text(errors='replace') for p in
                                                 diagnostics.glob('*.log') if p.name not in ('tls_keys.log', 'codex_tls_keys.log','proxy_tls_keys.log')}
                        # Preserve recoverable keys even if editcap unexpectedly fails.
                        if not traffic.get('tls_keys_embedded') and (diagnostics / 'tls_keys.log').exists() and (diagnostics/'tls_keys.log').stat().st_size:
                            result['diagnostics']['tls_keys_fallback'] = (diagnostics / 'tls_keys.log').read_text()
                    except Exception as exc:
                        infrastructure_error(result, 'Cannot retain task diagnostics: ' + str(exc), exc)
                    # Do not delete recoverable raw data if copying or stopping failed.
                    if spool_temp is not None and not copy_error and local_cleanup_ok:
                        try:
                            shutil.rmtree(spool_temp)
                        except Exception as exc:
                            infrastructure_error(result, 'Capture spool cleanup failed: ' + str(exc), exc)
                            result['cleanup_status'] = 'unconfirmed'
                    elif spool_temp is not None:
                        result['retained_capture_spool'] = str(capture_spool)
                    try:
                        diagnostics_temp.cleanup()
                    except Exception as exc:
                        infrastructure_error(result, 'Task diagnostics cleanup failed: ' + str(exc), exc)
                        result['cleanup_status'] = 'unconfirmed'
                    task_outcome(result, args, output)
                    print(f"CLEANUP {task['task_id']}: {result['cleanup_status']}", flush=True)
                    write_json(task_dir / 'result.json', result)
                    batch_checkpoint(run_root, experiment, task['task_id'], result['status'], result)
            print('END ' + json.dumps({k: result[k] for k in ['task_id', 'status', 'agent_status',
                'verify_passed', 'quality_passed', 'cleanup_status', 'error']}, ensure_ascii=False), flush=True)
            if _interrupt_requested or result['status'] == 'interrupted':
                raise KeyboardInterrupt
            if result['status'] == 'infrastructure_failed':
                experiment.update(status='blocked', error='; '.join(result['infrastructure_errors']) or 'Task cleanup unconfirmed',
                                  stopped_at_task=task['task_id'])
                break
        else:
            experiment['status'] = ('completed_with_failures' if any(r.get('status') != 'completed' for r in rows)
                                    else 'completed')
    except Exception as exc:
        experiment.update(status='blocked', error=f'{type(exc).__name__}: {exc}', stopped_at_task=experiment.get('next_task_id'))
        experiment['traceback'] = traceback.format_exc()
        print(experiment['traceback'], file=sys.stderr, flush=True)
    except KeyboardInterrupt:
        experiment.update(status='interrupted', error='Interrupted by user', stopped_at_task=experiment.get('next_task_id'))
    finally:
        with finishing():
            try:
                stop(proxy_proc)
            except Exception as exc:
                experiment.setdefault('cleanup_errors', []).append('Proxy: ' + str(exc))
                experiment.setdefault('cleanup_tracebacks', []).append(traceback.format_exc())
            if gateway is not None:
                try:
                    gateway.stop()
                except Exception as exc:
                    experiment.setdefault('cleanup_errors', []).append('Tool gateway: ' + str(exc))
                    experiment.setdefault('cleanup_tracebacks', []).append(traceback.format_exc())
            if spark is not None:
                try:
                    spark.close()
                except Exception as exc:
                    experiment.setdefault('cleanup_errors', []).append('Spark: ' + str(exc))
                    experiment.setdefault('cleanup_tracebacks', []).append(traceback.format_exc())
            if _interrupt_requested:
                experiment.update(status='interrupted', error='Interrupted by user during cleanup')
            if experiment.get('cleanup_errors') and experiment['status'] != 'interrupted':
                experiment.update(status='blocked', error='; '.join(experiment['cleanup_errors']))
            experiment['ended_at'] = now()
            # Only repair the current unfinished record. Never rewrite completed or
            # historical attempts, including results produced by older versions.
            for row in rows:
                if row.get('status') != 'running':
                    continue
                row.update(status='interrupted' if experiment['status'] == 'interrupted' else 'infrastructure_failed',
                           completed=False, error=row.get('error') or experiment.get('error') or 'Task finalization incomplete',
                           cleanup_status='unconfirmed', ended_at=now())
                if row['status'] != 'interrupted':
                    row.setdefault('infrastructure_errors', []).append(row['error'])
                if experiment.get('traceback'):
                    row.setdefault('tracebacks', []).append(experiment['traceback'])
                row.setdefault('agent_status', 'unknown')
                row.setdefault('quality_passed', False)
                row['experiment'] = {k: v for k, v in experiment.items() if k != 'progress'}
                write_json(run_root / task_directory(args.split, row['task_id']) / 'result.json', row)
                batch_checkpoint(run_root, experiment, row['task_id'], row['status'], row)
            try:
                experiment['diagnostics'] = {'tls_proxy_log': (scratch_root / 'tls_proxy.log').read_text(errors='replace')
                                             if (scratch_root / 'tls_proxy.log').exists() else ''}
                scratch.cleanup()
            except Exception as exc:
                experiment.setdefault('cleanup_errors', []).append(str(exc))
                experiment.setdefault('cleanup_tracebacks', []).append(traceback.format_exc())
                if experiment['status'] != 'interrupted':
                    experiment.update(status='blocked', error='Batch diagnostics cleanup failed: ' + str(exc))
            try:
                batch_checkpoint(run_root, experiment)
            except OSError:
                print('Cannot persist final batch checkpoint; previous checkpoint retained', file=sys.stderr, flush=True)
                traceback.print_exc()
                if experiment['status'] != 'interrupted':
                    experiment['status'] = 'blocked'
            print(f"BATCH {experiment['status']}: {run_root / 'experiment.json'}", flush=True)
    return 130 if experiment['status'] == 'interrupted' else 0 if experiment['status'] in ('completed', 'completed_with_failures') else 1


if __name__ == '__main__':
    raise SystemExit(main())
