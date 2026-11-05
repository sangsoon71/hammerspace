#!/usr/bin/env python3
"""
nfs_fio_bench.py - 여러 NFS 클라이언트에서 fio를 동시에 반복 실행하고
                   aggregate(합산) 결과와 클라이언트별 결과를 Google Sheet에 누적 기록

동작 방식
  fio 내장 client/server 모드를 사용합니다.
  - 각 NFS 클라이언트:  fio --server  (상주)
  - 컨트롤러(이 스크립트 실행 호스트): fio --client=h1 job_h1 --client=h2 job_h2 ...
  모든 클라이언트가 같은 job을 동시에 시작하고, fio가 계산한 "All clients"
  합산값(대역폭/IOPS 합, 지연은 전체 분포 기준)을 aggregate 행으로 기록합니다.
  --clients를 생략하면 로컬 단일 호스트 모드로 동작합니다.

준비
  1) 모든 클라이언트와 컨트롤러에 같은 버전의 fio 설치  (apt/dnf install fio)
     컨트롤러:  pip install gspread google-auth
  2) 각 클라이언트에서 fio 서버 실행 (또는 --start-servers로 ssh 자동 실행)
       fio --server --daemonize=/tmp/fio-server.pid
     방화벽에서 TCP 8765 허용 (컨트롤러 → 클라이언트)
  3) 모든 클라이언트가 같은 경로(--mount)에 NFS를 마운트해 둘 것
  4) Google 서비스 계정 JSON 키, 시트를 서비스 계정 이메일에 편집자로 공유

사용 예
  python3 nfs_fio_bench.py --mount /mnt/nfs \
      --clients nfs-c1,nfs-c2,nfs-c3,nfs-c4 \
      --sheet-id 1AbC... --creds ~/sa.json \
      --runs 3 --rw randread,randwrite,read,write --bs 4k,1m \
      --iodepth 32 --numjobs 1,4 --size 4G --runtime 60

  # 클라이언트 목록 파일(한 줄에 하나, # 주석 가능):   --clients-file clients.txt
  # 포트 지정:                                        --clients c1:8765,c2:9000
  # ssh로 fio 서버 자동 기동/종료 + 마운트옵션 수집:  --ssh --start-servers
  # 매 실행 전 모든 클라이언트 캐시 비우기:           --ssh --drop-caches
  #   (원격에서 'sudo -n'을 쓰므로 비밀번호 없는 sudo 또는 root ssh 필요)
  # 클라이언트 수를 늘려가며 스케일링 측정:          --scale 1,2,4
  #   (--clients 앞에서부터 1대, 2대, 4대를 사용)
  # 추가 fio 파라미터 (모두 시트에 기록됨):
  #   --nrfiles 1,16,64        job당 파일 수 (여러 값이면 조합에 포함)
  #   --ramp-time 10           워밍업(초), 결과에서 제외
  #   --startdelay 5-15        job 시작 지연 (범위면 job마다 무작위)
  #   --refill-buffers         매 I/O 버퍼 갱신 (dedup/압축 효과 배제)
  #   --norandommap            랜덤 블록 맵 미사용
  #   --random-generator lfsr  tausworthe | lfsr | tausworthe64

시트 기록
  scope=aggregate : 실행 1회당 1행, 전체 클라이언트 합산
  scope=client    : 클라이언트별 행 (--no-per-client로 끌 수 있음)
  scope=local     : --clients 없이 로컬 실행
  모든 행은 로컬 CSV(--csv)에도 기록됩니다.
"""
import argparse
import csv
import datetime
import itertools
import json
import os
import re
import shlex
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time
import uuid

HEADER = [
    "timestamp", "batch_id", "scope", "num_clients", "client", "controller",
    "nfs_server_export", "mountpoint", "nfs_opts",
    "rw", "bs", "iodepth", "numjobs", "nrfiles", "size", "runtime_s", "rwmixread",
    "ioengine", "direct", "ramp_time_s", "startdelay", "refill_buffers", "norandommap",
    "random_generator",
    "iteration",
    "read_bw_MiBps", "read_iops", "read_lat_mean_ms", "read_clat_p99_ms",
    "write_bw_MiBps", "write_iops", "write_lat_mean_ms", "write_clat_p99_ms",
    "total_bw_MiBps", "total_iops", "fio_version", "note",
]
DEFAULT_PORT = 8765


# ---------------------------------------------------------------- helpers
def log(msg):
    print(f"[{datetime.datetime.now():%H:%M:%S}] {msg}", flush=True)


def split_list(s):
    return [x.strip() for x in str(s).split(",") if x.strip()]


def parse_client(spec):
    """'host' 또는 'host:port' -> (host, port)"""
    if spec.count(":") == 1:
        h, p = spec.split(":")
        return h, int(p)
    return spec, DEFAULT_PORT


def safe(s):
    return re.sub(r"[^A-Za-z0-9_.-]", "_", s)


def mount_info_from_text(text, path):
    """/proc/mounts 내용에서 path가 속한 마운트 (dev, mnt, fstype, opts)"""
    best = None
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue
        dev, mnt, fstype, opts = parts[:4]
        mnt = mnt.replace("\\040", " ")
        if path == mnt or path.startswith(mnt.rstrip("/") + "/"):
            if best is None or len(mnt) > len(best[1]):
                best = (dev, mnt, fstype, opts)
    return best


def ssh(a, host, cmd, timeout=60):
    target = f"{a.ssh_user}@{host}" if a.ssh_user else host
    full = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10"] + \
        shlex.split(a.ssh_opts) + [target, cmd]
    return subprocess.run(full, capture_output=True, text=True, timeout=timeout)


def fio_version():
    try:
        return subprocess.run(["fio", "--version"], capture_output=True,
                              text=True).stdout.strip()
    except Exception:
        return ""


def to_seconds(v):
    """fio 시간 값('10', '10s', '2m', '500ms', '5-10')의 최대 초 (timeout 계산용)"""
    best = 0.0
    for num, unit in re.findall(r"(\d+(?:\.\d+)?)\s*(ms|us|s|m|h)?", str(v)):
        mult = {"ms": 1e-3, "us": 1e-6, "m": 60, "h": 3600}.get(unit, 1)
        best = max(best, float(num) * mult)
    return best


def write_jobfile(a, tmpdir, tag, rw, bs, qd, nj, nf):
    """클라이언트별 job 파일. 같은 NFS 디렉터리를 공유해도 파일이 겹치지 않도록
    filename_format에 클라이언트 태그를 넣는다. unlink=1로 실행 후 파일 삭제."""
    lines = [
        "[global]",
        f"directory={a.testdir}",
        f"filename_format=.fiobench.{a.batch}.{tag}.$jobname.$jobnum.$filenum",
        f"rw={rw}", f"bs={bs}", f"iodepth={qd}", f"numjobs={nj}", f"nrfiles={nf}",
        f"size={a.size}", f"runtime={a.runtime}", "time_based",
        f"ramp_time={a.ramp_time}", f"ioengine={a.ioengine}", f"direct={a.direct}",
        f"rwmixread={a.rwmixread}", "group_reporting",
        f"unlink={0 if a.keep_files else 1}",
    ]
    if a.startdelay:
        lines.append(f"startdelay={a.startdelay}")
    if a.refill_buffers:
        lines.append("refill_buffers")
    if a.norandommap:
        lines.append("norandommap")
    if a.random_generator:
        lines.append(f"random_generator={a.random_generator}")
    lines += [x.lstrip("-") for x in a.fio_extra] + ["", "[nfsbench]", ""]
    path = os.path.join(tmpdir, f"job_{safe(tag)}.fio")
    with open(path, "w") as f:
        f.write("\n".join(lines))
    return path


def parse_json(out):
    """fio 출력 앞부분의 텍스트를 건너뛰고 첫 JSON 객체만 파싱"""
    m = re.search(r"^\{", out, re.M)
    if not m:
        raise RuntimeError("fio JSON 출력 없음: " + out[:300])
    return json.JSONDecoder().raw_decode(out[m.start():])[0]


def run_fio(a, clients, tmpdir, rw, bs, qd, nj, nf):
    """clients=[] 이면 로컬 실행. 반환: (aggregate_job, {client: job})"""
    cmd = ["fio", "--output-format=json"]
    if clients:
        for c in clients:
            h, p = parse_client(c)
            cmd += [f"--client={h},{p}", write_jobfile(a, tmpdir, c, rw, bs, qd, nj, nf)]
    else:
        cmd += [write_jobfile(a, tmpdir, socket.gethostname(), rw, bs, qd, nj, nf)]
    log("실행: " + " ".join(cmd))
    p = subprocess.run(cmd, capture_output=True, text=True,
                       timeout=(a.runtime + a.ramp_time + to_seconds(a.startdelay)) * 3 + 600)
    if p.returncode != 0:
        raise RuntimeError(f"fio 실패 (rc={p.returncode}): "
                           f"{(p.stderr or p.stdout).strip()[-800:]}")
    res = parse_json(p.stdout)

    if not clients:
        return res["jobs"][0], {}

    stats = res.get("client_stats", [])
    agg = next((s for s in stats if s.get("jobname") == "All clients"), None)
    per = [s for s in stats if s.get("jobname") != "All clients"]
    if len(per) != len(clients):
        raise RuntimeError(f"클라이언트 {len(clients)}대 중 {len(per)}대만 결과 반환")
    if agg is None:          # 클라이언트 1대면 fio가 합산 항목을 만들지 않음
        agg = per[0]
    # client_stats의 hostname은 IP로 나올 수 있으므로 명령 순서대로 매핑
    return agg, dict(zip(clients, per))


def extract(job, side):
    d = job.get(side, {})
    if not d or d.get("io_bytes", 0) == 0:
        return ["", "", "", ""]
    bw = d.get("bw", 0) / 1024.0                                   # KiB/s -> MiB/s
    iops = d.get("iops", 0.0)
    lat = d.get("lat_ns", {}).get("mean", 0) / 1e6                 # ns -> ms
    p99 = d.get("clat_ns", {}).get("percentile", {}).get("99.000000", 0) / 1e6
    return [round(bw, 2), round(iops, 1), round(lat, 3), round(p99, 3)]


def metrics(job):
    r, w = extract(job, "read"), extract(job, "write")
    tot_bw = round(sum(x for x in (r[0], w[0]) if x != ""), 2)
    tot_iops = round(sum(x for x in (r[1], w[1]) if x != ""), 1)
    return r, w, tot_bw, tot_iops


# ---------------------------------------------------------------- sheets / csv
class SheetWriter:
    def __init__(self, sheet_id, worksheet, creds):
        import gspread
        gc = gspread.service_account(filename=os.path.expanduser(creds))
        sh = gc.open_by_key(sheet_id)
        try:
            self.ws = sh.worksheet(worksheet)
        except gspread.WorksheetNotFound:
            log(f"워크시트 '{worksheet}' 생성")
            self.ws = sh.add_worksheet(title=worksheet, rows=1000, cols=len(HEADER))
        first = self.ws.row_values(1)
        if not first:
            self.ws.append_row(HEADER, value_input_option="RAW")
        elif first != HEADER:
            log("경고: 기존 워크시트 헤더가 현재 형식과 다릅니다. "
                "--worksheet로 새 시트 이름을 지정하는 것을 권장합니다.")
        self.pending = []

    def append(self, rows, retries=5):
        rows = self.pending + rows
        self.pending = []
        if not rows:
            return True
        for attempt in range(retries):
            try:
                self.ws.append_rows(rows, value_input_option="USER_ENTERED")
                return True
            except Exception as e:
                wait = 2 ** attempt * 5
                log(f"Sheets 업로드 실패 ({e}); {wait}s 후 재시도")
                time.sleep(wait)
        self.pending = rows
        return False


def append_csv(path, rows):
    new = not os.path.exists(path) or os.path.getsize(path) == 0
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(HEADER)
        w.writerows(rows)


# ---------------------------------------------------------------- remote ops
def start_servers(a, clients):
    for c in clients:
        h, p = parse_client(c)
        cmd = (f"pgrep -f 'fio --server=,{p}' >/dev/null || "
               f"fio --server=,{p} --daemonize=/tmp/fio-server-{p}.pid")
        r = ssh(a, h, cmd)
        log(f"fio 서버 기동 {c}: {'OK' if r.returncode == 0 else r.stderr.strip()}")
    time.sleep(2)


def stop_servers(a, clients):
    for c in clients:
        h, p = parse_client(c)
        ssh(a, h, f"[ -f /tmp/fio-server-{p}.pid ] && kill $(cat /tmp/fio-server-{p}.pid); "
                  f"rm -f /tmp/fio-server-{p}.pid")
    log("fio 서버 종료")


def check_ports(clients):
    bad = []
    for c in clients:
        h, p = parse_client(c)
        try:
            socket.create_connection((h, p), timeout=5).close()
        except OSError as e:
            bad.append(f"{c} ({e})")
    if bad:
        sys.exit("fio 서버에 연결할 수 없습니다: " + ", ".join(bad) +
                 "\n각 클라이언트에서 'fio --server'를 실행하거나 --ssh --start-servers 사용")


def drop_caches(a, clients):
    cmd = "sync; echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null"
    if not clients:
        subprocess.run(["sh", "-c", cmd])
        return
    for c in clients:
        r = ssh(a, parse_client(c)[0], cmd)
        if r.returncode != 0:
            log(f"경고: {c} drop_caches 실패: {r.stderr.strip()[:200]}")


def collect_mounts(a, clients):
    """{client: (dev, mnt, fstype, opts)}  - 원격은 --ssh일 때만"""
    info = {}
    if not clients:
        with open("/proc/mounts") as f:
            info["local"] = mount_info_from_text(f.read(), os.path.realpath(a.mount))
        return info
    if not a.ssh:
        return info
    for c in clients:
        r = ssh(a, parse_client(c)[0], "cat /proc/mounts")
        info[c] = mount_info_from_text(r.stdout, a.mount) if r.returncode == 0 else None
    return info


# ---------------------------------------------------------------- main
def parse_args():
    p = argparse.ArgumentParser(description="멀티 클라이언트 NFS fio 측정 → Google Sheet 누적")
    p.add_argument("--mount", required=True, help="모든 클라이언트 공통 NFS 마운트 경로")
    p.add_argument("--subdir", default="", help="마운트 아래 테스트 디렉터리(미리 존재해야 함)")
    p.add_argument("--clients", default="", help="쉼표 구분 host[:port]. 생략 시 로컬 실행")
    p.add_argument("--clients-file", help="클라이언트 목록 파일")
    p.add_argument("--scale", default="", help="사용할 클라이언트 수 목록 (예: 1,2,4)")
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--rw", default="randread,randwrite,read,write")
    p.add_argument("--bs", default="4k,1m")
    p.add_argument("--iodepth", default="16")
    p.add_argument("--numjobs", default="1", help="클라이언트당 job 수")
    p.add_argument("--nrfiles", default="1",
                   help="job당 파일 수, 쉼표로 여러 값 지정 가능 (size는 이 파일들에 나눠짐)")
    p.add_argument("--size", default="1G", help="job당 전체 I/O 크기 (nrfiles개 파일 합계)")
    p.add_argument("--runtime", type=int, default=60)
    p.add_argument("--ramp-time", type=int, default=5,
                   help="측정 전 워밍업 시간(초), 이 구간은 결과에서 제외")
    p.add_argument("--startdelay", default="",
                   help="job 시작 지연 (예: 10, 10s, 5-15 → 범위면 job마다 무작위)")
    p.add_argument("--refill-buffers", action="store_true",
                   help="매 I/O마다 버퍼 새로 채움 (중복제거/압축 효과 배제)")
    p.add_argument("--norandommap", action="store_true",
                   help="랜덤 I/O 시 블록 맵 사용 안 함 (대용량 파일 CPU 부하 감소, 중복 오프셋 허용)")
    p.add_argument("--random-generator", default="",
                   choices=["", "tausworthe", "lfsr", "tausworthe64"],
                   help="랜덤 오프셋 생성기 (미지정 시 fio 기본값)")
    p.add_argument("--rwmixread", type=int, default=70)
    p.add_argument("--ioengine", default="libaio")
    p.add_argument("--direct", type=int, default=1, choices=[0, 1])
    p.add_argument("--sleep", type=int, default=5, help="실행 간 대기(초)")
    p.add_argument("--keep-files", action="store_true")
    p.add_argument("--note", default="")
    p.add_argument("--csv", default="fio_results.csv")
    p.add_argument("--no-per-client", action="store_true", help="클라이언트별 행은 기록하지 않음")
    # ssh
    p.add_argument("--ssh", action="store_true", help="ssh로 원격 작업 허용(마운트옵션 수집 등)")
    p.add_argument("--ssh-user", default="")
    p.add_argument("--ssh-opts", default="", help='추가 ssh 옵션 (예: "-i ~/.ssh/id_bench")')
    p.add_argument("--start-servers", action="store_true", help="ssh로 fio 서버 기동, 끝나면 종료")
    p.add_argument("--drop-caches", action="store_true", help="매 실행 전 캐시 비우기")
    # sheets
    p.add_argument("--sheet-id", default=os.environ.get("FIO_SHEET_ID"))
    p.add_argument("--worksheet", default="fio_results")
    p.add_argument("--creds", default=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS",
                                                     "~/service_account.json"))
    p.add_argument("--no-sheet", action="store_true")
    p.add_argument("fio_extra", nargs="*", help="-- 뒤에 job 파일에 추가할 fio 옵션 (예: -- fsync=1)")
    return p.parse_args()


def main():
    a = parse_args()
    if not shutil.which("fio"):
        sys.exit("fio가 설치되어 있지 않습니다.")

    clients = split_list(a.clients)
    if a.clients_file:
        with open(a.clients_file) as f:
            clients += [l.split("#")[0].strip() for l in f if l.split("#")[0].strip()]
    if (a.start_servers or a.drop_caches) and clients and not a.ssh:
        sys.exit("--start-servers / --drop-caches 는 --ssh 와 함께 사용하세요.")
    if not clients and not os.path.isdir(a.mount):
        sys.exit(f"경로가 없습니다: {a.mount}")

    a.testdir = os.path.join(a.mount, a.subdir) if a.subdir else a.mount
    a.batch = datetime.datetime.now().strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:4]

    scales = [int(x) for x in split_list(a.scale)] if a.scale else [len(clients)]
    if clients and max(scales) > len(clients):
        sys.exit(f"--scale 최대값({max(scales)})이 클라이언트 수({len(clients)})보다 큽니다.")

    if clients and a.start_servers:
        start_servers(a, clients)
    if clients:
        check_ports(clients)

    mounts = collect_mounts(a, clients)
    for c, mi in mounts.items():
        if mi is None or not mi[2].startswith("nfs"):
            log(f"경고: {c} 의 {a.mount} 는 NFS 마운트가 아닙니다 ({mi[2] if mi else '없음'})")
    first = next((m for m in mounts.values() if m), None)
    if mounts and len({m[3] for m in mounts.values() if m}) > 1:
        log("경고: 클라이언트 간 NFS 마운트 옵션이 다릅니다 (클라이언트별 행에 각각 기록)")

    writer = None
    if not a.no_sheet:
        if not a.sheet_id:
            sys.exit("--sheet-id (또는 FIO_SHEET_ID)가 필요합니다. 시트 없이 쓰려면 --no-sheet")
        writer = SheetWriter(a.sheet_id, a.worksheet, a.creds)

    controller = socket.gethostname()
    fver = fio_version()
    tmpdir = tempfile.mkdtemp(prefix="fiobench-")
    combos = list(itertools.product(scales, split_list(a.rw), split_list(a.bs),
                                    split_list(a.iodepth), split_list(a.numjobs),
                                    split_list(a.nrfiles)))
    total = len(combos) * a.runs
    per_run = a.runtime + a.ramp_time + to_seconds(a.startdelay) + a.sleep
    log(f"batch={a.batch} 클라이언트={clients or ['local']} "
        f"조합 {len(combos)}개 × {a.runs}회 = {total}회, "
        f"예상 {total * per_run / 60:.0f}분 이상")

    def base(scope, ncl, client, mi):
        mi = mi or ("", a.mount, "", "")
        return [None, a.batch, scope, ncl, client, controller, mi[0], mi[1], mi[3]]

    summary, n = {}, 0
    try:
        for sc, rw, bs, qd, nj, nf in combos:
            active = clients[:sc]
            for it in range(1, a.runs + 1):
                n += 1
                log(f"({n}/{total}) clients={sc or 1} rw={rw} bs={bs} "
                    f"iodepth={qd} numjobs={nj} nrfiles={nf} iter={it}")
                if a.drop_caches:
                    drop_caches(a, active)
                try:
                    agg, per = run_fio(a, active, tmpdir, rw, bs, qd, nj, nf)
                except Exception as e:
                    log(f"오류: {e}")
                    continue

                ts = datetime.datetime.now().isoformat(timespec="seconds")
                params = [rw, bs, qd, nj, nf, a.size, a.runtime,
                          a.rwmixread if rw in ("randrw", "rw", "readwrite") else "",
                          a.ioengine, a.direct, a.ramp_time, a.startdelay,
                          int(a.refill_buffers), int(a.norandommap),
                          a.random_generator or "default", it]
                rows = []
                r, w, tb, ti = metrics(agg)
                if active:
                    label = ",".join(active)
                    row = base("aggregate", len(active), label, first)
                else:
                    row = base("local", 1, controller, mounts.get("local"))
                row[0] = ts
                rows.append(row + params + r + w + [tb, ti, fver, a.note])
                if len(active) > 1 and not a.no_per_client:
                    for c, job in per.items():
                        cr, cw, cb, ci = metrics(job)
                        crow = base("client", len(active), c, mounts.get(c) or first)
                        crow[0] = ts
                        rows.append(crow + params + cr + cw + [cb, ci, fver, a.note])

                append_csv(a.csv, rows)
                if writer:
                    writer.append(rows)
                summary.setdefault((sc or 1, rw, bs, qd, nj, nf), []).append((tb, ti))
                extra = ""
                if per:
                    extra = "  (" + ", ".join(f"{c}:{metrics(j)[2]}" for c, j in per.items()) + ")"
                log(f"   → aggregate {tb} MiB/s, {ti} IOPS{extra}")
                if n < total:
                    time.sleep(a.sleep)
    except KeyboardInterrupt:
        log("중단됨 - 지금까지 결과는 저장되었습니다.")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
        if clients and a.start_servers:
            stop_servers(a, clients)
        if writer and writer.pending:
            log(f"업로드 못 한 {len(writer.pending)}행 재시도")
            if not writer.append([]):
                log(f"최종 실패 - CSV({a.csv})에는 모두 기록되어 있습니다.")

    print("\n=== aggregate 요약 (평균 ± 표준편차) ===")
    print(f"{'cli':>4} {'rw':<10}{'bs':>6}{'qd':>5}{'jobs':>6}{'files':>6}{'n':>4}"
          f"{'MiB/s':>20}{'IOPS':>22}")
    sd = lambda xs: statistics.stdev(xs) if len(xs) > 1 else 0.0
    for (sc, rw, bs, qd, nj, nf), v in summary.items():
        bws, iops = [x[0] for x in v], [x[1] for x in v]
        print(f"{sc:>4} {rw:<10}{bs:>6}{qd:>5}{nj:>6}{nf:>6}{len(v):>4}"
              f"{statistics.mean(bws):>12.1f} ± {sd(bws):<6.1f}"
              f"{statistics.mean(iops):>13.0f} ± {sd(iops):<6.0f}")



if __name__ == "__main__":
    main()
