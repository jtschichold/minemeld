#  Copyright 2015-2016 Palo Alto Networks, Inc
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


from minemeld import __version__
import psutil
from typing import (
    Type, List, TYPE_CHECKING,
    Dict, Optional,
)
import math
import os
import argparse
import multiprocessing
import signal
import logging
import os.path

import gevent
import gevent.signal
import gevent.monkey
gevent.monkey.patch_all(thread=False, select=False)

from minemeld.chassis import Chassis
from minemeld.orchestrator import Orchestrator
from minemeld.defaults import DEFAULT_MINEMELD_MAX_CHASSIS
import minemeld.config

if TYPE_CHECKING:
    from minemeld.config import MineMeldConfig


LOG = logging.getLogger(__name__)

def _run_chassis(chassis_id: int, chassis_plan: List[List[str]], config: 'MineMeldConfig'):
    try:
        # lower priority to make master and web
        # more "responsive"
        os.nice(5)

        chassis = Chassis(
            chassis_id=chassis_id,
            chassis_plan=chassis_plan,
            config=config
        )
        chassis.initialize()

        gevent.signal.signal(signal.SIGUSR1, chassis.on_sig_stop)

        while not chassis.fts_init():
            if chassis.poweroff.wait(timeout=0.1) is not None:
                break

            gevent.sleep(1)

        LOG.info('Nodes initialized')

        try:
            chassis.poweroff.wait()
            LOG.info('power off')

        except KeyboardInterrupt:
            LOG.error("We should not be here !")
            chassis.stop()

    except Exception:
        LOG.exception('Exception in chassis main procedure')
        raise


def _check_disk_space(num_nodes: int) -> Optional[int]:
    free_disk_per_node = int(os.environ.get(
        'MM_DISK_SPACE_PER_NODE',
        10*1024  # default: 10MB per node
    ))
    needed_disk = free_disk_per_node*num_nodes*1024
    free_disk = psutil.disk_usage('.').free

    LOG.debug('Disk space - needed: {} available: {}'.format(needed_disk, free_disk))

    if free_disk <= needed_disk:
        LOG.critical(
            ('Not enough space left on the device, available: {} needed: {}'
             ' - please delete traces, logs and old engine versions and restart').format(
                free_disk, needed_disk
            )
        )
        return None

    return free_disk


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Low-latency threat indicators processor"
    )
    parser.add_argument(
        '--version',
        action='version',
        version=__version__
    )
    parser.add_argument(
        '--multiprocessing',
        default=0,
        type=int,
        action='store',
        metavar='NP',
        help='enable multiprocessing. NP is the number of chassis, '
             '0 to use two chassis per machine core (default)'
    )
    parser.add_argument(
        '--nodes-per-chassis',
        default=15.0,
        type=float,
        action='store',
        metavar='NPC',
        help='number of nodes per chassis (default 15)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='verbose'
    )
    parser.add_argument(
        'config',
        action='store',
        metavar='CONFIG',
        help='path of the config file or of the config directory'
    )
    return parser.parse_args()


def _setup_environment(config: str) -> None:
    # make config dir available to nodes
    cdir = config
    if not os.path.isdir(cdir):
        cdir = os.path.dirname(config)
    os.environ['MM_CONFIG_DIR'] = cdir

    if not 'REQUESTS_CA_BUNDLE' in os.environ and 'MM_CA_BUNDLE' in os.environ:
        os.environ['REQUESTS_CA_BUNDLE'] = os.environ['MM_CA_BUNDLE']


def main() -> int:
    orchestrator = None
    processes_lock = None
    processes = None
    disk_space_monitor_glet = None

    def _cleanup():
        if orchestrator is not None:
            orchestrator.checkpoint_graph()

        if processes_lock is None:
            signal_received.set()
            return

        with processes_lock:
            if processes is None:
                signal_received.set()
                return

            for p in processes:
                if not p.is_alive():
                    continue

                try:
                    os.kill(p.pid, signal.SIGUSR1)
                except OSError:
                    continue

            while sum([int(t.is_alive()) for t in processes]) != 0:
                gevent.sleep(1)

        signal_received.set()

    def _sigint_handler(signum, sigstack) -> None:
        LOG.info('SIGINT received')
        gevent.spawn(_cleanup)

    def _sigterm_handler(signum, sigstack) -> None:
        LOG.info('SIGTERM received')
        gevent.spawn(_cleanup)

    def _disk_space_monitor(num_nodes: int) -> None:
        while True:
            if _check_disk_space(num_nodes=num_nodes) is None:
                gevent.spawn(_cleanup)
                break

            gevent.sleep(60)

    def _config_monitor(config: 'minemeld.config.MineMeldConfig') -> None:
        mtime = os.stat(config.path).st_mtime
        while True:
            new_mtime = os.stat(config.path).st_mtime
            if new_mtime != mtime:
                try:
                    new_config = minemeld.config.load(config.path)
                    new_config.compute_changes(config)

                except Exception as e:
                    LOG.warning(f'Invalid config detected, ignored: {str(e)}')
                    gevent.sleep(10)
                    continue

                reinit_needed = next((c for c in new_config.changes if c.change != minemeld.config.MineMeldConfigChange.CONFIG_HUP), None)
                if reinit_needed is not None:
                    LOG.info('Config change detected, reinit needed')
                    gevent.spawn(_cleanup)
                    break

            gevent.sleep(10)

    args = _parse_args()

    # logging
    loglevel = logging.INFO
    if args.verbose or os.environ.get('MM_VERBOSE') is not None:
        loglevel = logging.DEBUG

    logging.basicConfig(
        level=loglevel,
        format="%(asctime)s (%(process)d)%(module)s.%(funcName)s"
               " %(levelname)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )
    LOG.info("Starting mm-run.py version %s", __version__)
    LOG.info("mm-run.py arguments: %s", args)

    _setup_environment(args.config)

    # load and validate config
    config = minemeld.config.load(args.config)

    LOG.info("mm-run.py config: %s", config)

    if _check_disk_space(num_nodes=len(config.nodes)) is None:
        LOG.critical('Not enough disk space available, exit')
        return 2

    np = args.multiprocessing
    if np == 0:
        np = os.environ.get('MINEMELD_MAX_CHASSIS', DEFAULT_MINEMELD_MAX_CHASSIS)
    LOG.info('multiprocessing: #cores: %d', multiprocessing.cpu_count())
    LOG.info("multiprocessing: max #chassis: %d", np)

    npc = args.nodes_per_chassis
    if npc <= 0:
        LOG.critical('nodes-per-chassis should be a positive integer')
        return 2

    np = min(
        int(math.ceil(len(config.nodes)/npc)),
        np
    )
    LOG.info("Number of chassis: %d", np)

    chassis_plan: List[List[str]] = [[] for j in range(np)]
    for j, ft in enumerate(config.nodes.keys()):
        pn = j % len(chassis_plan)
        chassis_plan[pn].append(ft)

    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)

    processes = []
    for chassis_id in range(len(chassis_plan)):
        if len(chassis_plan[chassis_id]) == 0:
            continue

        p = multiprocessing.Process(
            target=_run_chassis,
            kwargs=dict(
                chassis_id=chassis_id,
                chassis_plan=chassis_plan,
                config=config
            )
        )
        processes.append(p)
        p.start()

    processes_lock = gevent.lock.BoundedSemaphore()
    signal_received = gevent.event.Event()

    gevent.signal.signal(signal.SIGINT, _sigint_handler)
    gevent.signal.signal(signal.SIGTERM, _sigterm_handler)

    try:
        orchestrator = Orchestrator(
            chassis_plan=chassis_plan
        )
        orchestrator.start()
        orchestrator.wait_for_chassis(timeout=10)
        # here nodes are all CONNECTED, fabric and mgmtbus up, with mgmtbus
        # dispatching and fabric not dispatching
        orchestrator.start_status_monitor()
        orchestrator.init_graph(config)
        # here nodes are all INIT
        orchestrator.start_chassis()
        # here nodes should all be starting

    except Exception:
        LOG.exception('Exception initializing graph')
        _cleanup()
        raise

    disk_space_monitor_glet = gevent.spawn(
        _disk_space_monitor, len(config.nodes))

    config_monitor_glet = gevent.spawn(
        _config_monitor,
        config
    )

    try:
        while not signal_received.wait(timeout=1.0):
            with processes_lock:
                r = [int(t.is_alive()) for t in processes]
                if sum(r) != len(processes):
                    LOG.info("One of the chassis has stopped, exit")
                    break

    except KeyboardInterrupt:
        LOG.info("Ctrl-C received, exiting")

    except Exception:
        LOG.exception("Exception in main loop")

    if disk_space_monitor_glet is not None:
        disk_space_monitor_glet.kill()

    # XXX - temporary disabled
    # if config_monitor_glet is not None:
    #    config_monitor_glet.kill()

    return 0
