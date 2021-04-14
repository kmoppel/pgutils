#!/usr/bin/env bash

### A script to 1) pull a full directly launchable compressed "tar" snapshot of a DB instance and
### 2) push it to some rempote host for DR purposes, keeping X last snapshots.


CRON_MODE=1 # no causual chat, only errors

INSTANCE_NAME=appx  # snapshots will be stored under $DR_SNAPSHOTS_PATH/$INSTANCE_NAME on the DR host


### REMOTE DR HOST PARAMS ###
DR_SNAPSHOTS_TO_KEEP=3
DR_SNAPSHOTS_PATH='/var/lib/postgresql/dr_snapshots'
# NB! ${DR_SNAPSHOTS_PATH}/${INSTANCE_NAME} needs to be pre-created manually!

DR_SSH_PORT=22
DR_SSH_USER=postgres
DR_SSH_HOST=localhost


### LOCAL DB HOST PARAMS ###
TEMP_SNAPSHOT_PATH='/var/lib/postgresql/backups/dr_temp_snap'
DROP_TEMP_SNAP_AFTER_TRANSFER=0

PG_BINDIR=/usr/lib/postgresql/13/bin
PG_HOST_SOCKET='/var/run/postgresql'
PG_PORT=5432
PG_USER=postgres

COMPRESS_LEVEL=3 # Gzip
DR_PUSH_RETRIES=3
ACTION="$1"


function print_usage() {
    echo "A script to pull compressed self-containing snapshots locally via pg_basebackup and rsync them to a remote DR host"
    echo "keeping last DR_SNAPSHOTS_TO_KEEP snapshots. NB! Requires enough free disk space on the DB host to store one snapshot!"
    echo ""
    echo "Usage (from DB host):"
    echo "  ./take_standalone_compressed_snapshot.sh info - shows current rempote DR snapshot infos"
    echo "  ./take_standalone_compressed_snapshot.sh backup - pull and transfer a new snapshot to the DR host"
    echo "  ./take_standalone_compressed_snapshot.sh pull-only - pull a new snapshot on the DB host"
    echo "  ./take_standalone_compressed_snapshot.sh push-only - transfer / re-transfer the most recent snapshot to the DR host"
    echo "  ./take_standalone_compressed_snapshot.sh expire - drop all extra DR snapshots. DR_SNAPSHOTS_TO_KEEP=3 by default"
    exit 0
}

if [ -n "$ACTION" ] ; then
    echo 'info, backup, pull-only, push-only, expire' | grep -q "$ACTION"
    if [ $? -ne 0 ]; then
      print_usage
      exit 1
    fi
else
    print_usage
fi


function chat() {
    if [ "$CRON_MODE" -lt 1 ]; then
        echo "$1"
    fi
}

function check_dr_host_ssh() {
    chat "Checking SSH connection to the DR host ..."
    RET=$(ssh -p $DR_SSH_PORT -o LogLevel=error $DR_SSH_USER@$DR_SSH_HOST date)
    if [ "$?" -ne 0 ] ; then
      echo "SSH connection check failed"
      exit 1
    fi
    chat "OK"
}

function info() {
    chat "Listing current DR snapshots for instance $INSTANCE_NAME ..."
    snaps=$(ssh -q -p ${DR_SSH_PORT} ${DR_SSH_USER}@${DR_SSH_HOST} ls ${DR_SNAPSHOTS_PATH}/${INSTANCE_NAME})
    if [ "$?" -ne 0 ] ; then
      echo "Could not list snapshot path for instance $INSTANCE_NAME on the DR host. abort"
      exit 1
    fi
    
    for snap in ${snaps} ; do
        size_cmd=$(ssh -q -p ${DR_SSH_PORT} ${DR_SSH_USER}@${DR_SSH_HOST} du -sh --apparent-size -L --time ${DR_SNAPSHOTS_PATH}/${INSTANCE_NAME}/${snap})
        size=$(echo "$size_cmd" | cut -f1)
        ts=$(echo "$size_cmd" | cut -f2)
        echo "$snap [size $size, last mod. time $ts]"
    done
}

function delete_prev_snap_data_if_exists() {
    chat "Clearing previous local snapshot data if any ..."
    if [ -d "${TEMP_SNAPSHOT_PATH}" ]; then
        rm -rf ${TEMP_SNAPSHOT_PATH}
    fi
    
    mkdir ${TEMP_SNAPSHOT_PATH}
    if [ "$?" -ne 0 ] ; then
      echo "Could not reset temp snapshot path. abort"
      exit 1
    fi
}

function apply_expire() {
    chat "Expiring older DR snapshots if needed (DR_SNAPSHOTS_TO_KEEP=$DR_SNAPSHOTS_TO_KEEP) ..."
    DR_SNAPSHOTS_TO_KEEP_MINUS_ONE=$((DR_SNAPSHOTS_TO_KEEP-1))
    if [ "$DR_SNAPSHOTS_TO_KEEP_MINUS_ONE" -lt 1 ]; then
        DR_SNAPSHOTS_TO_KEEP_MINUS_ONE=1
    fi
    snap_count=$(ssh -q -p ${DR_SSH_PORT} ${DR_SSH_USER}@${DR_SSH_HOST} ls -1 ${DR_SNAPSHOTS_PATH}/${INSTANCE_NAME} | wc -l)
    if [ "$?" -ne 0 ] ; then
      echo "Could not count previous snapshots. abort"
      exit 1
    fi            
    if [ "$snap_count" -le "$DR_SNAPSHOTS_TO_KEEP_MINUS_ONE" ]; then
        chat "No snapshots found to remove..."
    else
        threshold_snap=$(ssh -q -p ${DR_SSH_PORT} ${DR_SSH_USER}@${DR_SSH_HOST} ls -1 ${DR_SNAPSHOTS_PATH}/${INSTANCE_NAME} | sort -V | tail -${DR_SNAPSHOTS_TO_KEEP_MINUS_ONE} | head -1)
        if [ "$?" -ne 0 ] ; then
          echo "Could not determine the oldest snapshot to keep. abort"
          exit 1
        fi        
        all_snaps=$(ssh -q -p ${DR_SSH_PORT} ${DR_SSH_USER}@${DR_SSH_HOST} ls -1 ${DR_SNAPSHOTS_PATH}/${INSTANCE_NAME})
        if [ "$?" -ne 0 ] ; then
          echo "Could not list all previous snapshots. abort"
          exit 1
        fi        
        for snap in ${all_snaps}; do
            if [[ "$snap" < "$threshold_snap" ]]; then
                chat "Expiring snapshot $snap ..."
                ssh -q -p ${DR_SSH_PORT} ${DR_SSH_USER}@${DR_SSH_HOST} rm -rf ${DR_SNAPSHOTS_PATH}/${INSTANCE_NAME}/${snap}    # NB! ignoring remove errors for now...
            fi
        done
    fi    
}

function pull_snapshot() {
    chat "Starting pg_basebackup:"
    chat "${PG_BINDIR}/pg_basebackup -h ${PG_HOST_SOCKET} -p ${PG_PORT} -U ${PG_USER} -c fast -Z ${COMPRESS_LEVEL} -R -Ft -D ${TEMP_SNAPSHOT_PATH}"
    SNAP_START_EPOCH=$(date +%s)
    RET=$(${PG_BINDIR}/pg_basebackup -h ${PG_HOST_SOCKET} -p ${PG_PORT} -U ${PG_USER} -c fast -Z ${COMPRESS_LEVEL} -R -Ft -D ${TEMP_SNAPSHOT_PATH})
    if [ "$?" -ne 0 ] ; then
      echo "Could not pull a snapshot. abort"
      echo "$RET"
      exit 1
    fi
    SNAP_END_EPOCH=$(date +%s)
    chat "OK. pg_basebackup finished in $((SNAP_END_EPOCH-SNAP_START_EPOCH)) seconds"
}

function transfer_snapshot() {
    if [ ! -d "$TEMP_SNAPSHOT_PATH" ] ; then
        echo "Could not find a snapshot at $TEMP_SNAPSHOT_PATH. abort"
        exit 1
    fi
    chat "Pushing snapshot to DR via rsync ..."
    PUSH_START_EPOCH=$(date +%s)
    SNAP_END_TIME=$(date -d @`stat -L --format=%Y $TEMP_SNAPSHOT_PATH` +%Y-%m-%d_%H%M)
    if [ -z "$SNAP_END_TIME" ]; then
        echo "Could not determine snapshot last modification time. abort"
        exit 1
    fi

    for i in $(seq 1 $DR_PUSH_RETRIES); do
        RET=$(rsync -a -q -e "ssh -p ${DR_SSH_PORT} -o LogLevel=error" ${TEMP_SNAPSHOT_PATH}/ ${DR_SSH_USER}@${DR_SSH_HOST}:${DR_SNAPSHOTS_PATH}/${INSTANCE_NAME}/${SNAP_END_TIME})
        if [ "$?" -eq 0 ] ; then
            break
        fi
        if [ "$i" -eq $DR_PUSH_RETRIES ]; then
            echo "Could not push snapshot to the DR host. abort"
            exit 1
        fi
        chat "Retrying transfer to the DR host up to $DR_PUSH_RETRIES times. Sleep 1min ..."
        sleep 60
    done
    PUSH_STOP_EPOCH=$(date +%s)
    chat "OK. rsync finished in $((PUSH_STOP_EPOCH-PUSH_START_EPOCH)) seconds"

    if [ "$DROP_TEMP_SNAP_AFTER_TRANSFER" -gt 0 ]; then
        chat "Dropping temporary snapshot folder $TEMP_SNAPSHOT_PATH ..."
        rm -rf ${TEMP_SNAPSHOT_PATH}
    fi
}



### MAIN ###


if [ $ACTION == "info" ]; then
    CRON_MODE=0
    info
    exit 0
fi

chat "Starting ACTION $ACTION (`date`)"

if [ $ACTION == "expire" ]; then
    apply_expire
    exit 0
fi

check_dr_host_ssh

if [ $ACTION == "backup" -o $ACTION == "pull-only" ]; then
    delete_prev_snap_data_if_exists
    pull_snapshot
fi

if [ $ACTION == "backup" -o $ACTION == "push-only" ]; then
    apply_expire
    transfer_snapshot
fi

chat "Finished (`date`)"
