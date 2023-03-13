# Critical Resources Auditing and context-Aware Fencing Decisions Daemon (CRAAFDD) v1.0 
 ######################################################################
 #	 ██████╗██████╗  █████╗  █████╗ ███████╗██████╗ ██████╗       # 
 #	██╔════╝██╔══██╗██╔══██╗██╔══██╗██╔════╝██╔══██╗██╔══██╗      #
 #	██║     ██████╔╝███████║███████║█████╗  ██║  ██║██║  ██║      #
 #	██║     ██╔══██╗██╔══██║██╔══██║██╔══╝  ██║  ██║██║  ██║      #
 #	╚██████╗██║  ██║██║  ██║██║  ██║██║     ██████╔╝██████╔╝      #
 # 	 ╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚═════╝ ╚═════╝       #
 ######################################################################
 # License: AGPL v3.
 # Giuseppe Calò                giuseppe.calo@cmcc.it
 # Danilo Mazzarella            danilo.mazzarella@cmcc.it 
 # Marco Chiarelli              marco.chiarelli@cmcc.it
 #                              marco_chiarelli@yahoo.it
#

#!/bin/bash

AM_I_QUORUM=0 #1 #0 #1
MIN_QUORUM_NODES=1 #-1 # let it -1 for dynamic parsing through GPFS' mmgetstate command, otherwise specify a >= integer
SAMPLING_PERIOD=2 #1 #3 #0 #3
SAMPLING_PERIOD_WHEN_DISARMED=10
MAX_SNAPSHOTS=1 #3 #1 #3 #2
INTER_SNAPSHOTS_INTERVAL_SECONDS=3 #10 #3 #$(($MMGETSTATE_ELAPSED_SECONDS*$SAFETY_SNAPSHOTS_INTERVAL_MULTIPLIER))
SNAPSHOT_TIMESTAMP_EPSILON=3 #2 # 2 seconds of max difference
MAX_ALLOWED_SIMILAR_STAT_NODES=2
GPFS_CONTROL_PATH="/data/.craafdd/ces"
GPFS_MMGETSTATE_COMMAND="/usr/lpp/mmfs/bin/mmgetstate -Ls" # + -N n001-ibj"
PHYSICAL_QUORUM_CONDITION=0 #1 #0 #1 #0 #1 #0
MMGETSTATE_HOSTNAME_ROW_ID=3
MMGETSTATE_DEFINED_NODES_LINE="Number of n" # odes defined in the cluster
MMGETSTATE_ACTIVE_NODES_LINE="Number of l" # ocal nodes active in the cluster
MMGETSTATE_DEFINED_QUORUM_NODES_LINE="Number of quorum nodes d" # efined in the cluster:"
MMGETSTATE_ACTIVE_QUORUM_NODES_LINE="Number of quorum nodes a" # ctive in the cluster:"
MMGETSTATE_MIN_QUORUM_NODES_LINE="Quorum ="

FENCING_COMMAND="/usr/lpp/mmfs/bin/mmshutdown"
FENCING_DAEMON_LOGFILE="craafdd_log.txt"
AUDITED_NETWORK_INTERFACE="ib3" #"bond1" # "bond-bond1" # "eno1np0"

LFR_ERROR_STATEMENT=1 #0 # type 1 for continue, 0 for break

SLEEPING_SECONDS_AFTER_FENCING=0
SLEEPING_SECONDS_AFTER_LFR_ERROR=0
REMOVAL_BEHAVIOR_AFTER_FENCING=0 # type 1 to remove state file after fencing, 0 otherwise 
REMOVAL_BEHAVIOR_AFTER_LFR_ERROR=0 # type 1 to remove state file after lfr error, 0 otherwise

MAIL_CMD="sendmail"
MAIL_FROM="scc-noreply@cmcc.it"
MAIL_TO="marco_chiarelli@yahoo.it"

#./craafdd $AM_I_QUORUM $MIN_QUORUM_NODES $SAMPLING_PERIOD $SAMPLING_PERIOD_WHEN_DISARMED $MAX_SNAPSHOTS $INTER_SNAPSHOTS_INTERVAL_SECONDS $SNAPSHOT_TIMESTAMP_EPSILON $MAX_ALLOWED_SIMILAR_STAT_NODES "$GPFS_CONTROL_PATH" "$GPFS_MMGETSTATE_COMMAND" $PHYSICAL_QUORUM_CONDITION $MMGETSTATE_HOSTNAME_ROW_ID "$MMGETSTATE_DEFINED_NODES_LINE" "$MMGETSTATE_ACTIVE_NODES_LINE" "$MMGETSTATE_DEFINED_QUORUM_NODES_LINE" "$MMGETSTATE_ACTIVE_QUORUM_NODES_LINE" "$MMGETSTATE_MIN_QUORUM_NODES_LINE" "$FENCING_COMMAND" "$FENCING_DAEMON_LOGFILE" "$AUDITED_NETWORK_INTERFACE" $LFR_ERROR_STATEMENT $SLEEPING_SECONDS_AFTER_FENCING $SLEEPING_SECONDS_AFTER_LFR_ERROR $REMOVAL_BEHAVIOR_AFTER_FENCING $REMOVAL_BEHAVIOR_AFTER_LFR_ERROR "$MAIL_CMD" "$MAIL_FROM" "$MAIL_TO" &

./craafdd $AM_I_QUORUM $MIN_QUORUM_NODES $SAMPLING_PERIOD $SAMPLING_PERIOD_WHEN_DISARMED $MAX_SNAPSHOTS $INTER_SNAPSHOTS_INTERVAL_SECONDS $SNAPSHOT_TIMESTAMP_EPSILON $MAX_ALLOWED_SIMILAR_STAT_NODES "$GPFS_CONTROL_PATH" "$GPFS_MMGETSTATE_COMMAND" $PHYSICAL_QUORUM_CONDITION $MMGETSTATE_HOSTNAME_ROW_ID "$MMGETSTATE_DEFINED_NODES_LINE" "$MMGETSTATE_ACTIVE_NODES_LINE" "$MMGETSTATE_DEFINED_QUORUM_NODES_LINE" "$MMGETSTATE_ACTIVE_QUORUM_NODES_LINE" "$MMGETSTATE_MIN_QUORUM_NODES_LINE" "$FENCING_COMMAND" "$FENCING_DAEMON_LOGFILE" "$AUDITED_NETWORK_INTERFACE" $LFR_ERROR_STATEMENT $SLEEPING_SECONDS_AFTER_FENCING $SLEEPING_SECONDS_AFTER_LFR_ERROR $REMOVAL_BEHAVIOR_AFTER_FENCING $REMOVAL_BEHAVIOR_AFTER_LFR_ERROR "$MAIL_CMD" "$MAIL_FROM" "$MAIL_TO"
